// Package strace imports `strace -T -tt -f` (or -ttt) output into the IOFlux
// trace IR. It translates file syscalls (open/read/write/close/lseek/fsync and
// stat variants) into trace operations, tracking per-descriptor handles and
// file offsets. Each distinct PID/TID becomes one stream.
//
// strace observes only syscalls: mmap page-fault I/O is invisible, as are ops
// on descriptors opened before tracing began. Those limits are recorded in the
// trace header and counted in the import Report.
package strace

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const (
	captureMethod      = "import:strace"
	captureLimitations = "strace syscall trace; mmap page-fault I/O not captured; " +
		"ops on file descriptors opened before tracing or shared across threads are skipped; " +
		"STDIO/socket/non-file syscalls and durations ignored"
	generatedBy = "ioflux-import 0.1.0 / strace"
)

const nsPerDay = int64(24*3600) * 1_000_000_000

// Import parses strace output from r and writes an imported .ioflux trace to w.
// Nothing is written to w unless the produced trace passes validation.
func Import(r io.Reader, w io.Writer) (importer.Report, error) {
	p := newParser()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		p.line(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return importer.Report{}, fmt.Errorf("strace: read: %w", err)
	}
	p.finish()

	// A non-empty input in which no line was a recognizable syscall is not
	// strace output; fail rather than silently emit a valid-but-empty trace.
	if p.parsedSyscalls == 0 && p.lineUnparsed > 0 {
		return importer.Report{}, fmt.Errorf("strace: no parseable syscall lines found; expected `strace -tt` (or -ttt) output")
	}

	meta := importer.HeaderMeta{
		Kind:               trace.TraceImported,
		CaptureMethod:      captureMethod,
		CaptureLimitations: captureLimitations,
		GeneratedBy:        generatedBy,
		Notes:              p.notes(),
	}
	return p.b.WriteTo(w, meta)
}

type pending struct {
	name string
	args string // partial argument text from the <unfinished ...> line
	tNS  int64  // entry (arrival) time
}

type parser struct {
	b   *importer.Builder
	fdt *importer.FDTable

	// Midnight-rollover bookkeeping for -tt time-of-day timestamps.
	dayOffset int64
	prevAbs   int64
	havePrev  bool

	pidToStream map[int64]int64
	streamOrder []int64
	nextStream  int64

	unfinished map[int64]pending

	parsedSyscalls int // lines successfully recognized as a syscall
	lineUnparsed   int // lines that looked like data but did not parse as strace
}

// unparsedLine records a line that reached syscall parsing but could not be
// understood as strace output (distinct from a recognized syscall with
// malformed arguments). It feeds the "is this strace at all?" check in Import.
func (p *parser) unparsedLine() {
	p.b.Skip("unparsed_line")
	p.lineUnparsed++
}

func newParser() *parser {
	return &parser{
		b:           importer.NewBuilder(),
		fdt:         importer.NewFDTable(),
		pidToStream: make(map[int64]int64),
		unfinished:  make(map[int64]pending),
	}
}

func (p *parser) line(raw string) {
	s := strings.TrimRight(raw, "\r\n")
	rest := strings.TrimSpace(s)
	if rest == "" {
		return
	}
	pid, rest, _ := splitPID(rest)
	if strings.HasPrefix(rest, "---") || strings.HasPrefix(rest, "+++") {
		return // signal / exit lines without timestamps
	}
	tNS, body, ok := p.parseTimePrefix(rest)
	if !ok {
		p.unparsedLine() // no parseable timestamp: not strace -tt/-ttt output
		return
	}
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "---") || strings.HasPrefix(body, "+++") {
		return // timestamped signal / exit lines
	}

	if idx := strings.Index(body, "<unfinished ...>"); idx >= 0 {
		head := strings.TrimSpace(body[:idx])
		open := strings.IndexByte(head, '(')
		if open <= 0 {
			p.b.Skip("unparsed_line")
			return
		}
		p.unfinished[pid] = pending{
			name: strings.TrimSpace(head[:open]),
			args: head[open+1:],
			tNS:  tNS,
		}
		return
	}
	if strings.HasPrefix(body, "<...") {
		end := strings.Index(body, "resumed>")
		if end < 0 {
			p.b.Skip("unparsed_line")
			return
		}
		pend, ok := p.unfinished[pid]
		if !ok {
			p.b.Skip("unparsed_line")
			return
		}
		delete(p.unfinished, pid)
		tail := strings.TrimSpace(body[end+len("resumed>"):])
		combined := pend.name + "(" + pend.args + tail
		name, args, ret, ok := splitCall(combined)
		if !ok {
			p.b.Skip("unparsed_line")
			return
		}
		p.handle(pid, pend.tNS, name, args, ret)
		return
	}

	name, args, ret, ok := splitCall(body)
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	p.handle(pid, tNS, name, args, ret)
}

func (p *parser) handle(pid, tNS int64, name, args, ret string) {
	p.parsedSyscalls++
	s := p.stream(pid)
	// Absolute time; the Builder rebases all ops to the global minimum so the
	// trace starts at t=0 regardless of emission order.
	t := tNS
	a := splitArgs(args)
	switch name {
	case "open", "open64":
		p.doOpen(s, t, a, ret, false, false)
	case "openat":
		p.doOpen(s, t, a, ret, true, false)
	case "openat2":
		p.doOpen(s, t, a, ret, true, true)
	case "creat":
		p.doCreat(s, t, a, ret)
	case "read", "read64", "pread64", "pread":
		p.doRW(s, t, name, a, ret, trace.OpRead)
	case "write", "write64", "pwrite64", "pwrite":
		p.doRW(s, t, name, a, ret, trace.OpWrite)
	case "close":
		p.doClose(s, t, a, ret)
	case "fsync", "fdatasync":
		p.doFsync(s, t, a, ret)
	case "lseek", "lseek64", "_llseek":
		p.doLseek(s, a, ret)
	case "stat", "stat64", "lstat", "lstat64", "access", "statx", "newfstatat":
		p.doStatPath(s, t, name, a, ret)
	case "fstat", "fstat64", "fstatfs":
		p.doFstat(s, t, a, ret)
	default:
		// Non-file syscalls (mmap, brk, futex, socket I/O, ...) are ignored,
		// not counted: counting every such call would drown the Report.
	}
}

// failedRet reports whether an strace result string denotes a failed syscall
// (a negative return value, e.g. "-1 ENOENT (...)").
func failedRet(ret string) bool {
	n, ok := parseLeadingInt(ret)
	return ok && n < 0
}

// doOpen handles open/openat/openat2/creat-style opens. at is true for the
// openat family (a[0] is the dirfd); how is true for openat2, whose flags live
// inside an open_how struct ({flags=..., mode=..., resolve=...}) rather than a
// bare flag string.
func (p *parser) doOpen(s, t int64, a []string, ret string, at, how bool) {
	fd, ok := parseLeadingInt(ret)
	if !ok || fd < 0 {
		p.b.Skip("failed_open")
		return
	}
	var pathArg, flagStr, dirfdArg string
	if at {
		if len(a) < 3 {
			p.b.Skip("unparsed_line")
			return
		}
		dirfdArg, pathArg, flagStr = a[0], a[1], a[2]
		if how {
			// openat2: extract flags= from the open_how struct.
			if f, ok := structField(flagStr, "flags"); ok {
				flagStr = f
			} else {
				flagStr = ""
			}
		}
	} else {
		if len(a) < 2 {
			p.b.Skip("unparsed_line")
			return
		}
		pathArg, flagStr = a[0], a[1]
	}
	path, ok := parseQuoted(pathArg)
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	if at {
		resolved, ok := p.resolveAt(s, dirfdArg, path)
		if !ok {
			p.b.Skip("unresolved_dirfd")
			return
		}
		path = resolved
	}
	mode, flags, isDir, isApp := parseOpenFlags(flagStr)
	if isDir {
		p.fdt.OpenDir(s, int(fd), path) // recorded for resolution; no OPEN op
		return
	}
	tgt := p.b.Target(path, trace.TargetFile)
	h := p.fdt.Open(s, int(fd), tgt, path, isApp)
	op := trace.Op{T: t, S: s, Op: trace.OpOpen, Tgt: trace.Ptr(tgt), H: trace.Ptr(h), Mode: mode}
	if len(flags) > 0 {
		op.Flags = flags
	}
	p.b.Add(op)
}

func (p *parser) doCreat(s, t int64, a []string, ret string) {
	fd, ok := parseLeadingInt(ret)
	if !ok || fd < 0 {
		p.b.Skip("failed_open")
		return
	}
	if len(a) < 1 {
		p.b.Skip("unparsed_line")
		return
	}
	path, ok := parseQuoted(a[0])
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	tgt := p.b.Target(path, trace.TargetFile)
	h := p.fdt.Open(s, int(fd), tgt, path, false)
	p.b.Add(trace.Op{
		T: t, S: s, Op: trace.OpOpen, Tgt: trace.Ptr(tgt), H: trace.Ptr(h),
		Mode: trace.ModeWrite, Flags: []string{"create", "trunc"},
	})
}

func (p *parser) doRW(s, t int64, name string, a []string, ret string, kind trace.OpKind) {
	if len(a) < 1 {
		p.b.Skip("unparsed_line")
		return
	}
	fd, ok := parseLeadingInt(a[0])
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	e, ok := p.fdt.Resolve(s, int(fd))
	if !ok || !e.HasHandle {
		p.b.Skip("unresolved_fd")
		return
	}
	n, ok := parseLeadingInt(ret)
	if !ok || n < 0 {
		p.b.Skip("failed_syscall")
		return
	}
	if n == 0 {
		if kind == trace.OpRead {
			p.b.Skip("eof_read")
		} else {
			p.b.Skip("failed_syscall")
		}
		return
	}
	if kind == trace.OpWrite && e.AppendMode {
		// Append writes land at the current EOF; the recorded offset would be
		// wrong, so they cannot be faithfully replayed.
		p.b.Skip("append_write_unmodeled")
		return
	}

	positional := strings.HasPrefix(name, "pread") || strings.HasPrefix(name, "pwrite")
	var off int64
	if positional {
		if len(a) < 4 {
			p.b.Skip("unparsed_line")
			return
		}
		o, ok := parseLeadingInt(a[3])
		if !ok {
			p.b.Skip("unparsed_line")
			return
		}
		off = o
	} else {
		off = e.Cursor
		p.fdt.Advance(s, int(fd), n)
	}
	p.b.Add(trace.Op{T: t, S: s, Op: kind, H: trace.Ptr(e.Handle), Off: trace.Ptr(off), Len: trace.Ptr(n)})
}

func (p *parser) doClose(s, t int64, a []string, ret string) {
	if len(a) < 1 {
		return
	}
	fd, ok := parseLeadingInt(a[0])
	if !ok {
		return
	}
	if failedRet(ret) {
		// A failed close did not close the fd. Emit no CLOSE and leave the
		// handle open so any subsequent successful op on it still resolves.
		p.b.Skip("failed_syscall")
		return
	}
	h, hadHandle := p.fdt.Close(s, int(fd))
	if !hadHandle {
		return // directory entry or untracked fd: consume silently
	}
	p.b.Add(trace.Op{T: t, S: s, Op: trace.OpClose, H: trace.Ptr(h)})
}

func (p *parser) doFsync(s, t int64, a []string, ret string) {
	if len(a) < 1 {
		p.b.Skip("unparsed_line")
		return
	}
	fd, ok := parseLeadingInt(a[0])
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	if failedRet(ret) {
		p.b.Skip("failed_syscall")
		return
	}
	e, ok := p.fdt.Resolve(s, int(fd))
	if !ok || !e.HasHandle {
		p.b.Skip("unresolved_fd")
		return
	}
	p.b.Add(trace.Op{T: t, S: s, Op: trace.OpFsync, H: trace.Ptr(e.Handle)})
}

func (p *parser) doLseek(s int64, a []string, ret string) {
	if len(a) < 1 {
		return
	}
	fd, ok := parseLeadingInt(a[0])
	if !ok {
		return
	}
	pos, ok := parseLeadingInt(ret)
	if !ok || pos < 0 {
		return
	}
	p.fdt.SetCursor(s, int(fd), pos)
}

func (p *parser) doStatPath(s, t int64, name string, a []string, ret string) {
	if failedRet(ret) {
		// e.g. access("/missing", F_OK) = -1 ENOENT: the file was not present,
		// so it must not become a STAT target.
		p.b.Skip("failed_syscall")
		return
	}
	var pathArg, dirfdArg string
	at := false
	switch name {
	case "statx", "newfstatat":
		if len(a) < 2 {
			p.b.Skip("unparsed_line")
			return
		}
		dirfdArg, pathArg, at = a[0], a[1], true
	default:
		if len(a) < 1 {
			p.b.Skip("unparsed_line")
			return
		}
		pathArg = a[0]
	}
	path, ok := parseQuoted(pathArg)
	if !ok {
		p.b.Skip("unparsed_line")
		return
	}
	if at {
		if path == "" {
			// AT_EMPTY_PATH: the stat operates on the dirfd's file itself
			// (like fstat), not on a path under it.
			resolved, ok := p.fdTarget(s, dirfdArg)
			if !ok {
				p.b.Skip("unresolved_fd")
				return
			}
			path = resolved
		} else {
			resolved, ok := p.resolveAt(s, dirfdArg, path)
			if !ok {
				p.b.Skip("unresolved_dirfd")
				return
			}
			path = resolved
		}
	}
	tgt := p.b.Target(path, trace.TargetFile)
	p.b.Add(trace.Op{T: t, S: s, Op: trace.OpStat, Tgt: trace.Ptr(tgt)})
}

func (p *parser) doFstat(s, t int64, a []string, ret string) {
	if failedRet(ret) {
		p.b.Skip("failed_syscall")
		return
	}
	if len(a) < 1 {
		p.b.Skip("unparsed_line")
		return
	}
	path, ok := p.fdTarget(s, a[0])
	if !ok {
		p.b.Skip("unresolved_fd")
		return
	}
	tgt := p.b.Target(path, trace.TargetFile)
	p.b.Add(trace.Op{T: t, S: s, Op: trace.OpStat, Tgt: trace.Ptr(tgt)})
}

// fdTarget resolves an fd argument to a file path: it prefers the path recorded
// when the fd was opened and falls back to the strace -y/-yy path decoration
// (`3</data/file>`). ok is false if the fd is untracked and undecorated, or if
// the argument is not an fd (e.g. AT_FDCWD).
func (p *parser) fdTarget(s int64, fdArg string) (string, bool) {
	fd, deco, ok := splitFdArg(fdArg)
	if !ok {
		return "", false
	}
	if path, ok := p.fdt.PathFor(s, int(fd)); ok {
		return path, true
	}
	if deco != "" {
		return deco, true
	}
	return "", false
}

// resolveAt resolves an openat/statx path against its dirfd argument. Absolute
// paths and AT_FDCWD are returned unchanged; a relative path is joined onto the
// dirfd's recorded directory, falling back to the dirfd's strace -y/-yy path
// decoration. ok is false if a relative path names a dirfd whose directory we
// cannot determine.
func (p *parser) resolveAt(s int64, dirfdArg, path string) (string, bool) {
	if strings.HasPrefix(path, "/") {
		return path, true
	}
	if strings.HasPrefix(dirfdArg, "AT_FDCWD") {
		return path, true
	}
	fd, deco, ok := splitFdArg(dirfdArg)
	if !ok {
		return "", false
	}
	dir, ok := p.fdt.PathFor(s, int(fd))
	if !ok {
		if deco == "" {
			return "", false
		}
		dir = deco
	}
	return strings.TrimRight(dir, "/") + "/" + path, true
}

func (p *parser) stream(pid int64) int64 {
	if sid, ok := p.pidToStream[pid]; ok {
		return sid
	}
	sid := p.nextStream
	p.nextStream++
	p.pidToStream[pid] = sid
	p.streamOrder = append(p.streamOrder, pid)
	return sid
}

func (p *parser) finish() {
	for range p.unfinished {
		p.b.Skip("unfinished_dropped")
	}
	p.unfinished = nil
}

func (p *parser) notes() string {
	if len(p.streamOrder) == 0 {
		return "strace import"
	}
	var sb strings.Builder
	sb.WriteString("strace import; streams: ")
	for i, pid := range p.streamOrder {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "s%d=pid %d", p.pidToStream[pid], pid)
	}
	return sb.String()
}

// splitPID separates an optional leading PID column (digits followed by
// whitespace, present with strace -f) from the rest of the line. A line whose
// digits are immediately followed by ':' or '.' (a timestamp) has no PID column
// and maps to stream pid 0.
func splitPID(s string) (pid int64, rest string, ok bool) {
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i > 0 && i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		if v, ok := parseLeadingInt(s[:i]); ok {
			return v, strings.TrimLeft(s[i:], " \t"), true
		}
	}
	return 0, s, true
}

// parseTimePrefix parses an -tt (HH:MM:SS.frac) or -ttt (sec.frac) timestamp
// from the start of s, returning absolute nanoseconds and the remaining line.
func (p *parser) parseTimePrefix(s string) (tNS int64, body string, ok bool) {
	if len(s) >= 8 && s[2] == ':' && s[5] == ':' && isDigits(s[0:2]) && isDigits(s[3:5]) && isDigits(s[6:8]) {
		hh, _ := parseLeadingInt(s[0:2])
		mm, _ := parseLeadingInt(s[3:5])
		ss, _ := parseLeadingInt(s[6:8])
		i := 8
		var frac int64
		if i < len(s) && s[i] == '.' {
			i++
			d := i
			for d < len(s) && s[d] >= '0' && s[d] <= '9' {
				d++
			}
			frac = fracToNS(s[i:d])
			i = d
		}
		tod := (hh*3600+mm*60+ss)*1_000_000_000 + frac
		return p.applyRollover(tod), strings.TrimLeft(s[i:], " \t"), true
	}
	j := 0
	for j < len(s) && s[j] >= '0' && s[j] <= '9' {
		j++
	}
	if j > 0 && j < len(s) && s[j] == '.' {
		sec, _ := parseLeadingInt(s[:j])
		k := j + 1
		d := k
		for d < len(s) && s[d] >= '0' && s[d] <= '9' {
			d++
		}
		abs := sec*1_000_000_000 + fracToNS(s[k:d])
		return abs, strings.TrimLeft(s[d:], " \t"), true
	}
	return 0, "", false
}

// applyRollover converts an -tt time-of-day to a monotonic absolute time,
// adding a day each time the clock jumps backward by more than 12 hours
// (a midnight crossing). Small backward jitter is left for the Builder's
// per-stream clamp.
func (p *parser) applyRollover(tod int64) int64 {
	abs := tod + p.dayOffset
	if p.havePrev && abs < p.prevAbs-nsPerDay/2 {
		p.dayOffset += nsPerDay
		abs = tod + p.dayOffset
	}
	p.prevAbs = abs
	p.havePrev = true
	return abs
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// fracToNS scales a fractional-seconds digit string to nanoseconds (e.g.
// "123456" -> 123456000, "5" -> 500000000).
func fracToNS(frac string) int64 {
	if frac == "" {
		return 0
	}
	if len(frac) > 9 {
		frac = frac[:9]
	}
	v, ok := parseLeadingInt(frac)
	if !ok {
		return 0
	}
	for i := len(frac); i < 9; i++ {
		v *= 10
	}
	return v
}

// structField extracts a scalar field value from an strace-rendered struct such
// as the openat2 open_how ("{flags=O_WRONLY|O_APPEND, mode=0, resolve=0}"). It
// returns the text after "field=" up to the next comma or closing brace. ok is
// false if the field is absent.
func structField(s, field string) (string, bool) {
	key := field + "="
	i := strings.Index(s, key)
	if i < 0 {
		return "", false
	}
	v := s[i+len(key):]
	end := len(v)
	for j := 0; j < len(v); j++ {
		if v[j] == ',' || v[j] == '}' {
			end = j
			break
		}
	}
	return strings.TrimSpace(v[:end]), true
}

// parseOpenFlags maps an strace open-flags string (e.g. "O_WRONLY|O_CREAT") to
// a trace open mode and the modeled flag subset, in a fixed order for stable
// output. isDir is true for O_DIRECTORY; isApp is true for O_APPEND.
func parseOpenFlags(flagStr string) (mode trace.Mode, flags []string, isDir, isApp bool) {
	mode = trace.ModeRead
	var direct, create, trunc, sync bool
	for _, f := range strings.Split(flagStr, "|") {
		switch strings.TrimSpace(f) {
		case "O_WRONLY":
			mode = trace.ModeWrite
		case "O_RDWR":
			mode = trace.ModeReadWrite
		case "O_DIRECT":
			direct = true
		case "O_CREAT":
			create = true
		case "O_TRUNC":
			trunc = true
		case "O_APPEND":
			isApp = true
		case "O_SYNC", "O_DSYNC":
			sync = true
		case "O_DIRECTORY":
			isDir = true
		}
	}
	if direct {
		flags = append(flags, "direct")
	}
	if create {
		flags = append(flags, "create")
	}
	if trunc {
		flags = append(flags, "trunc")
	}
	if isApp {
		flags = append(flags, "append")
	}
	if sync {
		flags = append(flags, "sync")
	}
	return mode, flags, isDir, isApp
}
