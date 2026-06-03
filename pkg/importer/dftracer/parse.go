// Package dftracer imports DFTracer .pfw (Chrome Trace Event JSON) files into
// the IOFlux trace IR. Only POSIX-category open/read/write/close/lseek/fsync
// events are translated; other categories and unsupported POSIX event names
// are counted in the import Report and discarded.
//
// DFTracer records fname in most events so no dirfd resolution is needed.
// The fd table tracks open handles and sequential file cursors; an explicit
// args.offset overrides the cursor (positional I/O such as pread64).
package dftracer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const (
	captureMethod      = trace.CaptureMethod("import:dftracer")
	captureLimitations = "DFTracer POSIX trace; STDIO (fread/fwrite) and MPI-IO events not represented; " +
		"mmap page-fault I/O not captured; ops on file descriptors opened before tracing are skipped"
	generatedBy = "ioflux-import 0.1.0 / dftracer"
)

// dfEvent is the top-level Chrome Trace Event structure for a DFTracer record.
type dfEvent struct {
	Name string  `json:"name"`
	Cat  string  `json:"cat"`
	Ts   float64 `json:"ts"` // microseconds
	Pid  int64   `json:"pid"`
	Tid  int64   `json:"tid"`
	Args dfArgs  `json:"args"`
}

// dfArgs holds the POSIX event arguments. Pointer fields let the importer
// distinguish absent fields from zero values.
type dfArgs struct {
	Fname     string          `json:"fname"`
	Fd        *int64          `json:"fd"`
	Offset    *int64          `json:"offset"`
	Size      *int64          `json:"size"`
	Count     *int64          `json:"count"`      // alternative to size
	ReturnVal *int64          `json:"return_val"` // preferred return field
	Ret       *int64          `json:"ret"`        // fallback return field
	Flags     json.RawMessage `json:"flags"`      // int or string
}

// retVal returns the return value from args, preferring return_val over ret.
func (a *dfArgs) retVal() (int64, bool) {
	if a.ReturnVal != nil {
		return *a.ReturnVal, true
	}
	if a.Ret != nil {
		return *a.Ret, true
	}
	return 0, false
}

// reqCount returns the actual bytes transferred: the return value (not the
// requested count), since a short read/write still moves the cursor correctly.
func (a *dfArgs) transferCount() (int64, bool) {
	return a.retVal()
}

type pidTID struct{ pid, tid int64 }

type parser struct {
	b   *importer.Builder
	fdt *importer.FDTable

	ptToStream  map[pidTID]int64
	streamOrder []pidTID
	nextStream  int64

	parsedEvents int
	lineUnparsed int
}

func newParser() *parser {
	return &parser{
		b:          importer.NewBuilder(),
		fdt:        importer.NewFDTable(),
		ptToStream: make(map[pidTID]int64),
	}
}

// Import parses a DFTracer .pfw trace from r and writes an imported .ioflux
// trace to w. Nothing is written to w unless the produced trace passes
// validation.
func Import(r io.Reader, w io.Writer) (importer.Report, error) {
	p := newParser()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		p.line(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return importer.Report{}, fmt.Errorf("dftracer: read: %w", err)
	}

	if p.parsedEvents == 0 && p.lineUnparsed > 0 {
		return importer.Report{}, fmt.Errorf("dftracer: no parseable DFTracer event lines found; expected Chrome Trace JSON")
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

func (p *parser) line(raw string) {
	s := strings.TrimSpace(raw)
	if s == "" || s == "[" || s == "]" {
		return
	}
	// Strip trailing comma (Chrome Trace array format: "{...},").
	s = strings.TrimRight(s, ",")
	s = strings.TrimSpace(s)
	if s == "" || s == "[" || s == "]" {
		return
	}
	if len(s) == 0 || s[0] != '{' {
		// A non-empty non-bracket line that is not a JSON object cannot be a
		// Chrome Trace event; count it so non-DFTracer input can be detected.
		p.lineUnparsed++
		return
	}

	var ev dfEvent
	if err := json.Unmarshal([]byte(s), &ev); err != nil {
		p.b.Skip("unparseable_event")
		p.lineUnparsed++
		return
	}
	p.parsedEvents++

	if ev.Cat != "POSIX" {
		p.b.Skip("non_posix_event")
		return
	}

	t := int64(ev.Ts * 1000) // microseconds → nanoseconds
	stream := p.stream(pidTID{ev.Pid, ev.Tid})
	p.dispatch(stream, t, ev.Name, &ev.Args)
}

func (p *parser) dispatch(stream, t int64, name string, a *dfArgs) {
	switch name {
	case "open", "open64", "openat", "creat":
		p.doOpen(stream, t, a)
	case "read":
		p.doRW(stream, t, a, trace.OpRead)
	case "pread64", "pread":
		p.doRW(stream, t, a, trace.OpRead)
	case "write":
		p.doRW(stream, t, a, trace.OpWrite)
	case "pwrite64", "pwrite":
		p.doRW(stream, t, a, trace.OpWrite)
	case "close":
		p.doClose(stream, t, a)
	case "lseek", "lseek64":
		p.doLseek(stream, a)
	case "fsync", "fdatasync":
		p.doFsync(stream, t, a)
	default:
		p.b.Skip("unsupported_event")
	}
}

func (p *parser) doOpen(stream, t int64, a *dfArgs) {
	rv, ok := a.retVal()
	if !ok || rv < 0 {
		p.b.Skip("failed_open")
		return
	}
	if a.Fname == "" {
		p.b.Skip("unparseable_event")
		return
	}
	mode, flags, isDir, isApp := parseOpenFlags(a.Flags)
	if isDir {
		// Directory fds not needed for resolution in DFTracer (fname is always
		// present), so we discard directory opens entirely.
		return
	}
	newFD := int(rv)
	tgt := p.b.Target(a.Fname, trace.TargetFile)
	h := p.fdt.Open(stream, newFD, tgt, a.Fname, isApp)
	op := trace.Op{T: t, S: stream, Op: trace.OpOpen, Tgt: trace.Ptr(tgt), H: trace.Ptr(h), Mode: mode}
	if len(flags) > 0 {
		op.Flags = flags
	}
	p.b.Add(op)
}

func (p *parser) doRW(stream, t int64, a *dfArgs, kind trace.OpKind) {
	if a.Fd == nil {
		p.b.Skip("unresolved_fd")
		return
	}
	fd := int(*a.Fd)
	e, ok := p.fdt.Resolve(stream, fd)
	if !ok || !e.HasHandle {
		p.b.Skip("unresolved_fd")
		return
	}
	n, ok := a.transferCount()
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
		p.b.Skip("append_write_unmodeled")
		return
	}

	var off int64
	if a.Offset != nil {
		// Positional I/O (e.g. pread64): use the recorded offset and do not
		// advance the cursor (matches kernel semantics for O_PREAD).
		off = *a.Offset
	} else {
		off = e.Cursor
		p.fdt.Advance(stream, fd, n)
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: kind, H: trace.Ptr(e.Handle), Off: trace.Ptr(off), Len: trace.Ptr(n)})
}

func (p *parser) doClose(stream, t int64, a *dfArgs) {
	if a.Fd == nil {
		return
	}
	fd := int(*a.Fd)
	rv, _ := a.retVal()
	if rv < 0 {
		p.b.Skip("failed_syscall")
		return
	}
	h, hadHandle := p.fdt.Close(stream, fd)
	if !hadHandle {
		return
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: trace.OpClose, H: trace.Ptr(h)})
}

func (p *parser) doLseek(stream int64, a *dfArgs) {
	if a.Fd == nil {
		return
	}
	pos, ok := a.retVal()
	if !ok || pos < 0 {
		return
	}
	p.fdt.SetCursor(stream, int(*a.Fd), pos)
}

func (p *parser) doFsync(stream, t int64, a *dfArgs) {
	if a.Fd == nil {
		p.b.Skip("unparseable_event")
		return
	}
	rv, _ := a.retVal()
	if rv < 0 {
		p.b.Skip("failed_syscall")
		return
	}
	e, ok := p.fdt.Resolve(stream, int(*a.Fd))
	if !ok || !e.HasHandle {
		p.b.Skip("unresolved_fd")
		return
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: trace.OpFsync, H: trace.Ptr(e.Handle)})
}

func (p *parser) stream(pt pidTID) int64 {
	if sid, ok := p.ptToStream[pt]; ok {
		return sid
	}
	sid := p.nextStream
	p.nextStream++
	p.ptToStream[pt] = sid
	p.streamOrder = append(p.streamOrder, pt)
	return sid
}

func (p *parser) notes() string {
	if len(p.streamOrder) == 0 {
		return "dftracer import"
	}
	var sb strings.Builder
	sb.WriteString("dftracer import; streams: ")
	for i, pt := range p.streamOrder {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "s%d=pid %d tid %d", p.ptToStream[pt], pt.pid, pt.tid)
	}
	return sb.String()
}

// parseOpenFlags parses a DFTracer open-flags field (integer or string) into a
// trace open mode and the modeled flag subset. Both integer (raw Linux flags)
// and string ("O_WRONLY|O_CREAT") forms are accepted.
func parseOpenFlags(raw json.RawMessage) (mode trace.Mode, flags []string, isDir, isApp bool) {
	if len(raw) == 0 {
		return trace.ModeRead, nil, false, false
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		m, f, dir, app := parseIntFlags(n)
		return m, f, dir, app
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return parseStringFlags(s)
	}
	return trace.ModeRead, nil, false, false
}

// parseIntFlags maps Linux open(2) integer flags (x86-64 ABI) to a trace mode
// and modeled flag subset. O_DIRECT = 0x4000 and O_DIRECTORY = 0x10000 on
// x86-64; these values differ on other architectures, but DFTracer is
// typically deployed on x86-64 HPC/data-center nodes.
func parseIntFlags(n int64) (mode trace.Mode, flags []string, isDir, isApp bool) {
	const (
		oACCMODE   = int64(3)
		oWRONLY    = int64(1)
		oRDWR      = int64(2)
		oCREAT     = int64(64)      // 0100 octal
		oTRUNC     = int64(512)     // 01000 octal
		oAPPEND    = int64(1024)    // 02000 octal
		oDIRECT    = int64(16384)   // 040000 octal, x86-64 O_DIRECT
		oDIRECTORY = int64(65536)   // 0200000 octal, x86-64 O_DIRECTORY
		oDSYNC     = int64(4096)    // 010000 octal
		oSYNCFlag  = int64(1048576) // 04000000 octal, additional O_SYNC bit
	)
	switch n & oACCMODE {
	case oWRONLY:
		mode = trace.ModeWrite
	case oRDWR:
		mode = trace.ModeReadWrite
	default:
		mode = trace.ModeRead
	}
	isDir = n&oDIRECTORY != 0
	if n&oDIRECT != 0 {
		flags = append(flags, "direct")
	}
	if n&oCREAT != 0 {
		flags = append(flags, "create")
	}
	if n&oTRUNC != 0 {
		flags = append(flags, "trunc")
	}
	if n&oAPPEND != 0 {
		isApp = true
		flags = append(flags, "append")
	}
	if n&oDSYNC != 0 || n&oSYNCFlag != 0 {
		flags = append(flags, "sync")
	}
	return mode, flags, isDir, isApp
}

// parseStringFlags maps an open-flags string (e.g. "O_WRONLY|O_CREAT") to a
// trace mode and modeled flag subset, matching the strace flag format.
func parseStringFlags(s string) (mode trace.Mode, flags []string, isDir, isApp bool) {
	mode = trace.ModeRead
	var direct, create, trunc, sync bool
	for _, f := range strings.Split(s, "|") {
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
