// Package dftracer imports DFTracer .pfw (Chrome Trace Event JSON) files into
// the IOFlux trace IR. Only POSIX-category open/read/write/close/lseek/fsync
// events are translated; other categories and unsupported POSIX event names
// are counted in the import Report and discarded.
//
// Two on-disk variants are supported:
//
//   - Literal form: each event carries args.fname (the path), args.fd (the
//     descriptor), and, for positional I/O, args.offset.
//   - Hashed form (DFTracer 2.x): events reference files by args.fhash, a hash
//     resolved to a path via "FH" metadata events, and omit fd and offset
//     entirely. The importer builds the fhash→path table in a metadata pre-pass,
//     keys handles by file hash, and reconstructs offsets from per-file
//     sequential cursors.
//
// The fd table tracks open handles and sequential file cursors; an explicit
// args.offset overrides the cursor (positional I/O such as pread64).
package dftracer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/chanuollala/ioflux/pkg/importer"
	"github.com/chanuollala/ioflux/pkg/trace"
)

const (
	captureMethod      = trace.CaptureMethod("import:dftracer")
	captureLimitations = "DFTracer POSIX trace; STDIO (fread/fwrite) and MPI-IO events not represented; " +
		"mmap page-fault I/O not captured; ops on file descriptors opened before tracing are skipped; " +
		"cross-thread fd sharing not modeled (fd opened by one thread is unresolved when accessed by another); " +
		"hashed-filename traces (DFTracer 2.x) resolve paths via FH metadata and reconstruct read/write " +
		"offsets from sequential per-file cursors, since that format records byte counts but no offsets"
	generatedBy = "ioflux-import 0.1.0 / dftracer"
)

// synthFDBase is the starting value for synthetic descriptors assigned to
// hashed-form file references. It is far above any real fd so synthetic and
// literal descriptors never collide if a trace mixes both forms.
const synthFDBase = 1 << 30

// dfEvent is the top-level Chrome Trace Event structure for a DFTracer record.
type dfEvent struct {
	Name string  `json:"name"`
	Cat  string  `json:"cat"`
	Ts   float64 `json:"ts"`  // microseconds
	Dur  float64 `json:"dur"` // microseconds
	Pid  int64   `json:"pid"`
	Tid  int64   `json:"tid"`
	Args dfArgs  `json:"args"`
}

// dfArgs holds the POSIX event arguments. Pointer fields let the importer
// distinguish absent fields from zero values. Count, ReturnVal, and Ret use
// json.RawMessage because older DFTracer versions serialized numeric values as
// JSON strings (e.g. "ret":"131072"); rawInt handles both forms.
//
// Fhash references a file by hash (DFTracer 2.x); MetaName/MetaValue carry the
// path and hash of an "FH" metadata event (args.name / args.value).
type dfArgs struct {
	Fname     string          `json:"fname"`
	Fhash     string          `json:"fhash"`
	Fd        *int64          `json:"fd"`
	Offset    *int64          `json:"offset"`
	Size      *int64          `json:"size"`
	Count     json.RawMessage `json:"count"`
	ReturnVal json.RawMessage `json:"return_val"` // preferred return field
	Ret       json.RawMessage `json:"ret"`        // fallback return field
	Flags     json.RawMessage `json:"flags"`      // int or string
	MetaName  string          `json:"name"`       // FH metadata: the file path
	MetaValue string          `json:"value"`      // FH metadata: the file hash
}

// rawInt parses a JSON value that may be either a JSON number or a quoted
// decimal string. Older DFTracer versions serialized some numeric fields as
// strings (e.g. "ret":"131072"); both forms are accepted.
func rawInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, true
		}
	}
	return 0, false
}

// retVal returns the return value from args, preferring return_val over ret.
func (a *dfArgs) retVal() (int64, bool) {
	if n, ok := rawInt(a.ReturnVal); ok {
		return n, true
	}
	return rawInt(a.Ret)
}

// transferCount returns the actual bytes transferred: the return value (not the
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

	// fhashToPath maps a DFTracer file hash to its path, collected from FH
	// metadata events in a pre-pass. fhashFD assigns each file hash a stable
	// synthetic descriptor so the fd table can track hashed-form handles.
	fhashToPath map[string]string
	fhashFD     map[string]int
	nextSynthFD int

	parsedEvents int
	lineUnparsed int
}

func newParser() *parser {
	return &parser{
		b:           importer.NewBuilder(),
		fdt:         importer.NewFDTable(),
		ptToStream:  make(map[pidTID]int64),
		fhashToPath: make(map[string]string),
		fhashFD:     make(map[string]int),
	}
}

// Import parses a DFTracer .pfw trace from r and writes an imported .ioflux
// trace to w. Nothing is written to w unless the produced trace passes
// validation.
//
// Hashed-form traces define fhash→path in FH metadata events that may precede
// the POSIX events using them, so the input is buffered and scanned twice: a
// metadata pre-pass builds the hash table, then op translation runs.
func Import(r io.Reader, w io.Writer) (importer.Report, error) {
	p := newParser()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return importer.Report{}, fmt.Errorf("dftracer: read: %w", err)
	}

	for _, ln := range lines {
		p.scanMeta(ln) // pass 1: fhash→path from FH metadata
	}
	for _, ln := range lines {
		p.line(ln) // pass 2: translate events
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

// scanMeta records fhash→path mappings from FH metadata events. It updates no
// report counters; the translation pass owns those.
func (p *parser) scanMeta(raw string) {
	s := strings.TrimSpace(raw)
	if len(s) == 0 || s[0] != '{' {
		return
	}
	s = strings.TrimRight(s, ",")
	var ev dfEvent
	if json.Unmarshal([]byte(s), &ev) != nil {
		return
	}
	if ev.Cat == "dftracer" && ev.Name == "FH" && ev.Args.MetaValue != "" && ev.Args.MetaName != "" {
		p.fhashToPath[ev.Args.MetaValue] = ev.Args.MetaName
	}
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

	switch ev.Cat {
	case "POSIX":
		t := int64(ev.Ts * 1000) // microseconds → nanoseconds
		var dur *int64
		if ev.Dur > 0 {
			ns := int64(ev.Dur * 1000)
			dur = &ns
		}
		stream := p.stream(pidTID{ev.Pid, ev.Tid})
		p.dispatch(stream, t, ev.Name, dur, &ev.Args)
	case "dftracer":
		// Trace bookkeeping (FH/HH/SH/start/end/thread_name): consumed in the
		// metadata pass, not an I/O op — neither emitted nor counted as skipped.
	default:
		p.b.Skip("non_posix_event")
	}
}

func (p *parser) dispatch(stream, t int64, name string, dur *int64, a *dfArgs) {
	switch name {
	case "open", "open64", "openat", "creat":
		p.doOpen(stream, t, dur, a)
	case "read":
		p.doRW(stream, t, dur, a, trace.OpRead, false)
	case "pread64", "pread":
		p.doRW(stream, t, dur, a, trace.OpRead, true)
	case "write":
		p.doRW(stream, t, dur, a, trace.OpWrite, false)
	case "pwrite64", "pwrite":
		p.doRW(stream, t, dur, a, trace.OpWrite, true)
	case "close":
		p.doClose(stream, t, dur, a)
	case "lseek", "lseek64":
		p.doLseek(stream, a)
	case "fsync", "fdatasync":
		p.doFsync(stream, t, dur, a)
	default:
		p.b.Skip("unsupported_event")
	}
}

// synthFD returns a stable synthetic descriptor for a file hash, assigning a new
// one on first sighting. The fd table then tracks hashed-form handles through
// the same code path as literal descriptors.
func (p *parser) synthFD(fhash string) int {
	if fd, ok := p.fhashFD[fhash]; ok {
		return fd
	}
	fd := synthFDBase + p.nextSynthFD
	p.nextSynthFD++
	p.fhashFD[fhash] = fd
	return fd
}

// resolvePath returns the file path for an event: the literal fname when
// present, otherwise the path mapped from fhash via FH metadata. ok is false
// when a hashed reference names an undefined hash, or no file is identified.
func (p *parser) resolvePath(a *dfArgs) (string, bool) {
	if a.Fname != "" {
		return a.Fname, true
	}
	if a.Fhash != "" {
		path, ok := p.fhashToPath[a.Fhash]
		return path, ok
	}
	return "", false
}

// resolveFDKey returns the fd-table key for an I/O event: the literal fd when
// present, otherwise a synthetic descriptor derived from the file hash. ok is
// false when the event identifies no descriptor or hash.
func (p *parser) resolveFDKey(a *dfArgs) (int, bool) {
	if a.Fd != nil {
		return int(*a.Fd), true
	}
	if a.Fhash != "" {
		return p.synthFD(a.Fhash), true
	}
	return 0, false
}

func (p *parser) doOpen(stream, t int64, dur *int64, a *dfArgs) {
	rv, hasRV := a.retVal()
	if hasRV && rv < 0 {
		p.b.Skip("failed_open")
		return
	}
	path, ok := p.resolvePath(a)
	if !ok {
		if a.Fhash != "" {
			p.b.Skip("unresolved_fhash")
		} else {
			p.b.Skip("unparseable_event")
		}
		return
	}
	mode, flags, isDir, isApp := parseOpenFlags(a.Flags)
	if isDir {
		// Directory fds not needed for resolution in DFTracer (the path is
		// always recoverable), so directory opens are discarded entirely.
		return
	}

	// Descriptor key: literal form returns the fd in return_val; hashed form
	// carries no fd, so a synthetic descriptor is keyed off the file hash.
	var fdKey int
	switch {
	case a.Fd != nil:
		fdKey = int(*a.Fd)
	case hasRV:
		fdKey = int(rv)
	case a.Fhash != "":
		fdKey = p.synthFD(a.Fhash)
	default:
		p.b.Skip("failed_open")
		return
	}

	tgt := p.b.Target(path, trace.TargetFile)
	h := p.fdt.Open(stream, fdKey, tgt, path, isApp)
	op := trace.Op{T: t, S: stream, Op: trace.OpOpen, Tgt: trace.Ptr(tgt), H: trace.Ptr(h), Mode: mode, Dur: dur}
	if len(flags) > 0 {
		op.Flags = flags
	}
	p.b.Add(op)
}

func (p *parser) doRW(stream, t int64, dur *int64, a *dfArgs, kind trace.OpKind, positional bool) {
	fdKey, ok := p.resolveFDKey(a)
	if !ok {
		p.b.Skip("unresolved_fd")
		return
	}
	e, ok := p.fdt.Resolve(stream, fdKey)
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
	if positional && a.Offset != nil {
		// Positional I/O (pread64/pwrite64): use the recorded offset and do not
		// advance the cursor (matches kernel semantics for pread/pwrite).
		off = *a.Offset
	} else {
		off = e.Cursor
		p.fdt.Advance(stream, fdKey, n)
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: kind, H: trace.Ptr(e.Handle), Off: trace.Ptr(off), Len: trace.Ptr(n), Dur: dur})
}

func (p *parser) doClose(stream, t int64, dur *int64, a *dfArgs) {
	fdKey, ok := p.resolveFDKey(a)
	if !ok {
		return
	}
	rv, _ := a.retVal()
	if rv < 0 {
		p.b.Skip("failed_syscall")
		return
	}
	h, hadHandle := p.fdt.Close(stream, fdKey)
	if !hadHandle {
		return
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: trace.OpClose, H: trace.Ptr(h), Dur: dur})
}

func (p *parser) doLseek(stream int64, a *dfArgs) {
	fdKey, ok := p.resolveFDKey(a)
	if !ok {
		return
	}
	pos, ok := a.retVal()
	if !ok || pos < 0 {
		return
	}
	p.fdt.SetCursor(stream, fdKey, pos)
}

func (p *parser) doFsync(stream, t int64, dur *int64, a *dfArgs) {
	fdKey, ok := p.resolveFDKey(a)
	if !ok {
		p.b.Skip("unparseable_event")
		return
	}
	rv, _ := a.retVal()
	if rv < 0 {
		p.b.Skip("failed_syscall")
		return
	}
	e, ok := p.fdt.Resolve(stream, fdKey)
	if !ok || !e.HasHandle {
		p.b.Skip("unresolved_fd")
		return
	}
	p.b.Add(trace.Op{T: t, S: stream, Op: trace.OpFsync, H: trace.Ptr(e.Handle), Dur: dur})
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
