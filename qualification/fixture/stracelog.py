"""Minimal strace log parser for the qual-01 reconciliation.

Deliberately independent of pkg/importer/strace: if the reconciler reused the
importer's parser, a parser bug would cancel itself out and the comparison would
prove nothing. This reads `strace -f -tt -T -y` output and yields typed calls
with an entry timestamp, a duration, resolved paths, requested counts, and
return values.

The -tt timestamp is the syscall ENTRY time and <dur> the elapsed time, verified
empirically against clock_nanosleep, so [ts, ts+dur] is a valid in-flight
interval for concurrency reconstruction.
"""

import re
from dataclasses import dataclass, field

TIME_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\.(\d+)\s+(.*)$")
CALL_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\((.*)$")


@dataclass
class Call:
    pid: int
    ts_ns: int
    name: str
    args: list
    ret: str
    dur_ns: int = 0
    # Resolved by the caller: the file path this call touched, when known.
    path: str = ""
    extras: dict = field(default_factory=dict)

    def ret_int(self):
        m = re.match(r"^\s*(-?\d+)", self.ret)
        return int(m.group(1)) if m else None


def _split_top(s: str):
    """Split an argument list on top-level commas.

    strace arguments nest quotes, angle-bracket fd decorations (-y), structs,
    and arrays, so a plain split(",") mangles them.
    """
    out, depth, buf, i, in_str = [], 0, [], 0, False
    while i < len(s):
        c = s[i]
        if in_str:
            if c == "\\":
                buf.append(c)
                if i + 1 < len(s):
                    buf.append(s[i + 1])
                    i += 1
            elif c == '"':
                in_str = False
                buf.append(c)
            else:
                buf.append(c)
        elif c == '"':
            in_str = True
            buf.append(c)
        elif c in "([{<":
            depth += 1
            buf.append(c)
        elif c in ")]}>":
            depth -= 1
            buf.append(c)
        elif c == "," and depth == 0:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _parse_time(h, m, s, frac):
    ns = int(frac.ljust(9, "0")[:9])
    return ((int(h) * 3600 + int(m) * 60 + int(s)) * 1_000_000_000) + ns


def _split_dur(ret: str):
    ret = ret.strip()
    if not ret.endswith(">"):
        return ret, 0
    open_i = ret.rfind("<")
    if open_i < 0:
        return ret, 0
    try:
        secs = float(ret[open_i + 1 : -1])
    except ValueError:
        return ret, 0
    return ret[:open_i].strip(), int(secs * 1e9)


def decorated(arg: str):
    """Split an strace -y fd argument `3</path/to/file>` into (fd, path)."""
    m = re.match(r"^(-?\d+)<(.*)>$", arg.strip())
    if m:
        return int(m.group(1)), m.group(2)
    m = re.match(r"^(-?\d+)$", arg.strip())
    if m:
        return int(m.group(1)), ""
    return None, ""


def parse(path: str):
    """Yield Call records from an strace log, joining unfinished/resumed halves."""
    pending = {}
    calls = []
    day = 0
    prev = None
    with open(path, "r", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            pid = 0
            m = re.match(r"^(\d+)\s+(.*)$", line)
            if m:
                pid, line = int(m.group(1)), m.group(2)
            tm = TIME_RE.match(line.strip())
            if not tm:
                continue
            ts = _parse_time(*tm.groups()[:4])
            ts += day
            if prev is not None and ts < prev - 12 * 3600 * 1_000_000_000:
                day += 24 * 3600 * 1_000_000_000
                ts += 24 * 3600 * 1_000_000_000
            prev = ts
            body = tm.group(5).strip()
            if body.startswith("---") or body.startswith("+++"):
                continue

            if "<unfinished ...>" in body:
                head = body.split("<unfinished ...>")[0].strip()
                cm = CALL_RE.match(head)
                if cm:
                    pending[pid] = (cm.group(1), cm.group(2), ts)
                continue
            if body.startswith("<..."):
                idx = body.find("resumed>")
                if idx < 0 or pid not in pending:
                    continue
                name, args_head, ts0 = pending.pop(pid)
                body = name + "(" + args_head + body[idx + len("resumed>") :]
                ts = ts0

            cm = CALL_RE.match(body)
            if not cm:
                continue
            name, rest = cm.group(1), cm.group(2)
            eq = rest.rfind(") = ")
            if eq < 0:
                continue
            args = _split_top(rest[:eq])
            ret, dur = _split_dur(rest[eq + len(") = ") :])
            calls.append(Call(pid=pid, ts_ns=ts, name=name, args=args, ret=ret, dur_ns=dur))
    return calls


# ---------------------------------------------------------------------------
# Semantic resolution: turn raw calls into (op, path, off, requested, returned)


READ_NAMES = {"read", "pread64", "pread", "readv", "preadv", "preadv2"}
OPEN_NAMES = {"open", "openat", "openat2"}
STAT_NAMES = {"stat", "statx", "newfstatat", "fstat", "lstat", "access"}


def resolve(calls, keep_prefix: str):
    """Resolve fds to paths and project calls onto fixture-relevant operations.

    Only calls touching a path under keep_prefix are returned.

    fd->path resolution ALWAYS prefers the strace -y decoration, which strace
    reads from /proc/<pid>/fd and is therefore kernel truth at call time. The
    locally built fd table is only a fallback for an undecorated argument.
    The order matters: strace's pid column is a *tid*, while a descriptor table
    is shared by every thread in a process. A multi-threaded tracee (a Go
    replay worker, where a goroutine can open a file on one OS thread and read
    it from another) therefore makes a (tid, fd) table go stale and silently
    resolve reads to a file some other thread had open on the same fd number.
    Single-threaded tracees like the fixture's workers do not expose this, so
    trusting the table would fail only on the replay side -- exactly where a
    false disagreement is most expensive.
    """
    fdpath = {}  # (pid, fd) -> path, fallback only
    out = []
    for c in calls:
        if c.name in OPEN_NAMES:
            retfd, retpath = decorated(c.ret)
            if retfd is None or retfd < 0:
                continue
            if c.name == "open":
                path = _unquote(c.args[0]) if c.args else ""
            else:
                dirfd, dirpath = decorated(c.args[0]) if c.args else (None, "")
                rel = _unquote(c.args[1]) if len(c.args) > 1 else ""
                if rel.startswith("/"):
                    path = rel
                elif c.args and c.args[0].startswith("AT_FDCWD"):
                    path = rel
                else:
                    base = dirpath or fdpath.get((c.pid, dirfd), "")
                    path = base.rstrip("/") + "/" + rel if base else rel
            if retpath:
                path = retpath
            fdpath[(c.pid, retfd)] = path
            if path.startswith(keep_prefix):
                c.path = path
                c.extras["kind"] = "OPEN"
                out.append(c)
            continue

        if c.name == "close":
            fd, dpath = decorated(c.args[0]) if c.args else (None, "")
            path = dpath or fdpath.pop((c.pid, fd), "")
            fdpath.pop((c.pid, fd), None)
            if path and path.startswith(keep_prefix):
                c.path = path
                c.extras["kind"] = "CLOSE"
                out.append(c)
            continue

        if c.name in READ_NAMES:
            fd, dpath = decorated(c.args[0]) if c.args else (None, "")
            path = dpath or fdpath.get((c.pid, fd)) or ""
            if not path or not path.startswith(keep_prefix):
                continue
            requested = _arg_int(c.args, 2)
            off = _arg_int(c.args, 3) if c.name.startswith("pread") else None
            c.path = path
            c.extras["kind"] = "READ"
            c.extras["requested"] = requested
            c.extras["returned"] = c.ret_int()
            c.extras["explicit_off"] = off
            c.extras["syscall"] = c.name
            out.append(c)
            continue

        if c.name in STAT_NAMES:
            if c.name == "fstat":
                fd, dpath = decorated(c.args[0]) if c.args else (None, "")
                path = dpath or fdpath.get((c.pid, fd)) or ""
            elif c.name in ("statx", "newfstatat"):
                dirfd, dirpath = decorated(c.args[0]) if c.args else (None, "")
                rel = _unquote(c.args[1]) if len(c.args) > 1 else ""
                if rel.startswith("/"):
                    path = rel
                elif c.args and c.args[0].startswith("AT_FDCWD"):
                    path = rel
                else:
                    base = dirpath or fdpath.get((c.pid, dirfd), "")
                    path = base.rstrip("/") + "/" + rel if base else rel
            else:
                path = _unquote(c.args[0]) if c.args else ""
            if not path or not path.startswith(keep_prefix):
                continue
            c.path = path
            c.extras["kind"] = "FSTAT" if c.name == "fstat" else "STAT"
            c.extras["syscall"] = c.name
            out.append(c)
            continue

        if c.name in ("lseek", "_llseek"):
            fd, dpath = decorated(c.args[0]) if c.args else (None, "")
            path = dpath or fdpath.get((c.pid, fd)) or ""
            if path and path.startswith(keep_prefix):
                c.path = path
                c.extras["kind"] = "LSEEK"
                out.append(c)
            continue

        # Cache-control and durability calls are retained rather than dropped:
        # they are what marks the boundary between a replay's preparation phase
        # and its measured run, and dropping them here made that boundary
        # invisible to the caller.
        if c.name in ("fadvise64", "fadvise64_64", "posix_fadvise", "fsync", "fdatasync"):
            fd, dpath = decorated(c.args[0]) if c.args else (None, "")
            path = dpath or fdpath.get((c.pid, fd)) or ""
            if path and path.startswith(keep_prefix):
                c.path = path
                c.extras["kind"] = "FADVISE" if "advise" in c.name else "FSYNC"
                c.extras["syscall"] = c.name
                out.append(c)
            continue

    return out


def _unquote(arg: str) -> str:
    arg = arg.strip()
    if arg.startswith('"'):
        end = arg.rfind('"')
        if end > 0:
            return arg[1:end]
    return arg


def _arg_int(args, idx):
    if idx >= len(args):
        return None
    m = re.match(r"^\s*(-?\d+)", args[idx])
    return int(m.group(1)) if m else None


def sequential_offsets(events):
    """Attach a derived offset to sequential reads by tracking a per-fd cursor.

    read(2) carries no offset argument, so a sequential read's offset is the
    file cursor. Reconstructed per (pid, path) since each fixture worker holds
    one descriptor per shard at a time.
    """
    cursor = {}
    for e in events:
        if e.extras.get("kind") == "OPEN":
            cursor[(e.pid, e.path)] = 0
        elif e.extras.get("kind") == "READ":
            key = (e.pid, e.path)
            if e.extras.get("explicit_off") is not None:
                e.extras["off"] = e.extras["explicit_off"]
            else:
                e.extras["off"] = cursor.get(key, 0)
                cursor[key] = e.extras["off"] + max(0, e.extras.get("returned") or 0)
    return events
