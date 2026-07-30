"""qual-01 live-versus-replay reconciliation (plan.md §10).

Compares four artifacts across the dimensions §10.1 requires:

  A  analytic oracle  vs  application journal  vs  kernel counters
       -- establishes that the fixture did exactly what its spec says, using
          three independent instruments, before any tracer is trusted.
  B  live strace      vs  analytic oracle
       -- capture adequacy against an independent denominator (§9.4).
  C  imported trace   vs  analytic oracle
       -- what survived import: which operations, with which fields.
  D  replay strace    vs  imported trace
       -- execution validity and semantic conformance of the replay itself,
          observed from outside rather than self-reported.
  E  outstanding-I/O depth waveform, live vs replay.
  F  timing, reported but explicitly NOT claimed as comparable.

Nothing here is tuned to agree. Every comparison reports the residual.
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import oracle  # noqa: E402
import spec  # noqa: E402
import stracelog  # noqa: E402

PAGE = 4096


def roundup(n, m):
    return ((n + m - 1) // m) * m


# ---------------------------------------------------------------------------
# loaders


def load_journals(d: Path):
    out = {}
    for p in sorted(d.glob("worker-*.json")):
        j = json.loads(p.read_text())
        out[j["worker"]] = j
    return out


def load_trace(path: Path):
    lines = path.read_text().splitlines()
    header = json.loads(lines[0])
    ops = [json.loads(x) for x in lines[1:] if x.strip()]
    return header, ops


def stream_to_pid(header):
    """Recover stream -> source pid from the importer's header notes."""
    out = {}
    for m in re.finditer(r"s(\d+)=pid (\d+)", header.get("notes", "")):
        out[int(m.group(1))] = int(m.group(2))
    return out


def trace_sequences(header, ops):
    """Per-stream op sequences with handles resolved to target paths."""
    names = {t["id"]: t["name"] for t in header["targets"]}
    seqs = {}
    handle_target = {}
    for op in ops:
        s = op["s"]
        kind = op["op"]
        entry = {"op": kind, "op_id": op.get("op_id"), "t": op.get("t")}
        if kind == "OPEN":
            handle_target[op["h"]] = names[op["tgt"]]
            entry["path"] = names[op["tgt"]]
            entry["mode"] = op.get("mode")
        elif kind == "STAT":
            entry["path"] = names[op["tgt"]]
        elif kind in ("READ", "WRITE"):
            entry["path"] = handle_target.get(op.get("h"), "?")
            entry["off"] = op.get("off")
            entry["len"] = op.get("len")
        elif kind == "CLOSE":
            entry["path"] = handle_target.get(op.get("h"), "?")
        seqs.setdefault(s, []).append(entry)
    return seqs


def worker_of_path(path: str):
    m = re.search(r"shard_(\d+)\.bin$", path)
    if not m:
        return None
    return int(m.group(1)) % spec.WORKERS


# ---------------------------------------------------------------------------
# A: oracle self-consistency


def section_a(dataset_dir, journals):
    res = {"per_worker": {}, "mismatches": []}
    exp_all = oracle.expected_all(dataset_dir)
    for w, j in sorted(journals.items()):
        expected = exp_all[w]
        got = j["journal"]
        seq_equal = expected == got
        if not seq_equal:
            for i, (e, g) in enumerate(zip(expected, got)):
                if e != g:
                    res["mismatches"].append(
                        {"worker": w, "index": i, "expected": e, "got": g}
                    )
                    break
            if len(expected) != len(got):
                res["mismatches"].append(
                    {"worker": w, "length": [len(expected), len(got)]}
                )

        s = j["io_samples"]
        # A snapshot's own read(2) lands between s1 and s2; subtract exactly it.
        cal_ops = 1
        cal_bytes = s["s1"].get("_nbytes")
        exp_reads = sum(1 for e in expected if e["op"] == "READ")
        exp_bytes = sum(e["returned"] for e in expected if e["op"] == "READ")
        shards = len(spec.shards_for_worker(w))

        syscr = s["s2"]["syscr"] - s["s1"]["syscr"] - cal_ops
        rchar = s["s2"]["rchar"] - s["s1"]["rchar"]
        rchar_cal = (rchar - cal_bytes) if cal_bytes is not None else None
        read_bytes = s["s2"]["read_bytes"] - s["s1"]["read_bytes"]
        exp_device = shards * roundup(spec.SHARD_BYTES, PAGE)

        res["per_worker"][w] = {
            "pid": j["pid"],
            "journal_matches_analytic": seq_equal,
            "journal_len": len(got),
            "kernel_syscr": syscr,
            "expected_read_syscalls": exp_reads,
            "syscr_matches": syscr == exp_reads,
            "kernel_rchar_calibrated": rchar_cal,
            "expected_read_bytes": exp_bytes,
            "rchar_matches": rchar_cal == exp_bytes,
            "kernel_read_bytes": read_bytes,
            "expected_device_bytes_if_cold": exp_device,
            "cold_recipe_effective": read_bytes >= exp_device,
            "phase_ns": j["phase_ns"],
        }
    return res


# ---------------------------------------------------------------------------
# B: live capture vs analytic oracle


def kind_of(ev):
    return ev.extras.get("kind")


def live_events_by_worker(events, journals):
    pid_to_worker = {j["pid"]: w for w, j in journals.items()}
    out = {}
    unattributed = 0
    for ev in events:
        w = pid_to_worker.get(ev.pid)
        if w is None:
            unattributed += 1
            continue
        out.setdefault(w, []).append(ev)
    return out, unattributed


def observed_seq(events):
    """Project resolved strace events onto the journal's entry shape."""
    seq = []
    for ev in events:
        k = kind_of(ev)
        if k == "OPEN":
            seq.append({"op": "OPEN", "path": ev.path})
        elif k == "FSTAT":
            seq.append({"op": "FSTAT", "path": ev.path, "size": None})
        elif k == "READ":
            seq.append(
                {
                    "op": "READ",
                    "path": ev.path,
                    "off": ev.extras.get("off"),
                    "requested": ev.extras.get("requested"),
                    "returned": ev.extras.get("returned"),
                    "syscall": ev.extras.get("syscall"),
                }
            )
        elif k == "CLOSE":
            seq.append({"op": "CLOSE", "path": ev.path})
        elif k == "STAT":
            seq.append({"op": "STAT", "path": ev.path})
        elif k == "LSEEK":
            seq.append({"op": "LSEEK", "path": ev.path})
    return seq


def compare_to_oracle(expected, observed, check_requested=True):
    """Element-wise comparison; returns per-dimension agreement counts."""
    r = {
        "expected_ops": len(expected),
        "observed_ops": len(observed),
        "order_agreement": "exact",
        "op_kind_mismatch": 0,
        "path_mismatch": 0,
        "offset_mismatch": 0,
        "requested_mismatch": 0,
        "returned_mismatch": 0,
        "examples": [],
    }
    if len(expected) != len(observed):
        r["order_agreement"] = "length differs"
    for i in range(min(len(expected), len(observed))):
        e, o = expected[i], observed[i]
        if e["op"] != o["op"]:
            r["op_kind_mismatch"] += 1
            r["order_agreement"] = "diverges"
            if len(r["examples"]) < 4:
                r["examples"].append({"index": i, "expected": e, "observed": o})
            continue
        if e.get("path") != o.get("path"):
            r["path_mismatch"] += 1
        if e["op"] == "READ":
            if e.get("off") != o.get("off"):
                r["offset_mismatch"] += 1
                if len(r["examples"]) < 4:
                    r["examples"].append(
                        {"index": i, "dim": "off", "expected": e, "observed": o}
                    )
            if check_requested and e.get("requested") != o.get("requested"):
                r["requested_mismatch"] += 1
                if len(r["examples"]) < 4:
                    r["examples"].append(
                        {"index": i, "dim": "requested", "expected": e, "observed": o}
                    )
            if e.get("returned") != o.get("returned"):
                r["returned_mismatch"] += 1
                if len(r["examples"]) < 4:
                    r["examples"].append(
                        {"index": i, "dim": "returned", "expected": e, "observed": o}
                    )
    return r


def section_b(dataset_dir, journals, live_events):
    by_worker, unattributed = live_events_by_worker(live_events, journals)
    exp_all = oracle.expected_all(dataset_dir)
    res = {"unattributed_events": unattributed, "per_worker": {}}
    for w in sorted(exp_all):
        obs = observed_seq(by_worker.get(w, []))
        res["per_worker"][w] = compare_to_oracle(exp_all[w], obs)
        res["per_worker"][w]["read_syscall_names"] = sorted(
            {
                e.extras.get("syscall")
                for e in by_worker.get(w, [])
                if kind_of(e) == "READ"
            }
        )
    return res


# ---------------------------------------------------------------------------
# C: imported trace vs analytic oracle


def importable(expected):
    """The oracle sequence minus what the importer documents that it drops.

    strace's importer skips a read that returned 0 (`eof_read`). That is a real
    operation the application issued, so it stays in the oracle and is accounted
    for here as a declared, explained drop rather than silently removed.
    """
    out, dropped = [], []
    for e in expected:
        if e["op"] == "READ" and e["returned"] == 0:
            dropped.append(e)
            continue
        out.append(e)
    return out, dropped


def section_c(dataset_dir, journals, header, ops):
    seqs = trace_sequences(header, ops)
    s2p = stream_to_pid(header)
    pid_to_worker = {j["pid"]: w for w, j in journals.items()}
    exp_all = oracle.expected_all(dataset_dir)

    res = {
        "trace_ops": len(ops),
        "trace_streams": len(seqs),
        "stream_to_worker": {},
        "declared_drops": {},
        "per_worker": {},
        "len_semantics": {},
    }
    matched_returned = matched_requested = total_reads = 0
    ambiguous = 0

    for s, seq in sorted(seqs.items()):
        pid = s2p.get(s)
        w = pid_to_worker.get(pid)
        res["stream_to_worker"][s] = {"pid": pid, "worker": w}
        if w is None:
            continue
        exp, dropped = importable(exp_all[w])
        res["declared_drops"][w] = {"eof_read": len(dropped)}

        # Project the trace onto the oracle's vocabulary. The trace has one
        # length field, so it is compared against BOTH requested and returned
        # to determine, quantitatively, which one it holds.
        obs = []
        for e in seq:
            if e["op"] == "OPEN":
                obs.append({"op": "OPEN", "path": e["path"]})
            elif e["op"] == "STAT":
                obs.append({"op": "FSTAT", "path": e["path"], "size": None})
            elif e["op"] == "READ":
                obs.append(
                    {
                        "op": "READ",
                        "path": e["path"],
                        "off": e["off"],
                        "requested": e["len"],
                        "returned": e["len"],
                    }
                )
            elif e["op"] == "CLOSE":
                obs.append({"op": "CLOSE", "path": e["path"]})
        res["per_worker"][w] = compare_to_oracle(exp, obs, check_requested=False)

        for i in range(min(len(exp), len(obs))):
            if exp[i]["op"] != "READ" or obs[i]["op"] != "READ":
                continue
            total_reads += 1
            tl = obs[i]["returned"]
            if tl == exp[i]["returned"]:
                matched_returned += 1
            if tl == exp[i]["requested"]:
                matched_requested += 1
            if exp[i]["requested"] == exp[i]["returned"]:
                ambiguous += 1

    res["len_semantics"] = {
        "trace_reads_compared": total_reads,
        "len_equals_source_returned": matched_returned,
        "len_equals_source_requested": matched_requested,
        "indistinguishable_because_full_transfer": ambiguous,
        "discriminating_reads": total_reads - ambiguous,
    }
    return res


# ---------------------------------------------------------------------------
# D: replay execution vs the trace it replayed


def split_replay_phases(events):
    """Separate cache-control/preparation syscalls from the measured run.

    ioflux applies the cold recipe (sync + fadvise DONTNEED on every target)
    after preparation and before the scheduler starts, so the last fadvise is a
    hard boundary: dataset syscalls after it belong to the run. Both the
    boundary and the counts either side of it are reported so the split is
    visible rather than assumed.

    Returns (run, prep, boundary_index). boundary_index is None when no cache
    control was observed, in which case NOTHING is claimed to be preparation --
    the caller must treat the split as unavailable rather than assume the whole
    log is the run.
    """
    last = None
    for i, ev in enumerate(events):
        if ev.extras.get("kind") == "FADVISE":
            last = i
    if last is None:
        return events, [], None
    # The recipe's per-target lifecycle is open, fsync, fadvise, close, so the
    # final target's close lands *after* the last fadvise. Absorb exactly that
    # one call, identified by matching its path, rather than leaving it to be
    # reported as an extra operation the replay invented.
    end = last
    tail_path = events[last].path
    for j in range(last + 1, len(events)):
        if events[j].extras.get("kind") == "CLOSE" and events[j].path == tail_path:
            end = j
            break
        if events[j].extras.get("kind") in ("READ", "OPEN"):
            break
    prep, run = events[: end + 1], events[end + 1 :]
    if end != last:
        # Keep any op between the boundary calls in the run phase, not silently
        # in prep: only the matched close is absorbed.
        absorbed = events[last + 1 : end]
        prep = events[: last + 1] + [events[end]]
        run = absorbed + events[end + 1 :]
    return run, prep, last


def section_d(header, ops, journals, replay_events, replay_root, dataset_dir):
    seqs = trace_sequences(header, ops)
    s2p = stream_to_pid(header)
    pid_to_worker = {j["pid"]: w for w, j in journals.items()}
    stream_of_worker = {}
    for s, pid in s2p.items():
        w = pid_to_worker.get(pid)
        if w is not None:
            stream_of_worker[w] = s

    run_events, prep_events, boundary = split_replay_phases(replay_events)

    # Replay runs one goroutine per stream over a shared thread pool, so an OS
    # tid does not identify a stream. Ownership is recovered from the target
    # instead: the fixture's shard->worker assignment is static and disjoint,
    # so the shard index in each path names its stream unambiguously.
    #
    # The containment root's own directory descriptor (os.Root) is not a shard
    # and is counted separately rather than reported as an unexplained op.
    by_worker = {}
    root_dir_events = []
    unattributed = []
    for ev in run_events:
        if ev.path.rstrip("/") == replay_root.rstrip("/"):
            root_dir_events.append(ev)
            continue
        w = worker_of_path(ev.path)
        if w is None:
            unattributed.append(ev.path)
            continue
        by_worker.setdefault(w, []).append(ev)

    def by_kind(evs):
        out = {}
        for e in evs:
            k = e.extras.get("kind", "?")
            out[k] = out.get(k, 0) + 1
        return out

    res = {
        "phase_boundary_found": boundary is not None,
        "prepare_phase_events": len(prep_events),
        "prepare_phase_by_kind": by_kind(prep_events),
        "run_phase_events": len(run_events),
        "run_phase_by_kind": by_kind(run_events),
        "root_dir_handle_events": len(root_dir_events),
        "unattributed_run_events": len(unattributed),
        "per_worker": {},
        "syscall_substitutions": {},
    }
    read_names, stat_names = set(), set()
    total = {
        "expected": 0,
        "observed": 0,
        "kind": 0,
        "path": 0,
        "off": 0,
        "requested": 0,
        "returned": 0,
    }

    for w in sorted(stream_of_worker):
        s = stream_of_worker[w]
        # What the trace told replay to do, in the trace's own terms.
        exp = []
        for e in seqs.get(s, []):
            path = e.get("path", "")
            rel = path.replace(dataset_dir.rstrip("/"), replay_root.rstrip("/"), 1)
            if e["op"] == "OPEN":
                exp.append({"op": "OPEN", "path": rel})
            elif e["op"] == "STAT":
                exp.append({"op": "FSTAT", "path": rel, "size": None})
            elif e["op"] == "READ":
                exp.append(
                    {
                        "op": "READ",
                        "path": rel,
                        "off": e["off"],
                        "requested": e["len"],
                        "returned": e["len"],
                    }
                )
            elif e["op"] == "CLOSE":
                exp.append({"op": "CLOSE", "path": rel})

        obs = []
        for ev in by_worker.get(w, []):
            k = kind_of(ev)
            if k in ("FSTAT", "STAT"):
                stat_names.add(ev.extras.get("syscall"))
                obs.append({"op": "FSTAT", "path": ev.path, "size": None})
            elif k == "READ":
                read_names.add(ev.extras.get("syscall"))
                obs.append(
                    {
                        "op": "READ",
                        "path": ev.path,
                        "off": ev.extras.get("off"),
                        "requested": ev.extras.get("requested"),
                        "returned": ev.extras.get("returned"),
                    }
                )
            elif k in ("OPEN", "CLOSE"):
                obs.append({"op": k, "path": ev.path})

        cmp = compare_to_oracle(exp, obs)
        res["per_worker"][w] = cmp
        total["expected"] += cmp["expected_ops"]
        total["observed"] += cmp["observed_ops"]
        total["kind"] += cmp["op_kind_mismatch"]
        total["path"] += cmp["path_mismatch"]
        total["off"] += cmp["offset_mismatch"]
        total["requested"] += cmp["requested_mismatch"]
        total["returned"] += cmp["returned_mismatch"]

    res["totals"] = total
    res["syscall_substitutions"] = {
        "reads_issued_as": sorted(n for n in read_names if n),
        "stats_issued_as": sorted(n for n in stat_names if n),
    }
    return res


# ---------------------------------------------------------------------------
# E: outstanding-I/O depth


def depth_profile(events):
    """Reconstruct the in-flight read waveform from [entry, entry+dur] spans."""
    spans = [
        (ev.ts_ns, ev.ts_ns + ev.dur_ns)
        for ev in events
        if kind_of(ev) == "READ" and ev.dur_ns >= 0
    ]
    if not spans:
        return None
    edges = []
    for a, b in spans:
        edges.append((a, 1))
        edges.append((b, -1))
    edges.sort()
    depth = 0
    max_depth = 0
    weighted = 0.0
    busy = 0.0
    prev_t = edges[0][0]
    hist = {}
    for t, d in edges:
        if t > prev_t and depth > 0:
            dt = t - prev_t
            weighted += depth * dt
            busy += dt
            hist[depth] = hist.get(depth, 0) + dt
        prev_t = t
        depth += d
        max_depth = max(max_depth, depth)
    return {
        "read_spans": len(spans),
        "max_depth": max_depth,
        "mean_depth_while_busy": (weighted / busy) if busy else 0.0,
        "busy_ns": int(busy),
        "span_ns": edges[-1][0] - edges[0][0],
        "depth_time_share": {
            str(k): round(v / busy, 4) for k, v in sorted(hist.items())
        }
        if busy
        else {},
    }


def section_e(live_events, replay_run_events):
    return {
        "live": depth_profile(live_events),
        "replay": depth_profile(replay_run_events),
    }


# ---------------------------------------------------------------------------
# G: block-device counters (§10.1 "block-device counters")


def section_g(live_path, replay_path, warm_path):
    """Compare the bytes the block layer actually served on each side.

    An exact agreement here is stronger evidence than throughput parity: it shows
    the replay caused the same physical read volume, not merely that it finished
    in a similar time. The expected value is the dataset rounded up per file to
    the page size, since the kernel reads whole pages.
    """
    exp = spec.SHARDS * roundup(spec.SHARD_BYTES, PAGE)
    out = {"expected_device_bytes": exp}
    for label, path in (("live_cold", live_path), ("replay_cold", replay_path), ("replay_warm", warm_path)):
        if not path:
            continue
        d = json.loads(Path(path).read_text())
        out[label] = {
            "device_read_bytes": d["device_read_bytes"],
            "wall_ns": d["wall_ns"],
            "matches_expected": d["device_read_bytes"] == exp,
        }
    if "live_cold" in out and "replay_cold" in out:
        out["live_vs_replay_residual_bytes"] = (
            out["replay_cold"]["device_read_bytes"] - out["live_cold"]["device_read_bytes"]
        )
    return out


# ---------------------------------------------------------------------------


def verdict(report):
    """Per-dimension pass/fail against §10.2, with unmeasured dimensions named.

    A dimension is only "match" when a comparison actually ran and its residual
    was zero. Anything not measured is reported as not_measured, never as a
    pass; §10.2's goal list is not satisfied by silence.
    """
    out = {}

    a = report["A_oracle_self_consistency"]
    out["oracle_self_consistency"] = (
        "match"
        if all(
            v["journal_matches_analytic"] and v["syscr_matches"] and v["rchar_matches"]
            for v in a["per_worker"].values()
        )
        and not a["mismatches"]
        else "MISMATCH"
    )

    def zero_residual(section, dims):
        pw = section.get("per_worker", {})
        if not pw:
            return "not_measured"
        for v in pw.values():
            if v["expected_ops"] != v["observed_ops"]:
                return "MISMATCH"
            if v["order_agreement"] != "exact":
                return "MISMATCH"
            for d in dims:
                if v.get(d):
                    return "MISMATCH"
        return "match"

    dims = [
        "op_kind_mismatch",
        "path_mismatch",
        "offset_mismatch",
        "requested_mismatch",
        "returned_mismatch",
    ]
    out["capture_vs_oracle"] = zero_residual(report["B_capture_vs_oracle"], dims)
    out["trace_vs_oracle"] = zero_residual(
        report["C_trace_vs_oracle"], [d for d in dims if d != "requested_mismatch"]
    )
    out["replay_vs_trace"] = zero_residual(report.get("D_replay_vs_trace", {}), dims)

    ls = report["C_trace_vs_oracle"]["len_semantics"]
    if ls["discriminating_reads"] == 0:
        out["source_requested_length_preserved"] = "not_measured"
    elif ls["len_equals_source_requested"] == ls["trace_reads_compared"]:
        out["source_requested_length_preserved"] = "match"
    else:
        out["source_requested_length_preserved"] = "MISMATCH"

    e = report["E_outstanding_io_depth"]
    if e.get("live") and e.get("replay"):
        out["peak_outstanding_io"] = (
            "match" if e["live"]["max_depth"] == e["replay"]["max_depth"] else "MISMATCH"
        )
        out["mean_outstanding_io_residual"] = round(
            e["replay"]["mean_depth_while_busy"] - e["live"]["mean_depth_while_busy"], 3
        )
    else:
        out["peak_outstanding_io"] = "not_measured"

    d = report.get("D_replay_vs_trace", {})
    subs = d.get("syscall_substitutions", {})
    out["syscall_form_identical"] = (
        "MISMATCH: source read/fstat replayed as "
        + "/".join(subs.get("reads_issued_as", []) + subs.get("stats_issued_as", []))
        if subs.get("reads_issued_as") or subs.get("stats_issued_as")
        else "not_measured"
    )

    g = report.get("G_block_device_counters")
    if g and "live_vs_replay_residual_bytes" in g:
        out["block_device_bytes"] = (
            "match" if g["live_vs_replay_residual_bytes"] == 0 else "MISMATCH"
        )
        out["block_device_residual_bytes"] = g["live_vs_replay_residual_bytes"]
    else:
        out["block_device_bytes"] = "not_measured"

    # Dimensions §10.1 lists that this reconciliation does not attempt.
    out["not_attempted"] = [
        "inter-arrival distribution and issue-time error (the source clock is "
        "ptrace-distorted and replay ran in asap mode, so no arrival-time claim "
        "is defensible for this fixture)",
        "think/compute interval reproduction (replay performs no application "
        "compute, so the fixture's closed-loop pacing is not reproduced)",
        "repeated-trial stability, effect size, and confidence interval "
        "(single-trial reconciliation; roadmap Phase 3)",
    ]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--journals", required=True)
    ap.add_argument("--live-strace", required=True)
    ap.add_argument("--trace", required=True)
    ap.add_argument("--replay-strace")
    ap.add_argument("--replay-results")
    ap.add_argument("--replay-root")
    ap.add_argument("--baseline-journals")
    ap.add_argument("--devbytes-live")
    ap.add_argument("--devbytes-replay")
    ap.add_argument("--devbytes-replay-warm")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    dataset_dir = os.path.abspath(a.dataset)
    journals = load_journals(Path(a.journals))
    header, ops = load_trace(Path(a.trace))

    live_calls = stracelog.parse(a.live_strace)
    live_events = stracelog.sequential_offsets(
        stracelog.resolve(live_calls, dataset_dir)
    )

    report = {
        "fixture": spec.FIXTURE_ID,
        "dataset_dir": dataset_dir,
        "analytic_oracle_totals": oracle.totals(),
        "A_oracle_self_consistency": section_a(dataset_dir, journals),
        "B_capture_vs_oracle": section_b(dataset_dir, journals, live_events),
        "C_trace_vs_oracle": section_c(dataset_dir, journals, header, ops),
        "trace_header": {
            "kind": header.get("kind"),
            "capture_method": header.get("capture_method"),
            "summary": header.get("summary"),
            "num_targets": len(header.get("targets", [])),
        },
    }

    replay_run_events = []
    if a.replay_strace and a.replay_root:
        replay_root = os.path.abspath(a.replay_root)
        replay_calls = stracelog.parse(a.replay_strace)
        replay_events = stracelog.sequential_offsets(
            stracelog.resolve(replay_calls, replay_root)
        )
        report["D_replay_vs_trace"] = section_d(
            header, ops, journals, replay_events, replay_root, dataset_dir
        )
        replay_run_events, _, _ = split_replay_phases(replay_events)

    report["E_outstanding_io_depth"] = section_e(live_events, replay_run_events)

    timing = {
        "live_phase_ns_per_worker": {
            str(w): j["phase_ns"] for w, j in sorted(journals.items())
        },
        "note": "live phase time was measured under strace and is inflated by "
        "capture overhead; it is NOT comparable to replay duration",
    }
    if a.baseline_journals:
        base = load_journals(Path(a.baseline_journals))
        timing["baseline_phase_ns_per_worker"] = {
            str(w): j["phase_ns"] for w, j in sorted(base.items())
        }
        if base and journals:
            b = sum(j["phase_ns"] for j in base.values()) / len(base)
            t = sum(j["phase_ns"] for j in journals.values()) / len(journals)
            timing["capture_overhead_x"] = round(t / b, 2) if b else None
    if a.replay_results:
        rr = json.loads(Path(a.replay_results).read_text())
        timing["replay"] = {
            "duration_ns": rr.get("duration_ns"),
            "ops_completed": (rr.get("totals") or {}).get("ops_completed")
            or rr.get("ops_completed"),
        }
        report["replay_results"] = rr
    if a.devbytes_live or a.devbytes_replay:
        report["G_block_device_counters"] = section_g(
            a.devbytes_live, a.devbytes_replay, a.devbytes_replay_warm
        )
    report["F_timing"] = timing
    report["VERDICT"] = verdict(report)

    Path(a.out).write_text(json.dumps(report, indent=2, default=str))
    print(f"wrote {a.out}")
    print()
    for k, v in report["VERDICT"].items():
        if k == "not_attempted":
            print("  not attempted:")
            for item in v:
                print(f"    - {item}")
        else:
            print(f"  {k:38s} {v}")


if __name__ == "__main__":
    main()
