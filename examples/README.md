# Examples

## `real-trace-demo.sh`

End-to-end demo of IOFlux's core value: **capture a real process's storage
access pattern, then replay that exact pattern and report how faithfully the
backend reproduced it.**

The script:

1. Creates a small sharded dataset (16 × 16 MiB).
2. Runs a multi-worker Python reader and records its file I/O with `strace -y`.
3. Filters the capture down to the dataset (see the header comment for why the
   naive recipe fails) and `import`s it into a `.ioflux` trace.
4. Replays the same trace against the local filesystem two ways:
   - `timeline` / `cold` — honor the captured timing against an evicted cache.
   - `asap` / `warm` — closed-loop throughput ceiling against a primed cache.
5. Optionally (`S3=1`) replays against the bundled MinIO fixture.

```bash
examples/real-trace-demo.sh            # local legs only
S3=1 examples/real-trace-demo.sh       # also replay against MinIO
```

For the S3 leg, start the fixture first:

```bash
docker compose -f test/integration/docker-compose.yml up -d
```

What to look for in the reports: `trace_kind: imported` (a real, non-synthetic
trace), `coverage N/N` (every captured op replayed), `concurrency max-per-stream
1` (replay added no parallelism), and the `low-fidelity` flag when the backend
cannot reproduce the captured timing — IOFlux reports that honestly rather than
emitting flattering numbers.
