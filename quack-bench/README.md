# quack-bench

Can a DuckDB ingest serve live SQL over [Quack](https://duckdb.org/docs/2.0/quack/overview)
without slowing down? DuckDB allows one writer per file and blocks other processes
from opening it. Quack lets the writing process serve its own session to clients.

See [RESULTS.md](./RESULTS.md) for a run and analysis.

## What it does

A **writer** process mirrors a bedside-monitor ingest service:
- one in-memory DuckDB session that `ATTACH`es **one file per day**;
- one transaction per batch: numerics rows plus waveform rows (packed int16 `BLOB`);
- `quack_serve` on its own session, with cross-day views (`numerics_all`, `waves_all`)
  in the default catalog;
- a **day rollover** halfway through: it attaches the next day's file and keeps the
  previous one attached.

**Reader** processes loop a fixed query set through `quack_query` while the writer runs:

| Query | What it reads |
| --- | --- |
| `count` | row count over all days |
| `bed_numerics` | one bed's numerics, grouped |
| `all_numerics` | every bed's numerics, grouped |
| `bed_waves` | one bed's waveform BLOBs |
| `freshness` | age of the newest visible row |

Each scenario (checkpoint threshold × reader count) starts from empty files. The
writer ingests as fast as it can, and the speedup is simulated seconds ingested per
wall-clock second, the same "× real time" a catch-up consumer would report. The
writer finishes with an explicit `CHECKPOINT`, which is counted, so work postponed by
a high threshold isn't hidden.

A separate command, `bench-read`, measures what Quack costs a reader: it builds day
files once, then times the same queries read directly (`ATTACH … (READ_ONLY)`), via
`quack_query`, and via a client `ATTACH` of the server.

Two further checks:
- **ATTACH:** can a client `ATTACH 'quack:…'` and see the cross-day views?
- **Crash test:** kill the writer (SIGKILL / TerminateProcess) mid-ingest, reopen the
  day files, and compare row counts with what the writer logged as committed.

## Usage

Requires [uv](https://docs.astral.sh/uv/). Works on macOS, Linux and Windows; no admin
rights needed, since the server binds to localhost.

```bash
uv run bench                       # full sweep: thresholds 16MB 256MB 1GB × readers 0 1 4, 30 s each
uv run bench --seconds 10 --readers 0 4 --checkpoint-thresholds 1GB --crash-trials 1
uv run bench-read                  # read overhead: direct vs Quack, ~3 GB of day files
uv run pytest                      # smoke tests
```

A run writes tens of GB at high thresholds; each scenario's files are deleted when it
finishes. `--work-dir` picks the disk.

### CLI options

```
--readers INT [...]                 reader process counts to sweep (default 0 1 4)
--checkpoint-thresholds SIZE [...]  WAL size that triggers a checkpoint (default 16MB 256MB 1GB)
--seconds FLOAT                     duration of each scenario (default 30)
--beds INT                          beds per batch (default 193)
--msgs-per-bed INT                  messages per bed per batch, 6 s each (default 10 = 1 min)
--crash-trials INT                  writer kills (default 3, at the largest threshold)
--crash-after FLOAT                 seconds before the first kill (default 3)
--uri URI                           Quack listen URI (default quack:localhost:9494)
--work-dir PATH                     where day files are created (default ./bench_data)
```

## Project layout

```
src/qbench/
  datagen.py   vectorized synthetic batch: numerics + packed-int16 BLOB waveforms
  writer.py    day-file ingest + quack_serve + rollover (run as a subprocess)
  reader.py    query loop over quack_query; ATTACH probe
  cli.py       scenario sweep, crash test, report (`uv run bench`)
  readcost.py  query cost direct vs over Quack (`uv run bench-read`)
```
