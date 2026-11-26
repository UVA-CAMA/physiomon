# Results

Apple M3 Ultra (28 cores, 256 GB), internal NVMe, macOS. DuckDB 2.0.0, numpy 2.5.4,
pyarrow 26.0.1, Python 3.12. 2026-11-11.

Batch: 193 beds × 10 messages (1 simulated minute). Each message has 10 numerics and
9 waveform signals of 1,440 int16 samples, so a batch is 19,300 numerics rows plus
17,370 BLOBs, about 50 MB of samples. Each scenario ran for 30 s, once.

## Throughput vs readers and checkpoint threshold

| Checkpoint at | Readers | Speedup (× real time) | vs 16MB, 0 readers | vs same threshold, 0 readers | Batch p95 | Newest row age, p95 |
| --- | --- | --- | --- | --- | --- | --- |
| 16MB (default) | 0 | 75x | 100% | 100% | 4.67 s | – |
| 16MB | 1 | 65x | 88% | 88% | 5.36 s | 6.0 s |
| 16MB | 4 | 127x | 169% | 169% | 1.23 s | 1.5 s |
| 256MB | 0 | 168x | 225% | 100% | 2.02 s | – |
| 256MB | 1 | 152x | 204% | 90% | 2.27 s | 2.2 s |
| 256MB | 4 | 139x | 186% | 83% | 2.46 s | 2.3 s |
| 1GB | 0 | 408x | 546% | 100% | 1.05 s | – |
| 1GB | 1 | 374x | 501% | 92% | 1.23 s | 2.6 s |
| 1GB | 4 | 328x | 439% | 80% | 1.35 s | 2.9 s |

Query latency, ms (p50 / p95), at 1GB:

| Readers | count | bed_numerics | all_numerics | bed_waves | freshness |
| --- | --- | --- | --- | --- | --- |
| 1 | 2 / 2 | 3 / 4 | 6 / 8 | 18 / 68 | 2 / 3 |
| 4 | 3 / 14 | 4 / 20 | 7 / 27 | 28 / 161 | 3 / 13 |

## Read overhead: Quack vs reading the day files directly

`uv run bench-read`: two day files, 20 simulated minutes each (2.9 GB), no ingest
running. Each query ran 10 times after a warm-up; median shown. Two runs agreed within
about 10%; the first is shown. Results fetched as Arrow.

| Query | Result | Direct (READ_ONLY attach) | `quack_query` | Client `ATTACH` |
| --- | --- | --- | --- | --- |
| `count` | 1 row | 0.5 ms | 3.7 ms | 3.1 ms |
| `bed_numerics` | 10 rows | 1.2 ms | 5.7 ms | 4.7 ms |
| `all_numerics` | 1,930 rows | 2.8 ms | 9.9 ms | 9.1 ms |
| `numerics_rows` (every row) | 40 MB | 3.3 ms | 57 ms (709 MB/s) | 39 ms (1.0 GB/s) |
| `bed_waves` (one bed's BLOBs) | 10 MB | 11 ms | 51 ms (205 MB/s) | 53 ms (198 MB/s) |
| `unit_waves` (16 beds' BLOBs) | 168 MB | 18 ms (9.5 GB/s) | 279 ms (600 MB/s) | 284 ms (590 MB/s) |

- **Small results:** Quack adds a fixed 2–7 ms per query. Not noticeable to a person.
- **Large results:** Quack moves about 0.6–1 GB/s; direct reads from the OS file
  cache run at about 10 GB/s, so bulk pulls are about 15x slower over Quack.
  Pulling an hour of one unit's waveforms (≈500 MB here) takes about 1 s instead
  of 0.05 s.
- Direct reads are only possible for a file no process has open for writing. A day
  file the writer still has attached can only be read through Quack.

## Findings

1. **Checkpointing dominates write cost, not readers.** At DuckDB's default 16 MB
   threshold, almost every 50 MB batch triggers a checkpoint. A commit without one
   takes ~0.05 s; with one it takes 0.7–7 s, growing as the file grows. Raising the
   threshold to 1 GB made the writer 5.4x faster. In a separate run with automatic
   checkpoints off, 70 GB of WAL checkpointed in 81 s at the end (~0.9 GB/s), so
   larger, rarer checkpoints are far cheaper per byte.
2. **At 256MB–1GB, readers cost 8–20%.** One reader cost 8–10%; four continuously
   looping readers cost 17–20%. That's at or above an 80%-of-baseline target, with the
   readers running far harder than people would.
3. **At 16MB, readers appeared to *speed up* the writer.** Four readers: 169%. It
   reproduced when the scenario order was reversed, and plain CPU load didn't cause
   it, so it's inside DuckDB. It's **unexplained**. The WAL never exceeded 52 MB and
   the final checkpoint was instant, so readers weren't simply postponing checkpoints.
   It doesn't affect the recommendation, which is not to run at 16MB.
4. **Freshness:** readers saw rows a few seconds old at p95. Rows become visible on
   commit, so freshness is the batch interval plus commit time.
5. **Readers never saw a partial batch.** In a probe with 10,000-row transactions,
   counts were always exact multiples of 10,000.
6. **ATTACH works when the views live in the server's default catalog.** A client
   `ATTACH 'quack:…'` sees the server's default catalog, not its other attached
   databases directly. In an earlier probe, tables only in the attached day files
   weren't visible over ATTACH. `quack_query` runs on the server and sees everything.
7. **Crash safety:** 3 of 3 SIGKILLs at the 1 GB threshold reopened with every
   committed row and nothing partial, including a kill after the day rollover. The
   WAL size at each kill wasn't recorded; it was somewhere up to 1 GB. **WAL replay time wasn't measured**;
   at 1 GB it adds to restart time.
8. **`quack_serve` doesn't block.** It returns immediately and serves from background
   threads, so the ingest process can serve its own session.

## Caveats

- One run per scenario, 30 s each; no repeats or confidence intervals. Treat
  differences under ~10% as noise.
- Synthetic, repeated data (a sine wave plus noise). Real compression and checkpoint
  cost will differ.
- Fast local NVMe. A slower server disk shifts everything toward checkpoint cost,
  which makes finding 1 matter more.
- The readers loop without pause, so they are a worst case.
- Insert cost only: no HL7 parsing, no NATS.
- Read overhead: localhost only, warm file cache, files fully checkpointed. Over a
  network, Quack transfer speed would be bounded by the link.
