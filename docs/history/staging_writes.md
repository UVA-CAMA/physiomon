# Staging Writes — Implementation Plan

Turns `physiomon` from a toy batch runner into a long-running service that
writes each message to a local DuckDB staging database immediately on receipt,
then exports to parquet at compaction time. Durability is row-level: each
message is ACKed to NATS only after it has been written to DuckDB.

---

## Module split

The generator/demo code and the production consumer code are separated:

| File | Role |
|------|------|
| `service.py` | Long-running NATS subscriber, DuckDB writes, no generator. Production artifact. |
| `cli.py` | Demo/dev entry point. Drives mock generation, imports consumer logic from `service.py`. |
| `demo.py` (root) | Unchanged — orchestrates NATS + mllp2nats + cli for end-to-end demo. |

Tests drive `service.py` directly or via subprocess. No generator code inside
the service.

---

## Write path per message

```
receive NATS message
    → parse_vectorized([msg.data])          # Polars DataFrame
    → split by value_type                   # numerics / waveforms / alarms
    → decode waveform values                # list[i16] in place
    → con.append(table, df.to_arrow())      # zero-copy via Arrow, per table
    → await msg.ack()
```

One message → one parse → one DuckDB append per non-empty table → one ACK.
No in-memory buffer. No flush interval. No ACK wait tuning.

If the DuckDB append raises, the ACK is skipped. NATS redelivers on restart.
The existing `msg_ctrl_id` dedup handles duplicate delivery safely.

---

## DuckDB staging database

A single local `.duckdb` file holds three tables mirroring the target parquet
schemas. DuckDB stores `list[i16]` (waveform `values`) natively — no encoding
workaround needed.

```
{out_dir}/staging/staging.duckdb
```

**Keep this file local.** DuckDB uses memory-mapped files; placing it on a
network drive causes friction on Windows.

Tables created on first connect if absent:

```sql
CREATE TABLE IF NOT EXISTS staging_numerics  AS SELECT * FROM df WHERE 1=0;
CREATE TABLE IF NOT EXISTS staging_waveforms AS SELECT * FROM df WHERE 1=0;
CREATE TABLE IF NOT EXISTS staging_alarms    AS SELECT * FROM df WHERE 1=0;
```

(Schema is inferred from the first empty Polars DataFrame passed via Arrow.)

---

## Durability

**Durable NATS consumer.** An ephemeral consumer is forgotten on disconnect;
a durable consumer retains its position so the service resumes from the last
unACKed message on restart.

Consumer name lives in config (`PHYSIOMON_CONSUMER_DURABLE`, default
`"physiomon-service"`). Demo/test modes pass `durable=None` (ephemeral,
immediate ACK, unchanged behaviour).

```python
ConsumerConfig(
    durable=config.CONSUMER_DURABLE,   # None for demo/test
    deliver_policy=DeliverPolicy.ALL,
)
```

No `ack_wait` tuning required — ACKs are immediate after each DuckDB write.

---

## Shutdown

On SIGINT or SIGTERM:
1. Stop accepting new messages.
2. Close DuckDB connection (flushes WAL).
3. Drain NATS connection and exit.

No flush step needed — every received message is already in DuckDB.

---

## Compaction (stub — deferred)

At midnight, export the prior day's rows to the archive parquet hierarchy and
delete them from the staging database.

```sql
-- pseudocode; exact date filter depends on confirmed obs_datetime format
COPY (
    SELECT * FROM staging_numerics
    WHERE obs_datetime LIKE '20260607%'
)
TO 'archive/vitals/2026/MICU/bed-3/20260607.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

DELETE FROM staging_numerics WHERE obs_datetime LIKE '20260607%';
```

**Partition path** (`{unit}/{bed}`) requires parsing `bed_location` (PV1-3)
into its components. The exact format of that field is not yet confirmed from
real data — this is the primary open question blocking the compaction
implementation. Deferred until staging data is available for inspection.

Archive parquet files are written with atomic rename (`path.tmp` → `path`),
same as any parquet write.

---

## Out of scope (deferred)

**File catalog / patient index** — design TBD pending review of real data
volume and query patterns. See `decisions.md` and prior discussion.

---

## Implementation steps

1. Add `duckdb` to `pyproject.toml` dependencies.

2. Add to `config.py`:
   ```python
   CONSUMER_DURABLE = os.getenv("PHYSIOMON_CONSUMER_DURABLE", "physiomon-service")
   STAGING_DB       = os.getenv("PHYSIOMON_STAGING_DB", "out/staging/staging.duckdb")
   ```

3. Create `service.py`:
   - `open_staging_db(path) -> duckdb.DuckDBPyConnection` — connect, create
     tables from empty typed DataFrames if absent.
   - `ingest(con, raw: bytes) -> None` — parse, split, decode waveforms,
     append each non-empty frame via Arrow.
   - `run(args) -> None` — async NATS subscriber loop: receive → `ingest` →
     ACK. Signal handling and clean shutdown.

4. Refactor `cli.py` — remove consumer loop logic; import `service.run` for
   the production path. Keep all generator/demo code in `cli.py`.

5. Update `demo.py` and tests as needed to drive `service.py` directly or
   via subprocess.

6. Smoke test: run `service.py` against live NATS + `mllp2nats`, send case
   files, inspect `staging.duckdb` with DuckDB CLI, verify row counts and
   types, verify restart resumes from last ACKed position.
