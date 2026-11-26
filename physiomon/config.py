import os

NATS_URL          = os.getenv("PHYSIOMON_NATS_URL",          "nats://127.0.0.1:4222")
MLLP_HOST         = os.getenv("PHYSIOMON_MLLP_HOST",         "127.0.0.1")
MLLP_PORT         = int(os.getenv("PHYSIOMON_MLLP_PORT",     "2575"))
STREAM            = os.getenv("PHYSIOMON_STREAM",            "HL7")
SUBJECT           = os.getenv("PHYSIOMON_SUBJECT",           "hl7.inbound")
CONSUMER_DURABLE  = os.getenv("PHYSIOMON_CONSUMER_DURABLE",  "physiomon-service")
STAGING_DIR       = os.getenv("PHYSIOMON_STAGING_DIR",       "out/staging")  # one YYYYMMDD.duckdb per UTC day
# Max messages per fetch/transaction. A fetch returns whatever is already waiting,
# so live batches stay small; the cap matters when catching up. Keep it under the
# consumer's MaxAckPending (NATS default 1,000) and a batch's processing time under
# AckWait (default 30 s).
NATS_BATCH_SIZE   = int(os.getenv("PHYSIOMON_NATS_BATCH_SIZE", "500"))
# WAL size that triggers a DuckDB checkpoint. DuckDB's 16MB default checkpointed on
# almost every batch in quack-bench; 1GB made ingest 5.4x faster.
CHECKPOINT_THRESHOLD = os.getenv("PHYSIOMON_CHECKPOINT_THRESHOLD", "1GB")
# Live SQL over the attached day files via DuckDB Quack. Off unless QUACK_LISTEN is
# set, e.g. quack:localhost:9494 (localhost only). Readers need the token (4+ chars).
QUACK_LISTEN: str | None = os.getenv("PHYSIOMON_QUACK_LISTEN")
QUACK_TOKEN:  str | None = os.getenv("PHYSIOMON_QUACK_TOKEN")
# Folder holding DuckDB extensions (quack, httpfs) as <dir>/v2.0.0/<platform>/*.duckdb_extension,
# for servers without internet. Unset = DuckDB's per-user default, ~/.duckdb/extensions.
DUCKDB_EXTENSION_DIR: str | None = os.getenv("PHYSIOMON_DUCKDB_EXTENSION_DIR")

# Optional SQL Server (or compatible) connection string for the file catalog.
# When unset, catalog writes are skipped entirely — the default for demo and
# open-source use. Set in production to enable unified file index across HL7
# and HSDI data. Example:
#   PHYSIOMON_CATALOG_CONNECTION=mssql+pyodbc://user:pass@host/physiodata?driver=ODBC+Driver+18+for+SQL+Server
CATALOG_CONNECTION: str | None = os.getenv("PHYSIOMON_CATALOG_CONNECTION")
