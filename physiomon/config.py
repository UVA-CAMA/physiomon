import os

NATS_URL          = os.getenv("PHYSIOMON_NATS_URL",          "nats://127.0.0.1:4222")
MLLP_HOST         = os.getenv("PHYSIOMON_MLLP_HOST",         "127.0.0.1")
MLLP_PORT         = int(os.getenv("PHYSIOMON_MLLP_PORT",     "2575"))
STREAM            = os.getenv("PHYSIOMON_STREAM",            "HL7")
SUBJECT           = os.getenv("PHYSIOMON_SUBJECT",           "hl7.inbound")
CONSUMER_DURABLE  = os.getenv("PHYSIOMON_CONSUMER_DURABLE",  "physiomon-service")
STAGING_DB        = os.getenv("PHYSIOMON_STAGING_DB",        "out/staging/staging.duckdb")
NATS_BATCH_SIZE   = int(os.getenv("PHYSIOMON_NATS_BATCH_SIZE", "50"))

# Optional SQL Server (or compatible) connection string for the file catalog.
# When unset, catalog writes are skipped entirely — the default for demo and
# open-source use. Set in production to enable unified file index across HL7
# and HSDI data. Example:
#   PHYSIOMON_CATALOG_CONNECTION=mssql+pyodbc://user:pass@host/physiodata?driver=ODBC+Driver+18+for+SQL+Server
CATALOG_CONNECTION: str | None = os.getenv("PHYSIOMON_CATALOG_CONNECTION")
