"""HL7 v2 → Polars DataFrames, deduplication."""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass

import polars as pl

__all__ = ["ParsedMessage", "parse_vectorized", "dedup", "dedup_rolling"]

log = logging.getLogger(__name__)


@dataclass
class ParsedMessage:
    """One message parsed into per-segment DataFrames.

    msh — one row (always present)
    pid — zero or one rows (present only when ADT match exists)
    pv1 — one row
    obr — one row per OBR group
    obx — one row per OBX; split by value_type in service.ingest
    """
    msh: pl.DataFrame
    pid: pl.DataFrame
    pv1: pl.DataFrame
    obr: pl.DataFrame
    obx: pl.DataFrame


def _empty_parsed() -> ParsedMessage:
    return ParsedMessage(
        msh=pl.DataFrame(schema={
            "msg_ctrl_id": pl.String, "sending_app": pl.String,
            "sending_facility": pl.String, "receiving_app": pl.String,
            "receiving_facility": pl.String, "msg_timestamp": pl.String,
            "msg_type": pl.String, "processing_id": pl.String,
            "version_id": pl.String, "accept_ack_type": pl.String,
            "app_ack_type": pl.String, "msg_profile": pl.String,
        }),
        pid=pl.DataFrame(schema={
            "msg_ctrl_id": pl.String,
            "patient_id_list": pl.String, "patient_name": pl.String,
            "dob": pl.String, "admin_sex": pl.String, "account_number": pl.String,
        }),
        pv1=pl.DataFrame(schema={
            "msg_ctrl_id": pl.String,
            "patient_class": pl.String, "patient_location": pl.String,
            "visit_number": pl.String, "admit_datetime": pl.String,
            "visit_indicator": pl.String,
        }),
        obr=pl.DataFrame(schema={
            "msg_ctrl_id": pl.String,
            "obr_set_id": pl.String, "placer_order_num": pl.String,
            "filler_order_num": pl.String, "service_id": pl.String,
            "obs_start_time": pl.String, "obs_end_time": pl.String,
            "parent_number": pl.String,
        }),
        obx=pl.DataFrame(schema={
            "msg_ctrl_id": pl.String, "obr_set_id": pl.String,
            "obx_set_id": pl.String, "value_type": pl.String,
            "obs_identifier": pl.String, "obs_code": pl.String, "obs_name": pl.String,
            "sub_id": pl.String, "channel": pl.Int32,
            "value": pl.String, "units": pl.String, "ref_range": pl.String,
            "abn_flag": pl.String, "result_status": pl.String,
            "obs_datetime": pl.String, "equipment_id": pl.String, "obs_site": pl.String,
        }),
    )


def parse_vectorized(messages: list[bytes]) -> ParsedMessage:
    """Vectorized HL7 → per-segment DataFrames. No Python loops over rows.

    Segment delimiter normalised (\\r or \\n both accepted).
    msg_ctrl_id (MSH-10) and obr_set_id (OBR-1) are forward-filled as FK
    join keys so every derived table links back to its message and OBR group.

    Segments extracted per spec-message-schema.md (aggregator Y fields only):
      MSH: sending_app(3) sending_facility(4) receiving_app(5)
           receiving_facility(6) msg_timestamp(7) msg_type(9)
           processing_id(11) version_id(12) accept_ack_type(15)
           app_ack_type(16) msg_profile(21)
      PID: patient_id_list(3) patient_name(5) dob(7) admin_sex(8) account_number(18)
      PV1: patient_class(2) patient_location(3) visit_number(19)
           admit_datetime(44) visit_indicator(51)
      OBR: obr_set_id(1) placer_order_num(2) filler_order_num(3) service_id(4)
           obs_start_time(7) obs_end_time(8) parent_number(29)
      OBX: obx_set_id(1) value_type(2) obs_identifier(3) sub_id(4) value(5)
           units(6) ref_range(7) abn_flag(8) result_status(11) obs_datetime(14)
           equipment_id(18) obs_site(20)
           + derived: obs_code obs_name channel
    """
    if not messages:
        return _empty_parsed()

    g = pl.col("f")

    df = (
        pl.DataFrame({"raw": [m.decode() for m in messages]})
        .with_row_index("msg_id")
        .with_columns(
            pl.col("raw").str.replace_all("\n", "\r").str.split("\r").alias("segs")
        )
        .explode("segs")
        .filter(pl.col("segs").str.len_chars() >= 3)
        .with_columns(
            seg_type=pl.col("segs").str.slice(0, 3),
            f=pl.col("segs").str.split("|"),
        )
    )

    # Forward-fill msg_ctrl_id and obr_set_id as FK join keys
    df = df.with_columns(
        _msg_ctrl_id=pl.when(pl.col("seg_type") == "MSH").then(g.list.get(9, null_on_oob=True)),
        _obr_set_id =pl.when(pl.col("seg_type") == "OBR").then(g.list.get(1, null_on_oob=True)),
    ).with_columns(
        msg_ctrl_id=pl.col("_msg_ctrl_id").forward_fill().over("msg_id"),
        obr_set_id =pl.col("_obr_set_id").forward_fill().over("msg_id"),
    )

    msh = (
        df.filter(pl.col("seg_type") == "MSH")
        .select([
            pl.col("msg_ctrl_id"),
            g.list.get(2,  null_on_oob=True).alias("sending_app"),
            g.list.get(3,  null_on_oob=True).alias("sending_facility"),
            g.list.get(4,  null_on_oob=True).alias("receiving_app"),
            g.list.get(5,  null_on_oob=True).alias("receiving_facility"),
            g.list.get(6,  null_on_oob=True).alias("msg_timestamp"),
            g.list.get(8,  null_on_oob=True).alias("msg_type"),
            g.list.get(10, null_on_oob=True).alias("processing_id"),
            g.list.get(11, null_on_oob=True).alias("version_id"),
            g.list.get(14, null_on_oob=True).alias("accept_ack_type"),
            g.list.get(15, null_on_oob=True).alias("app_ack_type"),
            g.list.get(20, null_on_oob=True).alias("msg_profile"),
        ])
    )

    pid = (
        df.filter(pl.col("seg_type") == "PID")
        .select([
            pl.col("msg_ctrl_id"),
            g.list.get(3,  null_on_oob=True).alias("patient_id_list"),
            g.list.get(5,  null_on_oob=True).alias("patient_name"),
            g.list.get(7,  null_on_oob=True).alias("dob"),
            g.list.get(8,  null_on_oob=True).alias("admin_sex"),
            g.list.get(18, null_on_oob=True).alias("account_number"),
        ])
    )

    pv1 = (
        df.filter(pl.col("seg_type") == "PV1")
        .select([
            pl.col("msg_ctrl_id"),
            g.list.get(2,  null_on_oob=True).alias("patient_class"),
            g.list.get(3,  null_on_oob=True).alias("patient_location"),
            g.list.get(19, null_on_oob=True).alias("visit_number"),
            g.list.get(44, null_on_oob=True).alias("admit_datetime"),
            g.list.get(51, null_on_oob=True).alias("visit_indicator"),
        ])
    )

    obr = (
        df.filter(pl.col("seg_type") == "OBR")
        .select([
            pl.col("msg_ctrl_id"),
            g.list.get(1,  null_on_oob=True).alias("obr_set_id"),
            g.list.get(2,  null_on_oob=True).alias("placer_order_num"),
            g.list.get(3,  null_on_oob=True).alias("filler_order_num"),
            g.list.get(4,  null_on_oob=True).alias("service_id"),
            g.list.get(7,  null_on_oob=True).alias("obs_start_time"),
            g.list.get(8,  null_on_oob=True).alias("obs_end_time"),
            g.list.get(29, null_on_oob=True).alias("parent_number"),
        ])
    )

    obx = (
        df.filter(pl.col("seg_type") == "OBX")
        .with_columns(
            obx_set_id    = g.list.get(1,  null_on_oob=True),
            value_type    = g.list.get(2,  null_on_oob=True),
            obs_identifier= g.list.get(3,  null_on_oob=True),
            sub_id        = g.list.get(4,  null_on_oob=True),
            value         = g.list.get(5,  null_on_oob=True),
            units         = g.list.get(6,  null_on_oob=True),
            ref_range     = g.list.get(7,  null_on_oob=True),
            abn_flag      = g.list.get(8,  null_on_oob=True),
            result_status = g.list.get(11, null_on_oob=True),
            obs_datetime  = g.list.get(14, null_on_oob=True),
            equipment_id  = g.list.get(18, null_on_oob=True),
            obs_site      = g.list.get(20, null_on_oob=True),
        )
        .with_columns(_obs_parts=pl.col("obs_identifier").str.split("^"))
        .with_columns(
            obs_code=pl.col("_obs_parts").list.get(0, null_on_oob=True),
            obs_name=pl.col("_obs_parts").list.get(1, null_on_oob=True),
            channel =pl.col("sub_id").str.split(".").list.get(2, null_on_oob=True).cast(pl.Int32, strict=False),
        )
        .select([
            "msg_ctrl_id", "obr_set_id",
            "obx_set_id", "value_type", "obs_identifier", "obs_code", "obs_name",
            "sub_id", "channel", "value", "units", "ref_range", "abn_flag",
            "result_status", "obs_datetime", "equipment_id", "obs_site",
        ])
    )

    return ParsedMessage(msh=msh, pid=pid, pv1=pv1, obr=obr, obx=obx)


# dedup and dedup_rolling are not called by any current code path.
# Retained for use at compaction time to handle the narrow crash window
# between a DuckDB write and its NATS ACK, which can produce duplicate rows.

def dedup(df: pl.DataFrame) -> pl.DataFrame:
    """Remove duplicate messages within a batch using exact MSH-10 matching."""
    if df.is_empty():
        return df
    return (
        df
        .with_columns(pl.col("msg_ctrl_id").is_first_distinct().alias("_first"))
        .filter(pl.col("_first"))
        .drop("_first")
    )


class dedup_rolling:
    """Cross-batch exact deduplication on MSH-10 with a bounded rolling window.

    Window sizing: monitors × msg/min × retry_window_minutes.
    For 1000 monitors at ~10 msg/min with a 5-minute retry window: 50,000.
    """

    def __init__(self, window: int = 50_000):
        self._seen:   set[str]   = set()
        self._order:  deque[str] = deque()
        self._window: int        = window
        self._lock               = threading.Lock()

    def filter(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        with self._lock:
            ctrl_ids = df["msg_ctrl_id"].unique().to_list()
            new_ids  = [cid for cid in ctrl_ids if cid and cid not in self._seen]
            for cid in new_ids:
                self._seen.add(cid)
                self._order.append(cid)
            while len(self._order) > self._window:
                self._seen.discard(self._order.popleft())
            new_ids_set = set(new_ids)
        return df.filter(pl.col("msg_ctrl_id").is_in(list(new_ids_set)))

    def __len__(self) -> int:
        return len(self._seen)

    def __repr__(self) -> str:
        return f"dedup_rolling(seen={len(self._seen)}, window={self._window})"
