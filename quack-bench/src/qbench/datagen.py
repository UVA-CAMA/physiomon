"""Vectorized synthetic bedside-monitor batches: numerics rows and packed-int16 waveform BLOBs.

One batch is generated once and re-inserted with new msg_seq / timestamps, so the
timed loop measures inserts, not data generation.
"""

from dataclasses import dataclass

import numpy as np
import pyarrow as pa

MSG_INTERVAL_S = 6.0  # one waveform message per bed every ~6 s


@dataclass(frozen=True)
class Batch:
    numerics: pa.Table  # msg, bed, offset_us, obs_code, value
    waves: pa.Table  # msg, bed, offset_us, signal, samples (BLOB)
    n_msgs: int
    sim_seconds: float  # simulated time one batch covers
    wave_bytes: int


def make_batch(
    n_beds: int, msgs_per_bed: int, n_numerics: int, n_signals: int, samples: int, seed: int = 0
) -> Batch:
    rng = np.random.default_rng(seed)
    n_msgs = n_beds * msgs_per_bed
    msg = np.arange(n_msgs, dtype=np.int64)
    beds = np.array([f"UNIT{b // 16}^BED{b}" for b in range(n_beds)])  # one per bed, not per row
    bed = beds[msg % n_beds]
    offset_us = (msg // n_beds) * int(MSG_INTERVAL_S * 1e6)

    numerics = pa.table({
        "msg": np.repeat(msg, n_numerics),
        "bed": np.repeat(bed, n_numerics),
        "offset_us": np.repeat(offset_us, n_numerics),
        "obs_code": np.tile(np.arange(n_numerics, dtype=np.int32), n_msgs),
        "value": rng.normal(80, 10, n_msgs * n_numerics),
    })

    # ECG-like signal: sine + noise, so compression is neither trivial nor hopeless
    n_rows = n_msgs * n_signals
    t = np.arange(n_rows * samples)
    flat = (1000 * np.sin(t * 2 * np.pi / 240) + rng.normal(0, 30, t.size)).astype("<i2")
    offsets = np.arange(n_rows + 1, dtype=np.int64) * samples * flat.itemsize
    blobs = pa.LargeBinaryArray.from_buffers(
        pa.large_binary(), n_rows, [None, pa.py_buffer(offsets), pa.py_buffer(flat)]
    )
    waves = pa.table({
        "msg": np.repeat(msg, n_signals),
        "bed": np.repeat(bed, n_signals),
        "offset_us": np.repeat(offset_us, n_signals),
        "signal": np.tile(np.arange(n_signals, dtype=np.int32), n_msgs),
        "samples": blobs,
    })
    return Batch(numerics, waves, n_msgs, msgs_per_bed * MSG_INTERVAL_S, flat.nbytes)
