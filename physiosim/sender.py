"""Send HL7 messages to mllp2nats via MLLP.

Usage:
    python sender.py                                   # generate forever, all feeds, 1 patient, 1s interval
    python sender.py --files "examples/*.hl7"          # send matching files once
    python sender.py --count 20                        # stop after 20 msgs per patient per feed
    python sender.py --duration 30.0                   # run for 30 seconds
    python sender.py --patients 3 --interval 6.0
"""

import argparse
import glob
import os
import signal
import socket
import threading

import polars as pl

from generator import FEEDS, MLLP_END, MLLP_START, _FEED_GENERATORS, make_patients, run_feed

_DEFAULT_HOST = os.environ.get("PHYSIOMON_MLLP_HOST", "127.0.0.1")
_DEFAULT_PORT = int(os.environ.get("PHYSIOMON_MLLP_PORT", "2575"))


def load_files(pattern: str) -> list[bytes]:
    return [
        row.encode()
        for f in sorted(glob.glob(pattern))
        for row in pl.read_csv(f)["HL7STRIPPED"]
    ]


def mllp_send(messages: list[bytes], host: str, port: int) -> None:
    """Send HL7 messages via MLLP. No ACK handling — mllp2nats runs with ACKs disabled."""
    with socket.create_connection((host, port), timeout=30) as sock:
        for msg in messages:
            sock.sendall(MLLP_START + msg + MLLP_END)


def _parse_args():
    p = argparse.ArgumentParser(description="Send HL7 to mllp2nats via MLLP")
    p.add_argument("--files",    default=None,  help="glob of HL7 files to send once")
    p.add_argument("--count",    type=int,   default=None, help="messages per patient per feed (omit for infinite)")
    p.add_argument("--duration", type=float, default=None, help="seconds to run (omit for infinite)")
    p.add_argument("--patients", type=int,   default=1)
    p.add_argument("--interval", type=float, default=None, help="seconds between messages (default: 0 if finite, 1.0 if infinite)")
    p.add_argument("--host",     default=_DEFAULT_HOST)
    p.add_argument("--port",     type=int,   default=_DEFAULT_PORT)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.files:
        messages = load_files(args.files)
        print(f"sending {len(messages)} messages from {args.files}…")
        mllp_send(messages, args.host, args.port)
        print("done")
    else:
        finite   = args.count is not None or args.duration is not None
        interval = args.interval if args.interval is not None else (0.0 if finite else 1.0)
        patients = make_patients(args.patients)
        stop     = threading.Event()

        def _sig(_, __):
            print("\nstopping…")
            stop.set()

        signal.signal(signal.SIGINT,  _sig)
        signal.signal(signal.SIGTERM, _sig)

        threads = [
            threading.Thread(
                target=run_feed,
                args=(p, feed.name, feed.port, _FEED_GENERATORS[feed.name],
                      interval, args.host, args.count, stop),
                daemon=True,
            )
            for feed in FEEDS for p in patients
        ]
        for t in threads:
            t.start()

        desc = f"{args.count} msgs" if args.count else f"{args.duration}s" if args.duration else "∞"
        print(f"sending {desc} × {len(patients)} patients × {len(FEEDS)} feeds  interval={interval}s")

        if args.duration is not None:
            stop.wait(timeout=args.duration)
            stop.set()
        else:
            stop.wait()

        for t in threads:
            t.join(timeout=15)
        print("done")
