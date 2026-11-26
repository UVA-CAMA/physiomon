"""Report consumer progress: processed vs received rate (speedup factor) and catch-up ETA.

Read-only against NATS; safe to run while service.py is running.

    python progress.py              # sample processing rate over 60 s
    python progress.py --window 10
"""

import argparse
import asyncio
import datetime

import nats

import config


async def main(window: float) -> None:
    nc = await nats.connect(config.NATS_URL)
    js = nc.jetstream()

    # Received rate: average over the whole stream, from first/last arrival times
    s = await js.stream_info(config.STREAM)
    first = await js.get_msg(config.STREAM, s.state.first_seq)
    last = await js.get_msg(config.STREAM, s.state.last_seq)
    recv = s.state.messages / (last.time - first.time).total_seconds()

    c1 = await js.consumer_info(config.STREAM, config.CONSUMER_DURABLE)
    await asyncio.sleep(window)
    c2 = await js.consumer_info(config.STREAM, config.CONSUMER_DURABLE)
    proc = (c2.ack_floor.stream_seq - c1.ack_floor.stream_seq) / window

    idle = (datetime.datetime.now(datetime.UTC) - last.time).total_seconds() / 60
    eta = f"{c2.num_pending / (proc - recv) / 3600:.1f} h" if proc > recv else "never"
    print(f"received={recv:.1f} msg/s (avg since {first.time:%Y-%m-%d %H:%M}Z)  processed={proc:.1f} msg/s  "
          f"speedup={proc / recv:.1f}x  pending={c2.num_pending:,}  catch-up={eta}  last msg {idle:.0f} min ago")
    await nc.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--window", type=float, default=60.0, help="seconds to sample processing rate (default 60)")
    asyncio.run(main(ap.parse_args().window))
