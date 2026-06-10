"""Demo launcher: starts NATS, mllp2nats, and service.py if not running,
then runs sender.py with the given arguments.

Any argument is forwarded directly to sender.py.  Processes started by this
script are stopped on exit; already-running services are left alone.

Usage:
    python demo.py                           # fixed, 1 patient, 20 msgs
    python demo.py --mode stream --patients 2
    python demo.py --help                    # shows sender.py help
"""

import atexit
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT        = Path(__file__).resolve().parent
PHYSIO      = ROOT / "physiomon"
PHYSIO_SIM  = ROOT / "physiosim"
EXE         = ".exe" if sys.platform == "win32" else ""
MLLP_BIN   = ROOT / "mllp2nats" / f"mllp2nats{EXE}"

# These match the defaults in physiomon/config.py.
# If you override via PHYSIOMON_* env vars, start services manually instead.
NATS_HOST, NATS_PORT = "127.0.0.1", 4222
MLLP_HOST, MLLP_PORT = "127.0.0.1", 2575

_started: list[subprocess.Popen] = []


def _listening(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _start_nats() -> None:
    if _listening(NATS_HOST, NATS_PORT):
        print(f"nats-server already running on :{NATS_PORT}")
        return
    if not shutil.which("nats-server"):
        sys.exit(
            "nats-server not found in PATH.\n"
            "  macOS:   brew install nats-server\n"
            "  Windows: winget install NATS.io.nats-server"
        )
    p = subprocess.Popen(["nats-server", "-js"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _started.append(p)
    for _ in range(30):
        if _listening(NATS_HOST, NATS_PORT):
            print(f"started nats-server  pid={p.pid}")
            return
        time.sleep(0.1)
    sys.exit("nats-server did not start within 3s")


def _start_mllp2nats() -> None:
    if _listening(MLLP_HOST, MLLP_PORT):
        print(f"mllp2nats already running on :{MLLP_PORT}")
        return
    if not MLLP_BIN.exists():
        sys.exit(
            f"mllp2nats binary not found: {MLLP_BIN}\n"
            f"  Build it first:  cd mllp2nats && go build -o mllp2nats{EXE} ."
        )
    p = subprocess.Popen([str(MLLP_BIN)],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _started.append(p)
    for _ in range(30):
        if _listening(MLLP_HOST, MLLP_PORT):
            print(f"started mllp2nats    pid={p.pid}")
            return
        time.sleep(0.1)
    sys.exit("mllp2nats did not start within 3s")


def _cleanup() -> None:
    for p in _started:
        p.terminate()
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def _start_service() -> None:
    p = subprocess.Popen(
        ["uv", "run", "python", "service.py"],
        cwd=str(PHYSIO), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    _started.append(p)
    time.sleep(2)  # allow service to connect and subscribe
    print(f"started service.py      pid={p.pid}")


def main() -> None:
    atexit.register(_cleanup)
    _start_nats()
    _start_mllp2nats()
    _start_service()
    sender_args = sys.argv[1:] or ["--count", "20", "--patients", "1"]
    sys.exit(subprocess.run(["uv", "run", "python", "sender.py"] + sender_args, cwd=str(PHYSIO_SIM)).returncode)


if __name__ == "__main__":
    main()
