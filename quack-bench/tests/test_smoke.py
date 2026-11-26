from qbench.cli import main
from qbench.readcost import main as read_main


def test_smoke(tmp_path, capsys):
    main(["--readers", "0", "1", "--seconds", "3", "--beds", "5", "--checkpoint-thresholds", "16MB",
          "--crash-trials", "1", "--crash-after", "1", "--uri", "quack:localhost:9496",
          "--work-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "ATTACH from a client: ok" in out
    assert "-> OK" in out
    assert "query errors" not in out


def test_read_smoke(tmp_path, capsys):
    read_main(["--batches", "1", "--beds", "5", "--repeats", "1", "--uri", "quack:localhost:9497",
               "--work-dir", str(tmp_path)])
    assert "unit_waves" in capsys.readouterr().out
