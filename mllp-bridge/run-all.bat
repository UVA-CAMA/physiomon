@echo off
REM Start mllp2nats with three feed listeners, in its own console window.
REM Assumes nats-server (with -js) is already running and the HL7 stream exists.
REM No admin rights or Windows service required.
REM
REM   DEC (numerics)  -> port 2575
REM   ACM (alarms)    -> port 2576
REM   WCM (waveforms) -> port 2577
REM
REM All three publish to hl7.inbound (the default) so service.py consumes them all.

cd /d "%~dp0"

start "mllp2nats" cmd /k mllp2nats.exe --listen DEC=:2575 --listen ACM=:2576 --listen WCM=:2577 --log-level debug
