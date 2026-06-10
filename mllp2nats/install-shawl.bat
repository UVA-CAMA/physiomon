@echo off
REM Register mllp2nats as a Windows service using Shawl.
REM Edit ports, subjects, and NATS URL to match your environment.
REM Run as Administrator.

set BINDIR=C:\services\mllp2nats
set LOGDIR=C:\logs\mllp2nats
set NATS=nats://localhost:4222
set STREAM=HL7

echo Registering mllp2nats on port 2575...
shawl add --name mllp2nats ^
  --log-dir %LOGDIR% ^
  --log-rotate 5 ^
  --log-rotate-bytes 52428800 ^
  -- "%BINDIR%\mllp2nats.exe" ^
  --port 2575 --nats %NATS% --stream %STREAM% --subject hl7.inbound

REM Uncomment to run separate instances per feed on separate ports:
REM shawl add --name mllp2nats-dec --log-dir %LOGDIR%-dec --log-rotate 5 --log-rotate-bytes 52428800 -- "%BINDIR%\mllp2nats.exe" --port 2575 --nats %NATS% --stream %STREAM% --subject hl7.inbound.dec
REM shawl add --name mllp2nats-acm --log-dir %LOGDIR%-acm --log-rotate 5 --log-rotate-bytes 52428800 -- "%BINDIR%\mllp2nats.exe" --port 2576 --nats %NATS% --stream %STREAM% --subject hl7.inbound.acm
REM shawl add --name mllp2nats-wcm --log-dir %LOGDIR%-wcm --log-rotate 5 --log-rotate-bytes 52428800 -- "%BINDIR%\mllp2nats.exe" --port 2577 --nats %NATS% --stream %STREAM% --subject hl7.inbound.wcm

echo.
echo Done. Start with:
echo   sc start mllp2nats
