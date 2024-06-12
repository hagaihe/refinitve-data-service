@echo off
setlocal

set LOGFILE=build_and_save_image.log

echo Logging to %LOGFILE%

REM Run the PowerShell command to handle logging and console output
powershell -Command ^
    "function LogAndRun { param([string]$cmd) Write-Output ($cmd + ' | Tee-Object -FilePath ' + $env:LOGFILE + ' -Append'); Invoke-Expression $cmd }; " ^
    "LogAndRun 'echo [%date% %time%] Starting Docker image build'; " ^
    "LogAndRun 'docker build -t refinitiv-data-service:latest .'; " ^
    "if (Test-Path 'refinitiv-data-service.tar') { LogAndRun 'del refinitiv-data-service.tar' }; " ^
    "LogAndRun 'docker save -o refinitiv-data-service.tar refinitiv-data-service:latest'; " ^
    "LogAndRun 'echo [%date% %time%] Docker image refinitiv-data-service:latest has been built and saved to refinitiv-data-service.tar'"

echo Logging complete. See %LOGFILE% for details.
pause
