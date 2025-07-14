@echo off
echo Stopping all Java processes for the Distributed Messaging System...

REM Kill all Java processes running with distributed-messaging-system in the command line
for /f "tokens=2" %%a in ('tasklist /fi "imagename eq java.exe" /v ^| findstr "distributed-messaging-system"') do (
    echo Stopping process with PID: %%a
    taskkill /f /pid %%a
)

echo All processes stopped.
pause 