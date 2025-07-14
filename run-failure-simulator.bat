@echo off
rem Run the failure simulator

if "%1"=="" (
    echo Usage: run-failure-simulator.bat ^<server1^> ^<server2^> ...
    echo Example: run-failure-simulator.bat localhost:8000 localhost:8001 localhost:8002
    exit /b 1
)

set SERVERS=

rem Build the servers list
:loop
if "%1"=="" goto endloop
set SERVERS=%SERVERS% %1
shift
goto loop
:endloop

echo Starting failure simulator for servers: %SERVERS%
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.TestFailureSimulator %SERVERS% 