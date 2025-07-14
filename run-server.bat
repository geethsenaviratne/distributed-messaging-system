@echo off
rem Run a server node

if "%1"=="" (
    echo Usage: run-server.bat ^<serverID^> ^<port^> [peerServer1] [peerServer2] ...
    echo Example: run-server.bat server1 8000 localhost:8001 localhost:8002
    exit /b 1
)

set SERVER_ID=%1
set PORT=%2
set PEER_SERVERS=

rem Build the peer servers list
shift
shift
:loop
if "%1"=="" goto endloop
set PEER_SERVERS=%PEER_SERVERS% %1
shift
goto loop
:endloop

echo Starting server %SERVER_ID% on port %PORT% with peers: %PEER_SERVERS%
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain %SERVER_ID% %PORT% %PEER_SERVERS%