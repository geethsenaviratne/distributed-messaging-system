@echo off
echo =====================================================
echo Starting Distributed Messaging System with Raft Consensus
echo =====================================================
echo.
echo This will start:
echo - 3 Server nodes with Raft consensus (ports 9500, 9501, 9502)
echo - Leader will be elected automatically
echo.

rem Set Java options
set JAVA_OPTS=-Xmx1G -Dlogback.configurationFile=src/main/resources/logback.xml -Draft.enabled=true

rem Compile if needed
if not exist "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" (
  echo Building project...
  call mvn clean package -DskipTests
)

rem Start servers with Raft consensus enabled
start "Server 1 (Raft)" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9500 -id server1 -raft true -peerIds server2,server3 -peerAddresses localhost:9501,localhost:9502"
timeout /t 2
start "Server 2 (Raft)" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9501 -id server2 -raft true -peerIds server1,server3 -peerAddresses localhost:9500,localhost:9502"
timeout /t 2
start "Server 3 (Raft)" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9502 -id server3 -raft true -peerIds server1,server2 -peerAddresses localhost:9500,localhost:9501"
timeout /t 5

rem Wait for leader election to complete
echo.
echo Waiting for Raft leader election...
timeout /t 5

rem Start client
echo.
echo Starting client...
start "Client" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain -serverHost localhost -serverPort 9500 -raft true"

echo.
echo System started with Raft consensus.
echo Servers: localhost:9500, localhost:9501, localhost:9502
echo To stop the system, run stop-system.bat 