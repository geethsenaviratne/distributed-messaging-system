@echo off
echo =====================================================
echo Starting Distributed Messaging System
echo =====================================================
echo.
echo This will start:
echo - 2 Server nodes (ports 9000, 9001)
echo - Primary-backup replication mode
echo.

rem Set Java options
set JAVA_OPTS=-Xmx1G -Dlogback.configurationFile=src/main/resources/logback.xml

rem Compile if needed
if not exist "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" (
  echo Building project...
  call mvn clean package -DskipTests
)

rem Start servers in separate windows
start "Server 1" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain server1 9000 localhost:9001"
timeout /t 2
start "Server 2" cmd /c "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain server2 9001 localhost:9000"
timeout /t 2

rem Start clients after servers are initialized
echo.
echo Servers started. Starting clients...
start "Client 1" cmd /k "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain client1 localhost 9000 localhost:9001"
start "Client 2" cmd /k "java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain client2 localhost 9001 localhost:9000"


echo.
echo System started successfully.
echo - Server 1: localhost:9000 
echo - Server 2: localhost:9001
echo - Client 1 connected to: localhost:9000, backup: localhost:9001
echo - Client 2 connected to: localhost:9001, backup: localhost:9000
echo.
echo To stop the system, run stop-system.bat 