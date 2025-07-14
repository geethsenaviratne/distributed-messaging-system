@echo off
echo =====================================================
echo Time Synchronization Demonstration
echo =====================================================
echo.
echo This script demonstrates the time synchronization mechanism by:
echo - Starting servers with intentionally skewed clocks
echo - Showing the synchronization protocol in action
echo - Demonstrating handling of out-of-order messages
echo.

rem Set Java options with time offset parameters
set JAVA_OPTS_BASE=-Xmx1G -Dlogback.configurationFile=src/main/resources/logback.xml

rem Compile if needed
if not exist "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" (
  echo Building project...
  call mvn clean package -DskipTests
)

rem Start time server (reference clock)
echo Starting Time Server (reference clock)...
start "Time Server" cmd /c "java %JAVA_OPTS_BASE% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9500 -id timeServer -timeServer true"
timeout /t 3

rem Start server with clock ahead by 5 seconds
echo Starting Server with clock ahead by 5 seconds...
start "Fast Clock Server" cmd /c "java %JAVA_OPTS_BASE% -DtimeOffset=5000 -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9501 -id fastServer -timeServer false -timeSyncServerId timeServer -peerHost localhost -peerPort 9500"
timeout /t 3

rem Start server with clock behind by 2 seconds
echo Starting Server with clock behind by 2 seconds...
start "Slow Clock Server" cmd /c "java %JAVA_OPTS_BASE% -DtimeOffset=-2000 -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain -port 9502 -id slowServer -timeServer false -timeSyncServerId timeServer -peerHost localhost -peerPort 9500"
timeout /t 3

rem Start client that will send timestamped messages
echo.
echo Starting client to demonstrate time-synchronized message ordering...
start "Time Sync Client" cmd /c "java %JAVA_OPTS_BASE% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain -serverHost localhost -serverPort 9501 -timestampedMessages true"

echo.
echo Time synchronization demonstration started.
echo - Reference time server: localhost:9500
echo - Server with fast clock (+5s): localhost:9501
echo - Server with slow clock (-2s): localhost:9502
echo.
echo Watch the logs to observe time synchronization in action.
echo Client messages will be properly ordered based on synchronized timestamps.
echo.
echo To stop the demonstration, run stop-system.bat 