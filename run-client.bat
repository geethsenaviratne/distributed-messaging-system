@echo off
echo =====================================================
echo Starting Client Node
echo =====================================================
echo.

rem Set Java options
set JAVA_OPTS=-Xmx512M -Dlogback.configurationFile=src/main/resources/logback.xml

rem Set default server values
set SERVER_HOST=localhost
set SERVER_PORT=9500

rem Parse command line arguments
if not "%1"=="" set SERVER_HOST=%1
if not "%2"=="" set SERVER_PORT=%2

rem Compile if needed
if not exist "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" (
  echo Building project...
  call mvn clean package -DskipTests
)

echo Starting client connected to %SERVER_HOST%:%SERVER_PORT%
java %JAVA_OPTS% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain client1 %SERVER_HOST% %SERVER_PORT%

echo.
echo Client stopped.
pause 