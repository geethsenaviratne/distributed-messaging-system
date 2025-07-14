@echo off
SETLOCAL

REM Set Java options for memory and logging
SET JAVA_OPTS=-Xmx512m -Xms256m
SET LOG_LEVEL=-Dlogback.configurationFile=src/main/resources/logback.xml

REM Create logs directory if it doesn't exist
IF NOT EXIST logs mkdir logs

REM Check if the JAR exists, build if necessary
IF NOT EXIST target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar (
  echo Building the project...
  CALL mvn clean package -DskipTests
)

IF NOT EXIST target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar (
  echo Failed to build the project. Please check for errors.
  exit /b 1
)

echo.
echo Running Raft log replication correctness test...
echo Results will be written to logs/replication-test.log
echo.

REM Run the log replication test class
java %JAVA_OPTS% %LOG_LEVEL% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.ReplicationTest

REM Get the exit code
SET EXIT_CODE=%ERRORLEVEL%

echo.
IF %EXIT_CODE% EQU 0 (
  echo Test completed successfully. Check logs/replication-test.log for details.
) ELSE (
  echo Test failed with exit code %EXIT_CODE%. Check logs/replication-test.log for details.
)

exit /b %EXIT_CODE% 