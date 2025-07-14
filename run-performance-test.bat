@echo off
echo Starting consensus performance test...

set JAVA_OPTS=-Draft.enabled=true -Draft.debug=true
set LOGGING_CONFIG=-Dlogback.configurationFile=config\logback-performance.xml
set LOG_LEVEL=-Dlogging.level.com.ds.messaging.consensus=DEBUG

echo Creating logs directory if it doesn't exist...
if not exist logs\ mkdir logs\

REM Check if JAR file exists
if not exist target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar (
    echo Error: JAR file not found at target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar
    echo Please build the project first using Maven (mvn clean package)
    exit /b 1
)

echo Starting performance test for consensus algorithm...
echo Detailed logs will be saved to logs\performance-test.log

java %JAVA_OPTS% %LOGGING_CONFIG% %LOG_LEVEL% -cp target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.ConsensusPerformanceTest

echo Performance test completed. Check logs for results. 