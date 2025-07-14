@echo off
echo =====================================================
echo Raft Consensus Algorithm Performance Test
echo =====================================================
echo.
echo Testing high message rates and measuring:
echo 1. Message throughput
echo 2. Message latency (avg, 95p, 99p)
echo 3. Success rate
echo 4. Leader election time under load
echo.

rem Set Java options for the test
set JAVA_OPTS=-Xmx2G -Dlogback.configurationFile=config/logback-performance.xml -Draft.enabled=true

rem Compile if needed
if not exist "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" (
  echo Building project...
  call mvn clean package -DskipTests
)

rem Run the test
echo Starting performance test...
java %JAVA_OPTS% -cp "target\distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar" com.ds.messaging.ConsensusPerformanceTest

echo.
echo Test completed. See logs for detailed results. 