# Consensus Algorithm Performance Testing Guide

This guide explains how to evaluate the performance of the Raft consensus algorithm under high message transmission rates in the distributed messaging system.

## Overview

The performance testing framework creates a multi-server cluster with multiple clients and measures:

1. **Message throughput**: Number of messages processed per second
2. **Latency**: Time from message send to delivery (average, 95th and 99th percentiles)
3. **Success rate**: Percentage of messages successfully delivered
4. **Leadership transition time**: Time to elect a new leader when the current leader fails

## Test Architecture

The test creates the following components:
- 3 server nodes running Raft consensus algorithm
- 5 client nodes sending messages at configurable rates
- A systematic measurement framework tracking message statistics

## Running the Tests

The repository includes ready-to-use scripts for running the performance tests:

### On Linux/macOS:
```bash
./run-consensus-performance.sh
```

### On Windows:
```bash
run-consensus-performance.bat
```

## Test Parameters

The test is configured with the following default parameters, which can be modified in `ConsensusPerformanceTest.java`:

- **Number of servers**: 3 (recommended minimum for Raft)
- **Number of clients**: 5
- **Message size**: 1KB 
- **Message rates**: 100, 500, 1000, 2000, 5000 messages per second
- **Test duration**: 30 seconds per rate level (plus 10 seconds warmup)
- **Success threshold**: Test stops if success rate falls below 90%

## Understanding Results

The test produces a summary table with the following metrics for each message rate:

| Rate (msg/s) | Success (%) | Avg Latency (ms) | 95p Latency (ms) | 99p Latency (ms) | Leader Trans (ms) |
|--------------|-------------|------------------|------------------|------------------|------------------|
| 100          | 100.00      | 12.45           | 18.72            | 25.14            | 832              |
| 500          | 99.87       | 15.32           | 22.48            | 32.67            | 912              |
| 1000         | 98.92       | 24.87           | 35.67            | 48.23            | 1056             |
| ...          | ...         | ...              | ...              | ...              | ...              |

### Key Metrics Explained

1. **Rate**: The target message transmission rate in messages per second
2. **Success Rate**: Percentage of messages successfully delivered (100% is ideal)
3. **Average Latency**: Average time from send to delivery in milliseconds
4. **95th Percentile Latency**: 95% of messages were delivered within this time
5. **99th Percentile Latency**: 99% of messages were delivered within this time
6. **Leader Transition Time**: Time in milliseconds to elect a new leader and resume normal operation

## Interpreting the Results

The key considerations when interpreting results:

1. **Maximum Sustainable Rate**: The highest rate at which the system maintains high success rate (>95%)
2. **Latency Trends**: How latency increases with message rate
3. **Leader Transition Impact**: How leadership changes affect system stability

## Optimizing Performance

If performance doesn't meet requirements, consider these optimizations:

1. **Batch Size Adjustments**: Modify `RaftConfig.logBatchSize` to find optimal batching
2. **Heartbeat Interval**: Tune `RaftConfig.heartbeatIntervalMs` to balance network traffic and responsiveness
3. **Hardware Resources**: More CPU cores, faster disks, and network improvements
4. **JVM Tuning**: Adjust heap size, GC settings through `JAVA_OPTS` in the scripts

## Advanced Testing Scenarios

For more complex scenarios, modify the `ConsensusPerformanceTest.java` file:

1. **Controlled leader failure**: Test with different timing of leader failures
2. **Network partitioning**: Simulate network partitions between specific servers
3. **Variable message sizes**: Test with different message payload sizes
4. **Mixed workloads**: Combine different message types and read/write ratios

## Benchmarking Against Requirements

Use the performance results to assess whether the system meets specific requirements:

1. **Throughput requirements**: Can the system handle the expected message volume?
2. **Latency requirements**: Are messages delivered within acceptable time limits?
3. **Availability requirements**: Does the system recover quickly from leader failures?

## Detailed Logs

Detailed test results are available in `logs/performance-test.log`. These logs contain:
- Complete metrics for each test iteration
- Error messages and exceptions
- System state information during testing 