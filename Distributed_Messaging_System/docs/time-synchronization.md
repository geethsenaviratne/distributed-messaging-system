# Time Synchronization in Distributed Messaging System

This document describes the NTP-like time synchronization mechanism implemented in the distributed messaging system.

## Overview

Time synchronization is essential in distributed systems for:
- Properly ordering events
- Accurately timestamping messages
- Ensuring consistency in distributed operations
- Supporting time-based protocols and algorithms

Our implementation uses a simplified version of the Network Time Protocol (NTP) algorithm to maintain synchronized clocks across all nodes in the distributed messaging system.

## Architecture

The time synchronization system consists of the following components:

1. **TimeSync Class** - Core implementation of the NTP-like algorithm
2. **Time Sync Protocol Messages** - `TimeSyncRequestMessage` and `TimeSyncResponseMessage` classes
3. **TimestampedMessage Interface** - For standardized handling of message timestamps
4. **Integration with AbstractNode** - Base functionality for all nodes

## Time Server Selection

The system automatically selects a time server based on the following rules:

- In Raft mode: The Raft leader serves as the time server
- In Primary-Backup mode: The primary server serves as the time server
- Clients use their connected server as the time reference

## NTP Algorithm Implementation

Our implementation uses a simplified version of the NTP algorithm:

1. A client sends a request with timestamp T1 (client send time)
2. The server receives the request and records timestamp T2 (server receive time)
3. The server sends a response with T1, T2, and T3 (server send time)
4. The client receives the response and records timestamp T4 (client receive time)

The client then calculates:
- Round-trip delay: (T4 - T1) - (T3 - T2)
- Clock offset: ((T2 - T1) + (T3 - T4)) / 2

The system keeps track of multiple samples and uses the one with the lowest round-trip time for best accuracy.

## Timestamp Adjustment

Once clock offsets are determined, the system:
1. Adjusts outgoing message timestamps before sending
2. Adjusts incoming message timestamps upon receipt
3. Provides synchronized time through `getSynchronizedTime()` and `adjustTimestamp()` methods

## Benefits

This implementation provides:
- **Accuracy**: Typically within milliseconds on a local network
- **Fault Tolerance**: Each node can synchronize with any available server
- **Low Overhead**: Minimal network traffic (periodic synchronization only)
- **Transparency**: Application code can use standard timestamps without worrying about synchronization

## Limitations

The current implementation has some limitations:
- No handling of leap seconds
- No adjustment for clock drift (frequency differences between clocks)
- Limited security (no authentication of time sources)
- Simplified algorithm compared to full NTP protocol

## Testing & Demo

The system includes:
- `TimeSyncTest` - JUnit test for basic functionality
- `TimeSyncDemo` - Standalone demo showing synchronization with simulated clock skew

Run the demo with:
```
run-time-sync-demo.bat
```

## Performance Impact

The time synchronization adds minimal overhead:
- Network: A few small messages per sync interval (default: every 30 seconds)
- CPU: Negligible computation for timestamp adjustments
- Memory: Small footprint for tracking time offsets

## Future Improvements

Possible future enhancements:
- Full NTP implementation with multiple strata of time servers
- Integration with external time sources (e.g., atomic clocks, GPS)
- Clock drift compensation
- Security enhancements for time sources
- More sophisticated clock filtering algorithms 