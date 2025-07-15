package com.ds.messaging.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implements NTP-like time synchronization for the distributed messaging system.
 * This class provides functionality to synchronize clocks across distributed nodes,
 * using a simplified NTP-like algorithm that:
 * 1. Measures round-trip times to calculate network latency
 * 2. Computes clock offsets between nodes
 * 3. Adjusts local time perception based on a reference server clock
 */
public class TimeSync {
    private static final Logger logger = LoggerFactory.getLogger(TimeSync.class);
    private static final int DEFAULT_SYNC_INTERVAL_SECONDS = 30;
    private static final int SAMPLES_FOR_SYNC = 8;
    
    private final String nodeId;
    private final Node localNode;
    private final String timeServerNodeId;
    private final Map<String, ClockOffset> clockOffsets;
    private final ScheduledExecutorService scheduler;
    private boolean isTimeServer;
    
    // Estimated offset between local clock and reference clock
    private volatile long currentOffset = 0;
    // Minimum round-trip time observed (used for best offset calculation)
    private volatile long minRoundTripTime = Long.MAX_VALUE;
    
    /**
     * Creates a new TimeSync instance for a node
     * 
     * @param nodeId The ID of the local node
     * @param localNode The local node instance
     * @param timeServerNodeId The ID of the designated time server (can be null if this is the time server)
     */
    public TimeSync(String nodeId, Node localNode, String timeServerNodeId) {
        this.nodeId = nodeId;
        this.localNode = localNode;
        this.timeServerNodeId = timeServerNodeId;
        this.isTimeServer = timeServerNodeId == null || timeServerNodeId.equals(nodeId);
        this.clockOffsets = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        logger.info("TimeSync initialized for node {}. Is time server: {}", nodeId, isTimeServer);
    }
    
    /**
     * Starts the time synchronization service
     */
    public void start() {
        // Register the time sync message handler
        localNode.registerMessageHandler(new TimeSyncHandler());
        
        // If not a time server, start requesting time from server
        if (!isTimeServer) {
            scheduler.scheduleAtFixedRate(this::synchronizeTime, 0, 
                    DEFAULT_SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
            logger.info("Started time synchronization with server {} at {} second intervals", 
                    timeServerNodeId, DEFAULT_SYNC_INTERVAL_SECONDS);
        }
    }
    
    /**
     * Stops the time synchronization service
     */
    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Stopped time synchronization service");
    }
    
    /**
     * Initiates a time synchronization request to the time server
     */
    private void synchronizeTime() {
        if (isTimeServer) {
            return; // Time server doesn't need to sync with itself
        }
        
        try {
            // Create and send a time sync request message
            TimeSyncRequestMessage request = new TimeSyncRequestMessage(
                    nodeId, timeServerNodeId, System.currentTimeMillis());
            
            logger.debug("Sending time sync request to server {}", timeServerNodeId);
            localNode.sendMessage(request);
        } catch (Exception e) {
            logger.error("Error sending time sync request: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Updates the clock offset based on a time sync response
     * 
     * @param response The time sync response message
     * @param roundTripTime The measured round-trip time
     */
    private void processTimeSync(TimeSyncResponseMessage response, long roundTripTime) {
        // Calculate the clock offset using a simplified NTP formula:
        // offset = ((T2 - T1) + (T3 - T4)) / 2
        // where:
        // T1 = client send timestamp
        // T2 = server receive timestamp
        // T3 = server send timestamp
        // T4 = client receive timestamp
        
        long t1 = response.getClientSendTime();
        long t2 = response.getServerReceiveTime();
        long t3 = response.getServerSendTime();
        long t4 = System.currentTimeMillis();
        
        long offset = ((t2 - t1) + (t3 - t4)) / 2;
        
        // Store this sample if it has a lower round-trip time
        if (roundTripTime < minRoundTripTime) {
            minRoundTripTime = roundTripTime;
            currentOffset = offset;
            logger.debug("Updated clock offset to {} ms with RTT {} ms", offset, roundTripTime);
        }
        
        // Store the offset for this server
        clockOffsets.put(response.getSenderId(), new ClockOffset(offset, roundTripTime));
    }
    
    /**
     * Adjusts a timestamp from the local clock to the synchronized time
     * 
     * @param localTimestamp The local timestamp to adjust
     * @return The adjusted timestamp
     */
    public long getAdjustedTime(long localTimestamp) {
        if (isTimeServer) {
            return localTimestamp; // Time server is the reference, no adjustment needed
        }
        return localTimestamp + currentOffset;
    }
    
    /**
     * Gets the current system time adjusted for clock offset
     * 
     * @return The current synchronized time
     */
    public long getCurrentTime() {
        return getAdjustedTime(System.currentTimeMillis());
    }
    
    /**
     * Gets the current estimated offset from the reference clock
     * 
     * @return The current clock offset in milliseconds
     */
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    /**
     * Handles time synchronization messages
     */
    private class TimeSyncHandler implements MessageHandler {
        @Override
        public boolean handleMessage(Message message, String sourceNodeId) {
            if (message instanceof TimeSyncRequestMessage) {
                if (isTimeServer) {
                    handleTimeSyncRequest((TimeSyncRequestMessage) message);
                    return true;
                }
            } else if (message instanceof TimeSyncResponseMessage) {
                if (!isTimeServer) {
                    handleTimeSyncResponse((TimeSyncResponseMessage) message);
                    return true;
                }
            }
            return false;
        }
        
        private void handleTimeSyncRequest(TimeSyncRequestMessage request) {
            // Process time sync request and respond with current server time
            TimeSyncResponseMessage response = new TimeSyncResponseMessage(
                    nodeId,
                    request.getSenderId(),
                    request.getClientSendTime(),
                    System.currentTimeMillis(),
                    System.currentTimeMillis()
            );
            
            localNode.sendMessage(response);
            logger.debug("Responded to time sync request from {}", request.getSenderId());
        }
        
        private void handleTimeSyncResponse(TimeSyncResponseMessage response) {
            // Calculate round-trip time
            long receiveTime = System.currentTimeMillis();
            long clientSendTime = response.getClientSendTime();
            long roundTripTime = receiveTime - clientSendTime;
            
            // Process and update the time offset
            processTimeSync(response, roundTripTime);
            logger.debug("Processed time sync response from {} with RTT {} ms", 
                    response.getSenderId(), roundTripTime);
        }
    }
    
    /**
     * Stores clock offset information for a server
     */
    private static class ClockOffset {
        private final long offset;
        private final long roundTripTime;
        private final long timestamp;
        
        public ClockOffset(long offset, long roundTripTime) {
            this.offset = offset;
            this.roundTripTime = roundTripTime;
            this.timestamp = System.currentTimeMillis();
        }
        
        public long getOffset() {
            return offset;
        }
        
        public long getRoundTripTime() {
            return roundTripTime;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
    }
} 