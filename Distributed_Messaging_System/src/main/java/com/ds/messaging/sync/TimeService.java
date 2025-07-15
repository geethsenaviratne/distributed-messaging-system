package com.ds.messaging.sync;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Basic implementation of a time synchronization service using a simplified NTP-like protocol.
 * This service helps ensure that all nodes in the distributed system have approximately
 * synchronized clocks for consistent message ordering.
 */
public class TimeService implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(TimeService.class);
    
    private final Node localNode;
    private final ScheduledExecutorService scheduler;
    private final Map<String, TimeOffset> nodeTimeOffsets;
    private final long syncIntervalMs;
    private TimeOffset averageOffset;
    
    /**
     * Creates a new time service.
     * 
     * @param localNode The local node running this service
     * @param scheduler Scheduler for periodic time sync
     * @param syncIntervalMs Interval in milliseconds between time synchronization attempts
     */
    public TimeService(Node localNode, ScheduledExecutorService scheduler, long syncIntervalMs) {
        this.localNode = localNode;
        this.scheduler = scheduler;
        this.syncIntervalMs = syncIntervalMs;
        this.nodeTimeOffsets = new ConcurrentHashMap<>();
        this.averageOffset = new TimeOffset(0, 0);
        
        // Schedule periodic time synchronization
        scheduleTimeSynchronization();
    }
    
    /**
     * Schedule periodic time synchronization messages.
     */
    private void scheduleTimeSynchronization() {
        scheduler.scheduleAtFixedRate(() -> {
            if (localNode.getStatus().isRunning()) {
                sendTimeSyncRequest();
                updateAverageOffset();
            }
        }, 0, syncIntervalMs, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Send a time synchronization request to all connected nodes.
     */
    private void sendTimeSyncRequest() {
        Instant requestTime = Instant.now();
        Message timeSyncMessage = new Message(
                localNode.getId(),
                "all",
                requestTime.toEpochMilli() + "",
                MessageType.TIME_SYNC
        );
        
        localNode.sendMessage(timeSyncMessage);
        logger.debug("Sent time sync request at {}", requestTime);
    }
    
    /**
     * Update the average clock offset based on all node offsets.
     */
    private void updateAverageOffset() {
        if (nodeTimeOffsets.isEmpty()) {
            return;
        }
        
        long totalOffset = 0;
        long totalLatency = 0;
        
        for (TimeOffset offset : nodeTimeOffsets.values()) {
            totalOffset += offset.getOffset();
            totalLatency += offset.getLatency();
        }
        
        long avgOffset = totalOffset / nodeTimeOffsets.size();
        long avgLatency = totalLatency / nodeTimeOffsets.size();
        
        this.averageOffset = new TimeOffset(avgOffset, avgLatency);
        logger.debug("Updated average time offset: {}ms, latency: {}ms", avgOffset, avgLatency);
    }
    
    /**
     * Get the current estimated time based on synchronized offset.
     * 
     * @return The current estimated time
     */
    public Instant getCurrentTime() {
        return Instant.now().plusMillis(averageOffset.getOffset());
    }
    
    /**
     * Get the current time offset.
     * 
     * @return The current time offset in milliseconds
     */
    public long getTimeOffset() {
        return averageOffset.getOffset();
    }
    
    /**
     * Get the average network latency.
     * 
     * @return The average network latency in milliseconds
     */
    public long getAverageLatency() {
        return averageOffset.getLatency();
    }
    
    @Override
    public boolean handleMessage(Message message, String sourceNodeId) {
        if (message.getType() == MessageType.TIME_SYNC) {
            // This is a time sync request from another node
            if (!message.getSenderId().equals(localNode.getId())) {
                handleTimeSyncRequest(message);
                return true;
            }
            // This is a reply to our time sync request
            else if (message.getRecipientId().equals(localNode.getId())) {
                handleTimeSyncResponse(message, sourceNodeId);
                return true;
            }
        }
        return false;
    }
    
    /**
     * Handle a time synchronization request from another node.
     * 
     * @param request The time sync request message
     */
    private void handleTimeSyncRequest(Message request) {
        try {
            long remoteTime = Long.parseLong(request.getContent());
            long localTime = Instant.now().toEpochMilli();
            
            // Send a reply with the remote timestamp and our current timestamp
            Message response = new Message(
                    localNode.getId(),
                    request.getSenderId(),
                    remoteTime + ":" + localTime,
                    MessageType.TIME_SYNC
            );
            
            localNode.sendMessage(response);
            logger.debug("Replied to time sync request from {}", request.getSenderId());
        } catch (NumberFormatException e) {
            logger.error("Invalid time sync request content: {}", request.getContent());
        }
    }
    
    /**
     * Handle a time synchronization response from another node.
     * 
     * @param response The time sync response message
     * @param sourceNodeId The ID of the node that sent the response
     */
    private void handleTimeSyncResponse(Message response, String sourceNodeId) {
        try {
            String[] parts = response.getContent().split(":");
            if (parts.length != 2) {
                logger.error("Invalid time sync response format: {}", response.getContent());
                return;
            }
            
            long t1 = Long.parseLong(parts[0]); // When we sent the request
            long t2 = Long.parseLong(parts[1]); // When they received the request
            long t3 = response.getTimestampInstant().toEpochMilli(); // When they sent the response
            long t4 = Instant.now().toEpochMilli(); // When we received the response
            
            // Calculate round-trip time and offset
            long roundTripTime = (t4 - t1) - (t3 - t2);
            long offset = ((t2 - t1) + (t3 - t4)) / 2;
            
            // Store the offset for this node
            nodeTimeOffsets.put(sourceNodeId, new TimeOffset(offset, roundTripTime));
            
            logger.debug("Time sync with {}: offset={}ms, latency={}ms", 
                    sourceNodeId, offset, roundTripTime);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            logger.error("Error processing time sync response: {}", e.getMessage());
        }
    }
    
    /**
     * Class representing a time offset and latency.
     */
    private static class TimeOffset {
        private final long offset;
        private final long latency;
        
        public TimeOffset(long offset, long latency) {
            this.offset = offset;
            this.latency = latency;
        }
        
        public long getOffset() {
            return offset;
        }
        
        public long getLatency() {
            return latency;
        }
    }
} 