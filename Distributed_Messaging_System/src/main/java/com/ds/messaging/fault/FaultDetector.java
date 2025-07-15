package com.ds.messaging.fault;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Detects failures in the distributed system by monitoring heartbeats from other nodes.
 */
public class FaultDetector implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(FaultDetector.class);
    
    private final Map<String, Instant> lastHeartbeats;
    private final long heartbeatTimeoutMs;
    private final ScheduledExecutorService scheduler;
    private final FailureHandler failureHandler;
    
    /**
     * Creates a new fault detector.
     * 
     * @param heartbeatTimeoutMs Time in milliseconds after which a node is considered failed if no heartbeat is received
     * @param scheduler Scheduler for periodic failure detection
     * @param failureHandler Handler to be called when a failure is detected
     */
    public FaultDetector(long heartbeatTimeoutMs, ScheduledExecutorService scheduler, FailureHandler failureHandler) {
        this.lastHeartbeats = new ConcurrentHashMap<>();
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.scheduler = scheduler;
        this.failureHandler = failureHandler;
        
        // Schedule periodic failure detection
        scheduleFailureDetection();
    }
    
    /**
     * Register a node to be monitored
     * 
     * @param nodeId The ID of the node to monitor
     */
    public void registerNode(String nodeId) {
        lastHeartbeats.put(nodeId, Instant.now());
        logger.info("Registered node for failure detection: {}", nodeId);
    }
    
    /**
     * Unregister a node from monitoring
     * 
     * @param nodeId The ID of the node to stop monitoring
     */
    public void unregisterNode(String nodeId) {
        lastHeartbeats.remove(nodeId);
        logger.info("Unregistered node from failure detection: {}", nodeId);
    }
    
    /**
     * Process a heartbeat message from a node
     * 
     * @param nodeId The ID of the node that sent the heartbeat
     */
    public void heartbeat(String nodeId) {
        lastHeartbeats.put(nodeId, Instant.now());
        logger.debug("Received heartbeat from: {}", nodeId);
    }
    
    /**
     * Check for failed nodes
     */
    private void detectFailures() {
        Instant now = Instant.now();
        
        for (Map.Entry<String, Instant> entry : lastHeartbeats.entrySet()) {
            String nodeId = entry.getKey();
            Instant lastHeartbeat = entry.getValue();
            
            long millisSinceLastHeartbeat = now.toEpochMilli() - lastHeartbeat.toEpochMilli();
            
            if (millisSinceLastHeartbeat > heartbeatTimeoutMs) {
                logger.warn("Node {} appears to have failed. Last heartbeat was {} ms ago",
                        nodeId, millisSinceLastHeartbeat);
                
                // Notify the failure handler
                failureHandler.handleFailure(nodeId);
                
                // Remove the node from monitoring
                lastHeartbeats.remove(nodeId);
            }
        }
    }
    
    /**
     * Schedule periodic failure detection
     */
    private void scheduleFailureDetection() {
        scheduler.scheduleAtFixedRate(this::detectFailures, 
                heartbeatTimeoutMs / 2, heartbeatTimeoutMs / 2, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public boolean handleMessage(Message message, String sourceNodeId) {
        // Only process heartbeat messages
        if (message.getType() == MessageType.HEARTBEAT) {
            heartbeat(message.getSenderId());
            return true;
        }
        return false;
    }
    
    /**
     * Interface for handling node failures.
     */
    public interface FailureHandler {
        /**
         * Called when a node failure is detected
         * 
         * @param nodeId The ID of the failed node
         */
        void handleFailure(String nodeId);
    }
} 