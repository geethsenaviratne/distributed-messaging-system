package com.ds.messaging.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract base implementation of the Node interface with common functionality.
 */
public abstract class AbstractNode implements Node {
    private static final Logger logger = LoggerFactory.getLogger(AbstractNode.class);
    
    protected final String id;
    protected NodeStatus status;
    protected final List<MessageHandler> messageHandlers;
    protected TimeSync timeSync;
    protected String timeSyncServerId;
    
    /**
     * Creates a new node with a random UUID
     */
    public AbstractNode() {
        this.id = UUID.randomUUID().toString();
        this.status = NodeStatus.STOPPED;
        this.messageHandlers = new CopyOnWriteArrayList<>();
    }
    
    /**
     * Creates a new node with the specified ID
     * 
     * @param id The ID for this node
     */
    public AbstractNode(String id) {
        this.id = id;
        this.status = NodeStatus.STOPPED;
        this.messageHandlers = new CopyOnWriteArrayList<>();
    }
    
    @Override
    public String getId() {
        return id;
    }
    
    @Override
    public NodeStatus getStatus() {
        return status;
    }
    
    @Override
    public void registerMessageHandler(MessageHandler handler) {
        messageHandlers.add(handler);
        logger.debug("Registered message handler: {}", handler);
    }
    
    /**
     * Register a message handler for a specific message type.
     * This creates a filtered handler that only processes messages of the specified type.
     * 
     * @param type The message type to handle
     * @param handler The handler implementation that will process messages of the specified type
     */
    public void registerMessageHandler(MessageType type, MessageHandler handler) {
        MessageHandler typedHandler = new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceNodeId) {
                if (message.getType() == type) {
                    return handler.handleMessage(message, sourceNodeId);
                }
                return false;
            }
            
            @Override
            public String toString() {
                return "TypedMessageHandler{type=" + type + ", handler=" + handler + "}";
            }
        };
        
        registerMessageHandler(typedHandler);
        logger.debug("Registered message handler for type {}: {}", type, handler);
    }
    
    /**
     * Process a received message by passing it to all registered handlers
     * 
     * @param message The message to process
     * @param sourceNodeId The ID of the node that sent the message
     * @return true if at least one handler processed the message successfully
     */
    protected boolean processMessage(Message message, String sourceNodeId) {
        logger.debug("Processing message: {}", message);
        
        if (messageHandlers.isEmpty()) {
            logger.warn("No message handlers registered");
            return false;
        }
        
        boolean handled = false;
        for (MessageHandler handler : messageHandlers) {
            try {
                if (handler.handleMessage(message, sourceNodeId)) {
                    handled = true;
                }
            } catch (Exception e) {
                logger.error("Error handling message: {}", e.getMessage(), e);
            }
        }
        
        return handled;
    }
    
    /**
     * Sets up time synchronization for this node
     * 
     * @param timeSyncServerId The ID of the time server node (null if this node is the time server)
     */
    public void setupTimeSync(String timeSyncServerId) {
        this.timeSyncServerId = timeSyncServerId;
        this.timeSync = new TimeSync(id, this, timeSyncServerId);
        logger.info("Time synchronization set up with server: {}", 
                timeSyncServerId != null ? timeSyncServerId : "self (this node is time server)");
    }
    
    /**
     * Gets the current time, adjusted for clock synchronization if time sync is enabled
     * 
     * @return The current synchronized time
     */
    public long getSynchronizedTime() {
        if (timeSync != null) {
            return timeSync.getCurrentTime();
        }
        return System.currentTimeMillis();
    }
    
    /**
     * Converts a local timestamp to a synchronized timestamp
     * 
     * @param localTimestamp The local timestamp to adjust
     * @return The adjusted timestamp
     */
    public long adjustTimestamp(long localTimestamp) {
        if (timeSync != null) {
            return timeSync.getAdjustedTime(localTimestamp);
        }
        return localTimestamp;
    }
    
    /**
     * Gets the current clock offset from the reference server
     * 
     * @return The clock offset in milliseconds, or 0 if time sync is not enabled
     */
    public long getClockOffset() {
        if (timeSync != null) {
            return timeSync.getCurrentOffset();
        }
        return 0;
    }
    
    @Override
    public void start() throws IOException {
        status = NodeStatus.STARTING;
        logger.info("Starting node: {}", id);
        
        // Start time synchronization if configured
        if (timeSync != null) {
            timeSync.start();
            logger.info("Started time synchronization");
        }
    }
    
    @Override
    public void stop() {
        status = NodeStatus.STOPPING;
        logger.info("Stopping node: {}", id);
        
        // Stop time synchronization if configured
        if (timeSync != null) {
            timeSync.stop();
            logger.info("Stopped time synchronization");
        }
        
        status = NodeStatus.STOPPED;
    }
    
    @Override
    public List<String> getConnectedNodes() {
        return new ArrayList<>();
    }
} 