package com.ds.messaging.common;

/**
 * Interface for handling messages received by a node.
 */
public interface MessageHandler {
    /**
     * Process a received message
     * 
     * @param message The message to process
     * @param sourceNodeId The ID of the node that sent the message
     * @return true if the message was handled successfully, false otherwise
     */
    boolean handleMessage(Message message, String sourceNodeId);
} 