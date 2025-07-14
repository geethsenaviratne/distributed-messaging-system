package com.ds.messaging.common;

import java.io.IOException;
import java.util.List;

/**
 * Interface representing a node in the distributed messaging system.
 * Both clients and servers implement this interface.
 */
public interface Node {
    /**
     * Gets the unique identifier for this node
     * @return The node's ID
     */
    String getId();
    
    /**
     * Start the node, initializing connections and services
     * @throws IOException if there's an error starting the node
     */
    void start() throws IOException;
    
    /**
     * Stop the node, cleaning up resources
     */
    void stop();
    
    /**
     * Send a message to another node
     * @param message The message to send
     * @return true if the message was sent successfully, false otherwise
     */
    boolean sendMessage(Message message);
    
    /**
     * Get the current status of the node
     * @return The node status
     */
    NodeStatus getStatus();
    
    /**
     * Register a message handler to be called when a message is received
     * @param handler The handler to register
     */
    void registerMessageHandler(MessageHandler handler);
    
    /**
     * Get a list of connected nodes
     * @return List of connected node IDs
     */
    List<String> getConnectedNodes();
} 