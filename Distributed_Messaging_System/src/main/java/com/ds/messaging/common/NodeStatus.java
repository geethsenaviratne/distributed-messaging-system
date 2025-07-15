package com.ds.messaging.common;

/**
 * Represents the different states a node can be in.
 */
public enum NodeStatus {
    /**
     * The node is starting up
     */
    STARTING,
    
    /**
     * The node is running normally
     */
    RUNNING,
    
    /**
     * The node is temporarily unavailable but will recover
     */
    DEGRADED,
    
    /**
     * The node has failed and is not recovering
     */
    FAILED,
    
    /**
     * The node is in the process of stopping
     */
    STOPPING,
    
    /**
     * The node has been stopped gracefully
     */
    STOPPED;
    
    /**
     * Check if the node is in a running state (either normal or degraded).
     *
     * @return true if the node is in a running state
     */
    public boolean isRunning() {
        return this == RUNNING || this == DEGRADED;
    }
} 