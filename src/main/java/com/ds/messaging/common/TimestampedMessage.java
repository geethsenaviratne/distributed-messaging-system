package com.ds.messaging.common;

/**
 * Interface for messages that contain a timestamp.
 * Allows for standardized manipulation of message timestamps.
 */
public interface TimestampedMessage {
    /**
     * Gets the timestamp associated with this message
     * 
     * @return The timestamp in milliseconds since epoch
     */
    long getTimestamp();
    
    /**
     * Sets the timestamp for this message
     * 
     * @param timestamp The timestamp in milliseconds since epoch
     */
    void setTimestamp(long timestamp);
} 