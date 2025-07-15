package com.ds.messaging.common;

/**
 * Enum representing different types of messages in the system.
 * This helps the system handle different messages in appropriate ways.
 */
public enum MessageType {
    /**
     * Regular user message
     */
    USER_MESSAGE,
    
    /**
     * System heartbeat message for failure detection
     */
    HEARTBEAT,
    
    /**
     * Message used for leader election in Raft consensus
     */
    ELECTION,
    
    /**
     * Message used for voting in Raft consensus
     */
    VOTE,
    
    /**
     * Message used for log replication in Raft consensus
     */
    LOG_REPLICATION,
    
    /**
     * Legacy message for time synchronization (deprecated)
     */
    TIME_SYNC,
    
    /**
     * Request message for time synchronization using NTP-like protocol
     */
    TIME_SYNC_REQUEST,
    
    /**
     * Response message for time synchronization using NTP-like protocol
     */
    TIME_SYNC_RESPONSE,
    
    /**
     * Acknowledgement message
     */
    ACK,
    
    /**
     * Identification message used when nodes connect to each other
     */
    IDENTIFICATION,
    
    /**
     * Request message
     */
    REQUEST,
    
    /**
     * Response message
     */
    RESPONSE,
    
    /**
     * Error message
     */
    ERROR,
    
    /**
     * Quorum acknowledgment message for tracking replication status
     */
    QUORUM_ACK,
    
    /**
     * Synchronization request for replica recovery
     */
    SYNC_REQUEST,
    
    /**
     * Synchronization response containing missing messages
     */
    SYNC_RESPONSE,
    
    /**
     * Message forwarding for Raft - used to forward client messages to the leader
     */
    FORWARD,
    
    /**
     * Synchronization data containing full message content
     */
    SYNC_DATA
} 