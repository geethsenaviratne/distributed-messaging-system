package com.ds.messaging.common;

import java.util.List;
import java.util.UUID;

/**
 * Interface for storing and retrieving messages in the system.
 * This provides fault tolerance by ensuring messages are persisted.
 * Enhanced to support sequence-based retrieval and synchronization.
 */
public interface MessageStore {
    /**
     * Store a message in the message store
     * 
     * @param message The message to store
     * @return true if the message was stored successfully, false otherwise
     */
    boolean storeMessage(Message message);
    
    /**
     * Retrieve a message by its ID
     * 
     * @param messageId The ID of the message to retrieve
     * @return The message, or null if not found
     */
    Message getMessage(UUID messageId);
    
    /**
     * Retrieve all messages for a specific recipient
     * 
     * @param recipientId The ID of the recipient
     * @return A list of messages for the recipient
     */
    List<Message> getMessagesForRecipient(String recipientId);
    
    /**
     * Retrieve all messages sent by a specific sender
     * 
     * @param senderId The ID of the sender
     * @return A list of messages from the sender
     */
    List<Message> getMessagesFromSender(String senderId);
    
    /**
     * Delete a message from the store
     * 
     * @param messageId The ID of the message to delete
     * @return true if the message was deleted, false otherwise
     */
    boolean deleteMessage(UUID messageId);
    
    /**
     * Get the total number of messages in the store
     * 
     * @return The number of messages
     */
    int getMessageCount();
    
    /**
     * Gets messages ordered by sequence number, starting from a specific sequence.
     * 
     * @param startSequence The sequence number to start from (inclusive)
     * @param maxMessages The maximum number of messages to return
     * @return A list of messages in ascending sequence order
     */
    default List<Message> getMessagesBySequence(long startSequence, int maxMessages) {
        throw new UnsupportedOperationException("Sequence-based retrieval not supported");
    }
    
    /**
     * Gets recent messages in reverse chronological order (newest first).
     * 
     * @param maxMessages The maximum number of messages to return
     * @return A list of recent messages
     */
    default List<Message> getRecentMessages(int maxMessages) {
        throw new UnsupportedOperationException("Recent messages retrieval not supported");
    }
    
    /**
     * Gets the highest sequence number used in the message store.
     * 
     * @return The current sequence number
     */
    default long getCurrentSequence() {
        throw new UnsupportedOperationException("Sequence tracking not supported");
    }
    
    /**
     * Gets messages in a specific sequence range.
     * 
     * @param fromSequence The sequence number to start from (inclusive)
     * @param toSequence The sequence number to end at (inclusive)
     * @return A list of messages in the sequence range
     */
    default List<Message> getMessageRange(long fromSequence, long toSequence) {
        throw new UnsupportedOperationException("Sequence range retrieval not supported");
    }
    
    /**
     * Gets messages that need to be synchronized for a node that is catching up.
     * 
     * @param lastKnownSequence The last sequence number the node has seen
     * @param maxMessages The maximum number of messages to return
     * @return A list of messages the node is missing
     */
    default List<Message> getSyncMessages(long lastKnownSequence, int maxMessages) {
        throw new UnsupportedOperationException("Synchronization not supported");
    }
} 