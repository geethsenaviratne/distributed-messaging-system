package com.ds.messaging.server;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * In-memory implementation of the MessageStore interface.
 * Enhanced to support permanent storage with sequence-based retrieval.
 * Uses lock-free concurrency with ConcurrentHashMap and atomic operations.
 */
public class InMemoryMessageStore implements MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryMessageStore.class);
    
    // Primary storage by message ID
    private final ConcurrentHashMap<UUID, Message> messages;
    
    // Indexes for efficient retrieval
    private final ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<UUID, Boolean>> recipientMessages;
    private final ConcurrentHashMap<String, ConcurrentHashMap.KeySetView<UUID, Boolean>> senderMessages;
    
    // Sequence-based storage for ordered retrieval
    private final AtomicLong sequenceCounter = new AtomicLong(0);
    private final ConcurrentSkipListMap<Long, UUID> sequencedMessages = new ConcurrentSkipListMap<>();
    
    // Cache of recent messages for quick access (most recent 1000 messages)
    private final int maxRecentMessages = 1000;
    private final ConcurrentLinkedQueue<Message> recentMessages = new ConcurrentLinkedQueue<>();
    private final AtomicLong recentMessageCount = new AtomicLong(0);
    
    public InMemoryMessageStore() {
        this.messages = new ConcurrentHashMap<>();
        this.recipientMessages = new ConcurrentHashMap<>();
        this.senderMessages = new ConcurrentHashMap<>();
    }
    
    @Override
    public boolean storeMessage(Message message) {
        if (message == null) {
            return false;
        }
        
        // Store the message (putIfAbsent to prevent overwriting existing messages)
        Message existing = messages.putIfAbsent(message.getId(), message);
        if (existing != null) {
            // Message already exists, don't process it again
            logger.debug("Message already exists in store, skipping: {}", message.getId());
            return true;
        }
        
        // Update recipient index
        recipientMessages.computeIfAbsent(message.getRecipientId(), 
                k -> ConcurrentHashMap.newKeySet()).add(message.getId());
        
        // Update sender index
        senderMessages.computeIfAbsent(message.getSenderId(), 
                k -> ConcurrentHashMap.newKeySet()).add(message.getId());
        
        // Assign a sequence number if it doesn't have one
        long sequenceNumber = message.getSequenceNumber();
        if (sequenceNumber < 0) {
            sequenceNumber = sequenceCounter.incrementAndGet();
            message.setSequenceNumber(sequenceNumber);
        } else {
            // Update counter if needed using CAS operation
            long current;
            do {
                current = sequenceCounter.get();
                if (sequenceNumber <= current) {
                    break;
                }
            } while (!sequenceCounter.compareAndSet(current, sequenceNumber));
        }
        
        // Add to sequenced messages index
        sequencedMessages.put(sequenceNumber, message.getId());
        
        // Add to recent messages cache with thread-safe queue
        recentMessages.add(message);
        
        // Trim recent messages if needed
        long count = recentMessageCount.incrementAndGet();
        if (count > maxRecentMessages) {
            // Remove excess messages
            while (recentMessageCount.get() > maxRecentMessages && recentMessages.poll() != null) {
                recentMessageCount.decrementAndGet();
            }
        }
        
        logger.debug("Stored message: {} with sequence {}", message.getId(), sequenceNumber);
        return true;
    }
    
    @Override
    public Message getMessage(UUID messageId) {
        return messages.get(messageId);
    }
    
    @Override
    public List<Message> getMessagesForRecipient(String recipientId) {
        ConcurrentHashMap.KeySetView<UUID, Boolean> messageIds = recipientMessages.getOrDefault(
                recipientId, ConcurrentHashMap.newKeySet());
                
        return messageIds.stream()
                .map(messages::get)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Message::getSequenceNumber).reversed())
                .collect(Collectors.toList());
    }
    
    @Override
    public List<Message> getMessagesFromSender(String senderId) {
        ConcurrentHashMap.KeySetView<UUID, Boolean> messageIds = senderMessages.getOrDefault(
                senderId, ConcurrentHashMap.newKeySet());
                
        return messageIds.stream()
                .map(messages::get)
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(Message::getSequenceNumber).reversed())
                .collect(Collectors.toList());
    }
    
    /**
     * Gets messages ordered by sequence number, starting from a specific sequence.
     * 
     * @param startSequence The sequence number to start from (inclusive)
     * @param maxMessages The maximum number of messages to return
     * @return A list of messages in ascending sequence order
     */
    public List<Message> getMessagesBySequence(long startSequence, int maxMessages) {
        return sequencedMessages.tailMap(startSequence, true).values().stream()
                .limit(maxMessages)
                .map(messages::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    /**
     * Gets recent messages in reverse chronological order (newest first).
     * 
     * @param maxMessages The maximum number of messages to return
     * @return A list of recent messages
     */
    public List<Message> getRecentMessages(int maxMessages) {
        return recentMessages.stream()
                .limit(maxMessages)
                .collect(Collectors.toList());
    }
    
    /**
     * Gets the highest sequence number used in the message store.
     * 
     * @return The current sequence number
     */
    public long getCurrentSequence() {
        return sequenceCounter.get();
    }
    
    /**
     * Gets messages in a specific sequence range.
     * 
     * @param fromSequence The sequence number to start from (inclusive)
     * @param toSequence The sequence number to end at (inclusive)
     * @return A list of messages in the sequence range
     */
    public List<Message> getMessageRange(long fromSequence, long toSequence) {
        return sequencedMessages.subMap(fromSequence, true, toSequence, true).values().stream()
                .map(messages::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
    
    @Override
    public boolean deleteMessage(UUID messageId) {
        Message message = messages.remove(messageId);
        if (message != null) {
            // Remove from recipient index
            ConcurrentHashMap.KeySetView<UUID, Boolean> recipientMsgs = 
                    recipientMessages.get(message.getRecipientId());
            if (recipientMsgs != null) {
                recipientMsgs.remove(messageId);
            }
            
            // Remove from sender index
            ConcurrentHashMap.KeySetView<UUID, Boolean> senderMsgs = 
                    senderMessages.get(message.getSenderId());
            if (senderMsgs != null) {
                senderMsgs.remove(messageId);
            }
            
            // Remove from sequence index
            sequencedMessages.remove(message.getSequenceNumber());
            
            // We don't remove from recent messages as it would be too expensive
            // They'll naturally expire as new messages come in
            
            logger.debug("Deleted message: {}", messageId);
            return true;
        }
        return false;
    }
    
    @Override
    public int getMessageCount() {
        return messages.size();
    }
    
    /**
     * Gets messages that need to be synchronized for a node that is catching up.
     * 
     * @param lastKnownSequence The last sequence number the node has seen
     * @param maxMessages The maximum number of messages to return
     * @return A list of messages the node is missing
     */
    public List<Message> getSyncMessages(long lastKnownSequence, int maxMessages) {
        List<Message> messagesToSync = new ArrayList<>();
        
        // Use the sequence map to efficiently retrieve messages newer than lastKnownSequence
        NavigableMap<Long, UUID> newerMessages = sequencedMessages.tailMap(lastKnownSequence + 1, true);
        
        int count = 0;
        for (Map.Entry<Long, UUID> entry : newerMessages.entrySet()) {
            if (count >= maxMessages) {
                break;
            }
            
            UUID messageId = entry.getValue();
            Message message = messages.get(messageId);
            
            if (message != null) {
                messagesToSync.add(message);
                count++;
            }
        }
        
        logger.debug("Retrieved {} messages for sync with sequence > {}", 
                messagesToSync.size(), lastKnownSequence);
        
        return messagesToSync;
    }
} 