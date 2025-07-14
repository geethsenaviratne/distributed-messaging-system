package com.ds.messaging.consensus;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageStore;
import com.ds.messaging.common.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Integrates the Raft consensus algorithm with the messaging server.
 * Provides a high-level interface for submitting messages through the Raft protocol.
 */
public class RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
    
    private final String serverId;
    private final Set<String> peerServerIds;
    private final MessageStore messageStore;
    private final Consumer<Message> messageSender;
    
    private final RaftNode raftNode;
    private final RaftMessageAdapter messageAdapter;
    
    // Track message futures
    private final Map<String, CompletableFuture<Boolean>> messageFutures;
    
    // Message batching for improved throughput
    private final Queue<BatchEntry> messageBatchQueue;
    private final int maxBatchSize = 100; // Max messages per batch
    private final long maxBatchDelayMs = 10; // Max delay before processing a batch
    private ScheduledFuture<?> batchProcessorTask;
    private final Object batchLock = new Object();
    
    private static class BatchEntry {
        final Message message;
        final CompletableFuture<Boolean> future;
        
        BatchEntry(Message message, CompletableFuture<Boolean> future) {
            this.message = message;
            this.future = future;
        }
    }
    
    /**
     * Creates a new Raft server.
     * 
     * @param serverId The ID of this server
     * @param peerServerIds IDs of peer servers
     * @param messageStore Message store for committed messages
     * @param messageSender Function to send messages
     */
    public RaftServer(String serverId, Set<String> peerServerIds, 
                     MessageStore messageStore, Consumer<Message> messageSender) {
        this.serverId = serverId;
        this.peerServerIds = new HashSet<>(peerServerIds);
        this.messageStore = messageStore;
        this.messageSender = messageSender;
        this.messageFutures = new ConcurrentHashMap<>();
        this.messageBatchQueue = new ConcurrentLinkedQueue<>();
        
        // Create Raft message adapter
        this.messageAdapter = new RaftMessageAdapter(null, messageSender);
        
        // Create the Raft node with callbacks to send messages through the adapter
        this.raftNode = new RaftNode(
                serverId,
                peerServerIds,
                messageStore,
                this.messageAdapter::sendRequestVote,
                this.messageAdapter::sendAppendEntries,
                this.messageAdapter::sendInstallSnapshot,
                this.messageAdapter::sendRequestVoteResponse,
                this.messageAdapter::sendAppendEntriesResponse,
                this.messageAdapter::sendInstallSnapshotResponse
        );
        
        // Set the Raft node reference in the adapter
        try {
            java.lang.reflect.Field field = RaftMessageAdapter.class.getDeclaredField("raftNode");
            field.setAccessible(true);
            field.set(this.messageAdapter, this.raftNode);
        } catch (Exception e) {
            logger.error("Failed to set raftNode reference in adapter: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Start the Raft server.
     */
    public void start() {
        logger.info("Starting Raft server {}", serverId);
        raftNode.start();
        
        // Start batch processor
        batchProcessorTask = Executors.newSingleThreadScheduledExecutor()
            .scheduleWithFixedDelay(
                this::processBatch,
                0,
                maxBatchDelayMs,
                TimeUnit.MILLISECONDS
            );
        
        logger.info("Message batching enabled with max size {} and max delay {}ms", 
                maxBatchSize, maxBatchDelayMs);
    }
    
    /**
     * Stop the Raft server.
     */
    public void stop() {
        logger.info("Stopping Raft server {}", serverId);
        
        if (batchProcessorTask != null) {
            batchProcessorTask.cancel(false);
            processBatch(); // Process any remaining messages
        }
        
        raftNode.stop();
    }
    
    /**
     * Process a batch of messages together
     */
    private void processBatch() {
        List<BatchEntry> batch = new ArrayList<>();
        
        synchronized (batchLock) {
            BatchEntry entry;
            while ((entry = messageBatchQueue.poll()) != null && batch.size() < maxBatchSize) {
                batch.add(entry);
            }
        }
        
        if (batch.isEmpty()) {
            return;
        }
        
        if (batch.size() > 1) {
            logger.debug("Processing batch of {} messages", batch.size());
        }
        
        // Check if node is active before proceeding
        RaftState currentState = raftNode.getState();
        if (currentState != RaftState.FOLLOWER && currentState != RaftState.CANDIDATE && currentState != RaftState.LEADER) {
            logger.warn("Node is not active, cannot process batch of {} messages", batch.size());
            for (BatchEntry entry : batch) {
                entry.future.completeExceptionally(new IllegalStateException("Node is not active"));
            }
            return;
        }
        
        // Check leadership - but with retry logic for leadership transition
        boolean isCurrentlyLeader = raftNode.isLeader();
        if (!isCurrentlyLeader) {
            String currentLeader = raftNode.getCurrentLeader();
            
            // Special case: If we're in a leadership transition (we don't know who the leader is),
            // put these messages back in the queue for later processing
            if (currentLeader == null && raftNode.getState() == RaftState.CANDIDATE) {
                logger.info("Leadership transition in progress, requeueing {} messages for later processing", batch.size());
                synchronized (batchLock) {
                    for (BatchEntry entry : batch) {
                        messageBatchQueue.offer(entry);
                    }
                }
                return;
            }
            
            // Otherwise, complete exceptionally
            String errorMsg = currentLeader != null 
                ? "Not the leader. Current leader: " + currentLeader
                : "Not the leader and no leader is currently known";
            
            for (BatchEntry entry : batch) {
                entry.future.completeExceptionally(new IllegalStateException(errorMsg));
            }
            return;
        }
        
        // Process messages as a batch
        for (BatchEntry entry : batch) {
            Message message = entry.message;
            CompletableFuture<Boolean> future = entry.future;
            
            // Double-check leadership status before submitting each message to avoid race conditions
            if (!raftNode.isLeader()) {
                future.completeExceptionally(new IllegalStateException("No longer the leader during batch processing"));
                continue;
            }
            
            messageFutures.put(message.getId().toString(), future);
            
            // Clean up future when complete
            future.whenComplete((result, ex) -> {
                messageFutures.remove(message.getId().toString());
                if (ex != null) {
                    logger.error("Failed to process message {}: {}", message.getId(), ex.getMessage());
                } else {
                    logger.debug("Message {} successfully committed through Raft", message.getId());
                }
            });
            
            // Submit to the Raft node
            try {
                CompletableFuture<Boolean> nodeResult = raftNode.submitCommand(message);
                
                // Link the result of Raft processing to our future
                nodeResult.whenComplete((result, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(result);
                    }
                });
            } catch (Exception e) {
                String errorMsg = e.getMessage();
                if (errorMsg != null && (errorMsg.contains("Not the leader") || errorMsg.contains("Node is not active"))) {
                    logger.warn("Cannot process message {}: {}", message.getId(), errorMsg);
                } else {
                    logger.error("Error submitting message {} to Raft: {}", message.getId(), errorMsg, e);
                }
                future.completeExceptionally(e);
            }
        }
    }
    
    /**
     * Submit a message to the Raft cluster for consensus.
     * Now batches messages for improved throughput.
     * 
     * @param message The message to submit
     * @return A future that completes when the message is committed, or fails if this node is not the leader
     */
    public CompletableFuture<Boolean> submitMessage(Message message) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        
        // Check if the batch scheduler is shutdown
        if (batchProcessorTask == null || batchProcessorTask.isCancelled() || batchProcessorTask.isDone()) {
            logger.warn("Batch processor is not running, cannot submit message");
            future.completeExceptionally(
                new IllegalStateException("Batch processor is not running, cannot submit message"));
            return future;
        }
        
        synchronized (batchLock) {
            messageBatchQueue.add(new BatchEntry(message, future));
            
            // If we've reached max batch size, trigger immediate processing
            if (messageBatchQueue.size() >= maxBatchSize) {
                // Schedule immediate processing on a different thread to avoid blocking
                try {
                    CompletableFuture.runAsync(this::processBatch)
                        .exceptionally(ex -> {
                            logger.error("Error processing batch: {}", ex.getMessage(), ex);
                            return null;
                        });
                } catch (Exception e) {
                    logger.error("Failed to schedule batch processing: {}", e.getMessage(), e);
                    // Don't fail the future yet, as it may still be processed by the regular scheduler
                }
            }
        }
        
        return future;
    }
    
    /**
     * Handle a message from the messaging system.
     * 
     * @param message The message to handle
     * @param sourceNodeId The source node ID
     * @return true if the message was handled, false otherwise
     */
    public boolean handleMessage(Message message, String sourceNodeId) {
        // Pass to the message adapter for Raft messages
        if (isRaftMessage(message.getType())) {
            return messageAdapter.handleMessage(message, sourceNodeId);
        }
        
        // For user messages, submit to Raft if this is the leader
        if (message.getType() == MessageType.USER_MESSAGE) {
            if (raftNode.isLeader()) {
                submitMessage(message);
                return true;
            } else {
                // If not the leader, redirect
                String currentLeader = raftNode.getCurrentLeader();
                logger.info("Not the leader, redirecting message {} to leader {}", 
                        message.getId(), currentLeader);
                
                // In a real implementation, we would send a redirect message or response
                return false;
            }
        }
        
        return false;
    }
    
    /**
     * Check if this is a Raft-related message.
     * 
     * @param type The message type
     * @return true if this is a Raft message
     */
    private boolean isRaftMessage(MessageType type) {
        return type == MessageType.ELECTION || 
               type == MessageType.VOTE || 
               type == MessageType.LOG_REPLICATION || 
               type == MessageType.ACK || 
               type == MessageType.SYNC_REQUEST || 
               type == MessageType.SYNC_RESPONSE;
    }
    
    /**
     * Check if this server is the Raft leader.
     * 
     * @return true if this server is the leader
     */
    public boolean isLeader() {
        return raftNode.isLeader();
    }
    
    /**
     * Get the ID of the current leader.
     * 
     * @return The leader ID, or null if no leader is known
     */
    public String getCurrentLeader() {
        return raftNode.getCurrentLeader();
    }
    
    /**
     * Get the current Raft state.
     * 
     * @return The current state
     */
    public RaftState getState() {
        return raftNode.getState();
    }
    
    /**
     * Get the current term.
     * 
     * @return The current term
     */
    public int getCurrentTerm() {
        return raftNode.getCurrentTerm();
    }
} 