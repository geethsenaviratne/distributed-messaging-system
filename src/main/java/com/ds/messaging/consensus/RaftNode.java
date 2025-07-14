package com.ds.messaging.consensus;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageStore;
import com.ds.messaging.consensus.RaftRPC.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Implementation of a node in the Raft consensus algorithm.
 * Handles state transitions, elections, log replication, and leader functionality.
 */
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
    
    // Node identification
    private final String nodeId;
    private final Set<String> peerNodeIds;
    
    // Raft state
    private volatile RaftState state;
    private volatile int currentTerm;
    private volatile String votedFor;
    private volatile String currentLeader;
    
    // Log and state machine
    private final RaftLog log;
    private final MessageStore messageStore;
    
    // Persistence
    private final Path persistencePath;
    private final ReentrantReadWriteLock persistenceLock;
    
    // Leader state (reinitialized after election)
    private volatile Map<String, Integer> nextIndex;
    private volatile Map<String, Integer> matchIndex;
    
    // Scheduling and concurrency
    private final ScheduledExecutorService scheduler;
    private final ExecutorService workerExecutor;
    private volatile ScheduledFuture<?> electionTimeout;
    private volatile ScheduledFuture<?> heartbeatTask;
    private final AtomicBoolean running;
    
    // Election timeout randomization
    private final Random random;
    
    // Callbacks for RPCs
    private final Consumer<RequestVoteRPC> requestVoteCallback;
    private final Consumer<AppendEntriesRPC> appendEntriesCallback;
    private final Consumer<InstallSnapshotRPC> installSnapshotCallback;
    private final Consumer<RequestVoteResponse> requestVoteResponseCallback;
    private final Consumer<AppendEntriesResponse> appendEntriesResponseCallback;
    private final Consumer<InstallSnapshotResponse> installSnapshotResponseCallback;
    
    /**
     * Create a new Raft node.
     * 
     * @param nodeId ID of this node
     * @param peerNodeIds IDs of peer nodes
     * @param messageStore Store for committed messages
     * @param requestVoteCallback Callback for sending RequestVote RPCs
     * @param appendEntriesCallback Callback for sending AppendEntries RPCs
     * @param installSnapshotCallback Callback for sending InstallSnapshot RPCs
     * @param requestVoteResponseCallback Callback for sending RequestVote responses
     * @param appendEntriesResponseCallback Callback for sending AppendEntries responses
     * @param installSnapshotResponseCallback Callback for sending InstallSnapshot responses
     */
    public RaftNode(String nodeId, 
                   Set<String> peerNodeIds,
                   MessageStore messageStore,
                   Consumer<RequestVoteRPC> requestVoteCallback,
                   Consumer<AppendEntriesRPC> appendEntriesCallback,
                   Consumer<InstallSnapshotRPC> installSnapshotCallback,
                   Consumer<RequestVoteResponse> requestVoteResponseCallback,
                   Consumer<AppendEntriesResponse> appendEntriesResponseCallback,
                   Consumer<InstallSnapshotResponse> installSnapshotResponseCallback) {
        this.nodeId = nodeId;
        this.peerNodeIds = new HashSet<>(peerNodeIds);
        this.messageStore = messageStore;
        this.requestVoteCallback = requestVoteCallback;
        this.appendEntriesCallback = appendEntriesCallback;
        this.installSnapshotCallback = installSnapshotCallback;
        this.requestVoteResponseCallback = requestVoteResponseCallback;
        this.appendEntriesResponseCallback = appendEntriesResponseCallback;
        this.installSnapshotResponseCallback = installSnapshotResponseCallback;
        
        this.log = new RaftLog(nodeId);
        this.state = RaftState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.currentLeader = null;
        
        this.persistencePath = Paths.get("logs", "raft", nodeId);
        this.persistenceLock = new ReentrantReadWriteLock();
        
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        
        // Increase thread pool size and use a more robust rejection policy
        ThreadPoolExecutor schedulerExecutor = new ThreadPoolExecutor(
            4, 8, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactory() {
                private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = defaultFactory.newThread(r);
                    t.setName("raft-scheduler-" + t.getName());
                    t.setDaemon(true);
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        this.scheduler = new ScheduledThreadPoolExecutor(4, schedulerExecutor.getThreadFactory());
        ((ScheduledThreadPoolExecutor)this.scheduler).setRemoveOnCancelPolicy(true);
        ((ScheduledThreadPoolExecutor)this.scheduler).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        
        this.workerExecutor = Executors.newFixedThreadPool(8); // Increased from 4
        this.running = new AtomicBoolean(false);
        this.random = new Random();
        
        // Initialize storage directory
        try {
            Files.createDirectories(persistencePath);
        } catch (IOException e) {
            logger.error("Failed to create persistence directory: {}", e.getMessage());
        }
        
        // Load persistent state
        loadPersistentState();
    }
    
    /**
     * Start the Raft node.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting Raft node {}", nodeId);
            
            // Start as a follower
            becomeFollower(currentTerm);
            
            // Start applying committed entries
            scheduler.scheduleWithFixedDelay(this::applyCommittedEntries, 100, 100, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Stop the Raft node.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Raft node {}", nodeId);
            
            // Cancel scheduled tasks
            if (electionTimeout != null) {
                electionTimeout.cancel(false);
            }
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
            }
            
            // Shutdown executors with proper handling
            try {
                // First attempt orderly shutdown
                scheduler.shutdown();
                workerExecutor.shutdown();
                
                // Wait for tasks to complete
                boolean schedulerTerminated = scheduler.awaitTermination(5, TimeUnit.SECONDS);
                boolean workerTerminated = workerExecutor.awaitTermination(5, TimeUnit.SECONDS);
                
                // If orderly shutdown fails, force shutdown
                if (!schedulerTerminated) {
                    logger.warn("Scheduler did not terminate in time, forcing shutdown");
                    List<Runnable> droppedTasks = scheduler.shutdownNow();
                    logger.warn("Dropped {} tasks from scheduler", droppedTasks.size());
                }
                
                if (!workerTerminated) {
                    logger.warn("Worker executor did not terminate in time, forcing shutdown");
                    List<Runnable> droppedTasks = workerExecutor.shutdownNow();
                    logger.warn("Dropped {} tasks from worker executor", droppedTasks.size());
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for thread pools to terminate", e);
                Thread.currentThread().interrupt();
                
                // Force shutdown if interrupted
                scheduler.shutdownNow();
                workerExecutor.shutdownNow();
            }
            
            logger.info("Raft node {} stopped", nodeId);
        }
    }
    
    /**
     * Submit a command to the Raft cluster.
     * If this node is the leader, it will append the command to its log and replicate it.
     * If this node is not the leader, it will redirect to the leader if known.
     * 
     * @param command The command (message) to submit
     * @return A future that completes when the command is committed, or fails if not the leader
     */
    public CompletableFuture<Boolean> submitCommand(Message command) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        
        // Check if we're still running
        if (!isActive()) {
            logger.warn("Cannot submit command: node is not active");
            future.completeExceptionally(
                new IllegalStateException("Node is not active"));
            return future;
        }
        
        if (state != RaftState.LEADER) {
            if (currentLeader != null) {
                logger.info("Not the leader, current leader is {}", currentLeader);
                future.completeExceptionally(
                        new IllegalStateException("Not the leader. Current leader: " + currentLeader));
            } else {
                logger.info("Not the leader and no leader known");
                future.completeExceptionally(
                        new IllegalStateException("Not the leader and no leader is currently known"));
            }
            return future;
        }
        
        // We are the leader, append to our log
        try {
            workerExecutor.execute(() -> {
                try {
                    int index = log.append(currentTerm, command);
                    logger.debug("Appended command {} to log at index {}", command.getId(), index);
                    
                    // Track this command for completion
                    trackCommandCompletion(index, future);
                    
                    // Replicate to followers
                    replicateLogToFollowers();
                } catch (Exception e) {
                    logger.error("Error processing command: {}", e.getMessage(), e);
                    future.completeExceptionally(e);
                }
            });
        } catch (RejectedExecutionException e) {
            // Handle case where executor rejects the task (e.g., during shutdown)
            logger.warn("Command rejected by executor, likely during shutdown: {}", e.getMessage());
            future.completeExceptionally(
                new IllegalStateException("Command rejected by executor, likely during shutdown", e));
        }
        
        return future;
    }
    
    /**
     * Track a command's completion based on its log index.
     * The future will complete when the command is committed.
     * 
     * @param index The log index of the command
     * @param future The future to complete
     */
    private void trackCommandCompletion(int index, CompletableFuture<Boolean> future) {
        // Check if already committed
        if (index <= log.getCommitIndex()) {
            future.complete(true);
            return;
        }
        
        // Check if we're still running
        if (!isActive()) {
            logger.warn("Cannot track command completion: node is not active");
            future.completeExceptionally(
                new IllegalStateException("Node is not active"));
            return;
        }
        
        try {
            // Create atomic reference to store scheduled future for cancellation
            final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();
            
            // Schedule a task that will check for commitment and clean itself up
            ScheduledFuture<?> checkTask = scheduler.scheduleAtFixedRate(() -> {
                try {
                    // Check if the command is committed
                    if (index <= log.getCommitIndex()) {
                        future.complete(true);
                        ScheduledFuture<?> task = futureRef.get();
                        if (task != null) {
                            task.cancel(false);
                        }
                        return;
                    }
                    
                    // Check if we're still leader and running
                    if (!isActive() || state != RaftState.LEADER) {
                        future.completeExceptionally(
                                new IllegalStateException("No longer the leader or node not active"));
                        ScheduledFuture<?> task = futureRef.get();
                        if (task != null) {
                            task.cancel(false);
                        }
                        return;
                    }
                } catch (Exception e) {
                    // Handle any exceptions to prevent task from dying silently
                    logger.error("Error in command completion check for index {}: {}", index, e.getMessage(), e);
                    future.completeExceptionally(e);
                    ScheduledFuture<?> task = futureRef.get();
                    if (task != null) {
                        task.cancel(false);
                    }
                }
            }, 50, 50, TimeUnit.MILLISECONDS);
            
            // Store the future for cancellation
            futureRef.set(checkTask);
            
            // Set a timeout to prevent hanging futures
            if (!scheduler.isShutdown()) {
                scheduler.schedule(() -> {
                    if (!future.isDone()) {
                        future.completeExceptionally(
                            new TimeoutException("Command commitment check timed out after 15 seconds"));
                        checkTask.cancel(false);
                    }
                }, 15, TimeUnit.SECONDS);
            }
        } catch (RejectedExecutionException e) {
            // Handle case where scheduler rejects the task (e.g., during shutdown)
            logger.warn("Task rejected by scheduler, likely during shutdown: {}", e.getMessage());
            future.completeExceptionally(
                new IllegalStateException("Task rejected by scheduler, likely during shutdown", e));
        }
    }
    
    /**
     * Check if this node is active and can accept new tasks.
     * Thread-safe method to check the node's running state and executor health.
     * 
     * @return true if the node is active and healthy
     */
    public synchronized boolean isActive() {
        return running.get() 
               && !scheduler.isShutdown() 
               && !scheduler.isTerminated()
               && !workerExecutor.isShutdown() 
               && !workerExecutor.isTerminated();
    }
    
    /**
     * Apply committed entries to the state machine.
     */
    private void applyCommittedEntries() {
        if (!isActive()) {
            return;
        }
        
        List<LogEntry> entriesToApply = log.getEntriesToApply();
        if (entriesToApply.isEmpty()) {
            return;
        }
        
        logger.debug("Applying {} committed entries to state machine", entriesToApply.size());
        
        for (LogEntry entry : entriesToApply) {
            // Apply the command to the state machine
            if (entry.getCommand() != null) {
                messageStore.storeMessage(entry.getCommand());
                logger.debug("Applied entry {} to state machine: {}", 
                        entry.getIndex(), entry.getCommand().getId());
            }
            
            // Update the last applied index
            log.setLastApplied(entry.getIndex());
        }
    }
    
    //
    // State transitions
    //
    
    /**
     * Transition to follower state.
     * 
     * @param term The current term
     */
    private void becomeFollower(int term) {
        logger.info("Transitioning to FOLLOWER state for term {}", term);
        
        // Update term if necessary
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            savePersistentState();
        }
        
        state = RaftState.FOLLOWER;
        
        // Cancel any leader heartbeats
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
        }
        
        // Reset election timeout
        resetElectionTimeout();
    }
    
    /**
     * Transition to candidate state and start an election.
     */
    private void becomeCandidate() {
        logger.info("Transitioning to CANDIDATE state");
        
        state = RaftState.CANDIDATE;
        currentTerm++;
        votedFor = nodeId; // Vote for self
        currentLeader = null;
        
        savePersistentState();
        
        logger.info("------- ELECTION DEBUG ------- Starting election for term {} on node {}", currentTerm, nodeId);
        logger.info("ELECTION DEBUG: Current log state - lastLogIndex: {}, lastLogTerm: {}", log.getLastLogIndex(), log.getLastLogTerm());
        logger.info("ELECTION DEBUG: Number of peers: {}", peerNodeIds.size());
        
        // Reset the election timeout
        resetElectionTimeout();
        
        // Request votes from all peers
        RequestVoteRPC voteRequest = new RequestVoteRPC(
                nodeId,
                currentTerm,
                log.getLastLogIndex(),
                log.getLastLogTerm()
        );
        
        logger.info("ELECTION DEBUG: Sending vote requests to peers: {}", peerNodeIds);
        requestVoteCallback.accept(voteRequest);
        
        // Vote for ourselves
        int votesReceived = 1;
        logger.info("ELECTION DEBUG: Voted for self, current votes: 1");
        
        // If we're the only node, become leader immediately
        if (peerNodeIds.isEmpty()) {
            logger.info("ELECTION DEBUG: No peers detected, immediately becoming leader for term {}", currentTerm);
            becomeLeader();
        }
    }
    
    /**
     * Transition to leader state.
     */
    private void becomeLeader() {
        logger.info("Node {} becoming leader for term {}", nodeId, currentTerm);
        logger.info("------- ELECTION DEBUG ------- BECOMING LEADER for term {}", currentTerm);
        state = RaftState.LEADER;
        currentLeader = nodeId;
        
        // Initialize nextIndex and matchIndex for all followers
        nextIndex = new ConcurrentHashMap<>();
        matchIndex = new ConcurrentHashMap<>();
        
        for (String peerId : peerNodeIds) {
            // Initialize nextIndex to leader's last log index + 1
            nextIndex.put(peerId, log.getLastLogIndex() + 1);
            // Initialize matchIndex to 0
            matchIndex.put(peerId, 0);
            logger.info("ELECTION DEBUG: Initialized nextIndex for follower {} to {}", 
                    peerId, log.getLastLogIndex() + 1);
        }
        
        // Cancel election timeout
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
            electionTimeout = null;
        }
        
        // Send initial empty AppendEntries RPCs (heartbeats) to establish authority
        logger.info("ELECTION DEBUG: Sending initial heartbeats to establish leadership");
        sendHeartbeats();
        
        // Schedule periodic heartbeats
        // Reduced heartbeat interval from typical 100ms to 50ms for more aggressive replication
        int heartbeatIntervalMs = 50; 
        logger.info("Scheduling heartbeats with interval of {}ms", heartbeatIntervalMs);
        heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeats,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
    }
    
    /**
     * Reset the election timeout with a randomized value.
     */
    private void resetElectionTimeout() {
        if (electionTimeout != null) {
            electionTimeout.cancel(false);
        }
        
        // Increase base timeout and randomization to avoid split votes
        int baseTimeout = 300; // increased from likely lower value
        int randomOffset = random.nextInt(300); // wider range for better vote distribution
        int timeout = baseTimeout + randomOffset;
        
        logger.debug("Setting election timeout to {}ms for node {}", timeout, nodeId);
        
        electionTimeout = scheduler.schedule(
            this::startElection,
            timeout,
            TimeUnit.MILLISECONDS
        );
    }
    
    /**
     * Start an election when the election timeout expires.
     */
    private void startElection() {
        if (!running.get() || state == RaftState.LEADER) {
            return;
        }
        
        logger.info("Election timeout expired, starting election");
        logger.info("ELECTION DEBUG: Election timeout expired for node {}, current state: {}, current term: {}", 
                nodeId, state, currentTerm);
        becomeCandidate();
    }
    
    //
    // RPC handlers
    //
    
    /**
     * Handle a RequestVote RPC from a candidate.
     * 
     * @param rpc The RequestVote RPC
     */
    public void handleRequestVote(RequestVoteRPC rpc) {
        workerExecutor.execute(() -> {
            logger.debug("Received RequestVote from {} for term {}", rpc.getSenderId(), rpc.getTerm());
            
            boolean voteGranted = false;
            
            // If the term is greater than our current term, update it and become a follower
            if (rpc.getTerm() > currentTerm) {
                logger.info("Received RequestVote with higher term: {}", rpc.getTerm());
                logger.info("ELECTION DEBUG: Stepping down to follower due to higher term from {}", rpc.getSenderId());
                becomeFollower(rpc.getTerm());
            }
            
            // Decide whether to vote for the candidate
            if (rpc.getTerm() == currentTerm && 
                    (votedFor == null || votedFor.equals(rpc.getSenderId()))) {
                
                // Check if candidate's log is at least as up-to-date as ours
                int lastLogIndex = log.getLastLogIndex();
                int lastLogTerm = log.getLastLogTerm();
                
                logger.info("ELECTION DEBUG: Considering vote for {} (term: {})", rpc.getSenderId(), rpc.getTerm());
                logger.info("ELECTION DEBUG: My log - lastLogIndex: {}, lastLogTerm: {}", lastLogIndex, lastLogTerm);
                logger.info("ELECTION DEBUG: Candidate log - lastLogIndex: {}, lastLogTerm: {}", 
                        rpc.getLastLogIndex(), rpc.getLastLogTerm());
                
                if (rpc.getLastLogTerm() > lastLogTerm || 
                        (rpc.getLastLogTerm() == lastLogTerm && 
                         rpc.getLastLogIndex() >= lastLogIndex)) {
                    
                    votedFor = rpc.getSenderId();
                    voteGranted = true;
                    savePersistentState();
                    
                    // Reset election timeout since we voted
                    resetElectionTimeout();
                    
                    logger.info("ELECTION DEBUG: Vote GRANTED to {} in term {}", rpc.getSenderId(), currentTerm);
                } else {
                    logger.info("ELECTION DEBUG: Vote DENIED to {}, candidate log is not up-to-date", rpc.getSenderId());
                }
            } else if (rpc.getTerm() < currentTerm) {
                logger.info("ELECTION DEBUG: Vote DENIED to {}, term {} is outdated (my term: {})", 
                        rpc.getSenderId(), rpc.getTerm(), currentTerm);
            } else {
                logger.info("ELECTION DEBUG: Vote DENIED to {}, already voted for {} in term {}", 
                        rpc.getSenderId(), votedFor, currentTerm);
            }
            
            // Send response
            RequestVoteResponse response = new RequestVoteResponse(
                    nodeId,
                    currentTerm,
                    voteGranted
            );
            
            logger.info("ELECTION DEBUG: Sending vote response to {}: granted={}, term={}", 
                    rpc.getSenderId(), voteGranted, currentTerm);
            requestVoteResponseCallback.accept(response);
        });
    }
    
    /**
     * Handle a RequestVote response.
     * 
     * @param response The RequestVote response
     */
    public void handleRequestVoteResponse(RequestVoteResponse response) {
        workerExecutor.execute(() -> {
            logger.debug("Received RequestVote response from {} for term {}", 
                    response.getSenderId(), response.getTerm());
            
            // If we're not a candidate, ignore
            if (state != RaftState.CANDIDATE) {
                logger.debug("Ignoring vote from {} because we're not a candidate", 
                        response.getSenderId());
                return;
            }
            
            // If term is greater than ours, become a follower
            if (response.getTerm() > currentTerm) {
                logger.info("Received RequestVote response with higher term: {}", response.getTerm());
                logger.info("ELECTION DEBUG: Stepping down to follower due to higher term from {}", response.getSenderId());
                becomeFollower(response.getTerm());
                return;
            }
            
            // If the vote was granted and term matches our current term
            if (response.isVoteGranted() && response.getTerm() == currentTerm) {
                // In a real implementation, we would track votes received from each peer
                // Simulate vote counting by incrementing
                int votesReceived = countVotesReceived() + 1;
                
                int quorumSize = (peerNodeIds.size() + 1) / 2 + 1;
                logger.info("ELECTION DEBUG: Received vote from {}, votes received: {}, quorum size: {}", 
                        response.getSenderId(), votesReceived, quorumSize);
                
                // If we have a majority, become leader
                if (votesReceived >= quorumSize) {
                    logger.info("ELECTION DEBUG: Won election with {} votes for term {}", votesReceived, currentTerm);
                    becomeLeader();
                }
            } else {
                logger.info("ELECTION DEBUG: Vote denied by {} for term {}, vote granted: {}", 
                        response.getSenderId(), response.getTerm(), response.isVoteGranted());
            }
        });
    }
    
    /**
     * Count the number of votes received in the current election.
     * This would be replaced with actual vote tracking in a real implementation.
     * 
     * @return The number of votes received
     */
    private int countVotesReceived() {
        // In a real implementation, we would track votes received from each peer
        // For this simplified version, we just count ourselves
        return 1;
    }
    
    /**
     * Handle an AppendEntries RPC from the leader.
     * 
     * @param rpc The AppendEntries RPC
     */
    public void handleAppendEntries(AppendEntriesRPC rpc) {
        workerExecutor.execute(() -> {
            logger.debug("Received AppendEntries from {} for term {}", 
                    rpc.getSenderId(), rpc.getTerm());
            
            boolean success = false;
            int matchIndex = 0;
            
            // If the term is greater than our current term, update it and become a follower
            if (rpc.getTerm() > currentTerm) {
                logger.info("Received AppendEntries with higher term: {}", rpc.getTerm());
                becomeFollower(rpc.getTerm());
            }
            
            // If the term is valid (>= our term), process the request
            if (rpc.getTerm() >= currentTerm) {
                // Update current leader
                currentLeader = rpc.getLeaderId();
                
                // Reset election timeout since we heard from the leader
                resetElectionTimeout();
                
                // If we're a candidate, step down
                if (state == RaftState.CANDIDATE) {
                    becomeFollower(rpc.getTerm());
                }
                
                // Try to append the entries
                success = log.appendEntries(
                        rpc.getPrevLogIndex(),
                        rpc.getPrevLogTerm(),
                        rpc.getEntries(),
                        rpc.getLeaderCommit()
                );
                
                if (success) {
                    // If successful, update match index
                    matchIndex = rpc.getPrevLogIndex() + rpc.getEntries().size();
                    logger.debug("Successfully appended {} entries", rpc.getEntries().size());
                } else {
                    logger.debug("Failed to append entries, log consistency check failed");
                }
            } else {
                logger.info("Rejected AppendEntries from {} because term {} is outdated", 
                        rpc.getSenderId(), rpc.getTerm());
            }
            
            // Send response
            AppendEntriesResponse response = new AppendEntriesResponse(
                    nodeId,
                    currentTerm,
                    success,
                    matchIndex
            );
            
            appendEntriesResponseCallback.accept(response);
        });
    }
    
    /**
     * Handle an AppendEntries response.
     * 
     * @param response The AppendEntries response
     */
    public void handleAppendEntriesResponse(AppendEntriesResponse response) {
        workerExecutor.execute(() -> {
            logger.debug("Received AppendEntries response from {} for term {}", 
                    response.getSenderId(), response.getTerm());
            
            // If we're not the leader, ignore
            if (state != RaftState.LEADER) {
                return;
            }
            
            // If term is greater than ours, become a follower
            if (response.getTerm() > currentTerm) {
                logger.info("Received AppendEntries response with higher term: {}", response.getTerm());
                becomeFollower(response.getTerm());
                return;
            }
            
            // Process the response
            String peerId = response.getSenderId();
            
            if (response.isSuccess()) {
                // Update nextIndex and matchIndex for this follower
                int newMatchIndex = response.getMatchIndex();
                matchIndex.put(peerId, newMatchIndex);
                nextIndex.put(peerId, newMatchIndex + 1);
                
                logger.debug("Updated matchIndex for {} to {}", peerId, newMatchIndex);
                
                // Check if we can update commitIndex
                updateCommitIndex();
            } else {
                // If the append failed, decrement nextIndex and retry
                int currentNextIndex = nextIndex.getOrDefault(peerId, log.getLastLogIndex() + 1);
                nextIndex.put(peerId, Math.max(1, currentNextIndex - 1));
                
                logger.debug("AppendEntries failed for {}, decreasing nextIndex to {}", 
                        peerId, nextIndex.get(peerId));
                
                // Retry log replication for this follower
                replicateLogToFollower(peerId);
            }
        });
    }
    
    /**
     * Send heartbeats to all followers.
     * This is the same as AppendEntries with no entries.
     */
    private void sendHeartbeats() {
        if (!isActive() || state != RaftState.LEADER) {
            return;
        }
        
        logger.debug("Sending heartbeats to followers");
        
        // For each follower, send an AppendEntries RPC
        for (String peerId : peerNodeIds) {
            replicateLogToFollower(peerId);
        }
    }
    
    /**
     * Replicate the log to all followers.
     */
    private void replicateLogToFollowers() {
        if (!isActive() || state != RaftState.LEADER) {
            return;
        }
        
        logger.debug("Replicating log to followers");
        
        // For each follower, send an AppendEntries RPC
        for (String peerId : peerNodeIds) {
            replicateLogToFollower(peerId);
        }
    }
    
    /**
     * Replicate the log to a specific follower.
     * 
     * @param peerId The ID of the follower
     */
    private void replicateLogToFollower(String peerId) {
        if (!isActive() || state != RaftState.LEADER) {
            return;
        }
        
        int followerNextIndex = nextIndex.getOrDefault(peerId, log.getLastLogIndex() + 1);
        int prevLogIndex = followerNextIndex - 1;
        int prevLogTerm = 0;
        
        if (prevLogIndex > 0) {
            LogEntry prevEntry = log.getEntry(prevLogIndex);
            if (prevEntry != null) {
                prevLogTerm = prevEntry.getTerm();
            } else {
                logger.warn("Previous log entry not found for index {}", prevLogIndex);
                prevLogIndex = 0;
            }
        }
        
        // Get entries to send
        List<LogEntry> entries = log.getEntriesFrom(followerNextIndex);
        int maxEntries = RaftConfig.getInstance().getMaxEntriesPerAppend();
        if (entries.size() > maxEntries) {
            entries = entries.subList(0, maxEntries);
        }
        
        // Create AppendEntries RPC
        AppendEntriesRPC rpc = new AppendEntriesRPC(
                nodeId,
                currentTerm,
                nodeId,
                prevLogIndex,
                prevLogTerm,
                entries,
                log.getCommitIndex()
        );
        
        // Send RPC
        appendEntriesCallback.accept(rpc);
        
        logger.debug("Sent AppendEntries to {} with {} entries, prevLogIndex={}, prevLogTerm={}", 
                peerId, entries.size(), prevLogIndex, prevLogTerm);
    }
    
    /**
     * Update the commit index based on the match indices of followers.
     */
    private void updateCommitIndex() {
        if (state != RaftState.LEADER) {
            return;
        }
        
        int currentCommitIndex = log.getCommitIndex();
        
        // Find the highest index that is replicated on a majority of servers
        for (int n = log.getLastLogIndex(); n > currentCommitIndex; n--) {
            // Only commit entries from the current term (leader safety property)
            LogEntry entry = log.getEntry(n);
            if (entry != null && entry.getTerm() != currentTerm) {
                continue;
            }
            
            // Count replications (including ourselves)
            int replicationCount = 1;
            
            for (int matchIdx : matchIndex.values()) {
                if (matchIdx >= n) {
                    replicationCount++;
                }
            }
            
            // Check if we have a majority
            int quorumSize = (peerNodeIds.size() + 1) / 2 + 1;
            if (replicationCount >= quorumSize) {
                log.setCommitIndex(n);
                logger.debug("Updated commit index to {}", n);
                break;
            }
        }
    }
    
    //
    // Persistence
    //
    
    /**
     * Save persistent Raft state to disk.
     */
    private void savePersistentState() {
        persistenceLock.writeLock().lock();
        try {
            Path stateFile = persistencePath.resolve("state.dat");
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(stateFile))) {
                out.writeInt(currentTerm);
                out.writeObject(votedFor);
                logger.debug("Saved persistent state: term={}, votedFor={}", currentTerm, votedFor);
            } catch (IOException e) {
                logger.error("Failed to save persistent state: {}", e.getMessage());
            }
        } finally {
            persistenceLock.writeLock().unlock();
        }
    }
    
    /**
     * Load persistent Raft state from disk.
     */
    private void loadPersistentState() {
        persistenceLock.readLock().lock();
        try {
            Path stateFile = persistencePath.resolve("state.dat");
            if (!Files.exists(stateFile)) {
                logger.info("No persistent state found, using defaults");
                return;
            }
            
            try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(stateFile))) {
                currentTerm = in.readInt();
                votedFor = (String) in.readObject();
                logger.info("Loaded persistent state: term={}, votedFor={}", currentTerm, votedFor);
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Failed to load persistent state: {}", e.getMessage());
            }
        } finally {
            persistenceLock.readLock().unlock();
        }
    }
    
    /**
     * Get the current Raft state.
     * 
     * @return The current state
     */
    public RaftState getState() {
        return state;
    }
    
    /**
     * Get the current term.
     * 
     * @return The current term
     */
    public int getCurrentTerm() {
        return currentTerm;
    }
    
    /**
     * Get the ID of the node we voted for in the current term.
     * 
     * @return The node ID, or null if we haven't voted
     */
    public String getVotedFor() {
        return votedFor;
    }
    
    /**
     * Get the ID of the current leader.
     * 
     * @return The leader ID, or null if no leader is known
     */
    public String getCurrentLeader() {
        return currentLeader;
    }
    
    /**
     * Check if this node is the leader.
     * 
     * @return True if this node is the leader
     */
    public boolean isLeader() {
        return state == RaftState.LEADER;
    }
    
    /**
     * Get the commit index.
     * 
     * @return The current commit index
     */
    public int getCommitIndex() {
        return log.getCommitIndex();
    }
    
    /**
     * Get the last applied index.
     * 
     * @return The last applied index
     */
    public int getLastApplied() {
        return log.getLastApplied();
    }
} 