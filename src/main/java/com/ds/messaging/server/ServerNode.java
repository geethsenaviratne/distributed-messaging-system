package com.ds.messaging.server;

import com.ds.messaging.common.*;
import com.ds.messaging.consensus.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Server implementation of a node in the distributed messaging system.
 * Handles multiple client connections and message routing.
 * Implements primary-backup replication with strong consistency.
 * Can optionally use Raft consensus algorithm for message ordering.
 */
public class ServerNode extends AbstractNode {
    private static final Logger logger = LoggerFactory.getLogger(ServerNode.class);
    
    // Configuration flag for enabling Raft consensus
    private static final boolean RAFT_ENABLED = Boolean.getBoolean("raft.enabled");
    
    private final int port;
    private ServerSocket serverSocket;
    private final Map<String, ClientConnection> clientConnections;
    private final Map<String, ServerConnection> serverConnections;
    private final ExecutorService connectionExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private final MessageStore messageStore;
    private final Set<UUID> processedMessages;
    
    private final List<String> peerServerAddresses;
    private boolean running;
    
    // Sequence counter for this server
    private final AtomicLong localSequenceCounter = new AtomicLong(0);
    
    // Reference to the replication manager (for primary-backup mode)
    private final ReplicationManager replicationManager;
    
    // Reference to the Raft server (for Raft consensus mode)
    private RaftServer raftServer;
    
    // Pending operations waiting for acknowledgments
    private final Map<UUID, CompletableFuture<Boolean>> pendingOperations = new ConcurrentHashMap<>();
    
    // Timeout for operations (ms)
    private final long operationTimeoutMs = 15000;
    
    // Last sequence number known for each server
    private final Map<String, Long> serverSequences = new ConcurrentHashMap<>();
    
    // Queue for messages that couldn't be replicated due to no backup servers available
    private final Queue<Message> pendingReplicationQueue = new ConcurrentLinkedQueue<>();
    
    // Track which messages have been delivered to which clients (clientId -> Set<messageId>)
    private final Map<String, Set<UUID>> deliveredMessages = new ConcurrentHashMap<>();
    
    /**
     * Creates a new server node.
     * 
     * @param id The ID for this server
     * @param port The port to listen on
     * @param peerServers List of peer server addresses in the format "host:port"
     */
    public ServerNode(String id, int port, List<String> peerServers) {
        super(id);
        this.port = port;
        this.peerServerAddresses = new ArrayList<>(peerServers);
        this.clientConnections = new ConcurrentHashMap<>();
        this.serverConnections = new ConcurrentHashMap<>();
        
        // Enhanced thread pool configuration
        ThreadFactory connectionThreadFactory = new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
            private final AtomicLong count = new AtomicLong(0);
            
            @Override
            public Thread newThread(Runnable r) {
                Thread t = defaultFactory.newThread(r);
                t.setName("server-connection-" + count.incrementAndGet());
                return t;
            }
        };
        
        ThreadFactory schedulerThreadFactory = new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
            private final AtomicLong count = new AtomicLong(0);
            
            @Override
            public Thread newThread(Runnable r) {
                Thread t = defaultFactory.newThread(r);
                t.setName("server-scheduler-" + count.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        
        // Create robust connection executor with bounded queue and caller runs policy
        this.connectionExecutor = new ThreadPoolExecutor(
            8, 32, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(500),
            connectionThreadFactory,
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        // Create robust scheduled executor
        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(
            4, schedulerThreadFactory, new ThreadPoolExecutor.CallerRunsPolicy()
        );
        scheduledExecutor.setRemoveOnCancelPolicy(true);
        scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduledExecutor = scheduledExecutor;
        
        this.messageStore = new InMemoryMessageStore();
        this.processedMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.running = false;
        this.replicationManager = ReplicationManager.getInstance();
        
        // Log whether Raft consensus is enabled
        if (RAFT_ENABLED) {
            logger.info("Raft consensus algorithm is ENABLED");
        } else {
            logger.info("Raft consensus algorithm is DISABLED, using primary-backup replication");
        }
    }
    
    @Override
    public void start() throws IOException {
        super.start();
        
        // Start the server socket
        serverSocket = new ServerSocket(port);
        running = true;
        
        // Connect to peer servers first so we have connections for Raft
        connectToPeerServers();
        
        // Initialize and start Raft if enabled
        if (RAFT_ENABLED) {
            initializeRaft();
        } else {
            // Register with the replication manager (primary-backup mode)
            replicationManager.registerServer(id);
        }
        
        // Setup time synchronization
        setupTimeSync();
        
        // Start accepting client connections
        connectionExecutor.submit(this::acceptConnections);
        
        // Start heartbeat mechanism
        startHeartbeat();
        
        // Start sync mechanism for message recovery
        startSyncMechanism();
        
        // Register message handlers
        registerMessageHandlers();
        
        status = NodeStatus.RUNNING;
        
        if (RAFT_ENABLED) {
            logger.info("Server node started on port {} with ID {}, using Raft consensus", port, id);
        } else {
            logger.info("Server node started on port {} with ID {}, primary server is: {}", 
                    port, id, replicationManager.getPrimaryServer());
        }
    }
    
    /**
     * Initialize and start the Raft consensus server.
     */
    private void initializeRaft() {
        // Extract peer server IDs from the addresses
        Set<String> peerServerIds = new HashSet<>();
        for (String peerAddress : peerServerAddresses) {
            String peerId = extractServerIdFromAddress(peerAddress);
            if (peerId != null) {
                peerServerIds.add(peerId);
            }
        }
        
        // Create and start the Raft server
        raftServer = new RaftServer(
                id,
                peerServerIds,
                messageStore,
                this::sendMessage
        );
        
        raftServer.start();
        
        logger.info("Raft consensus initialized with peers: {}", peerServerIds);
    }
    
    /**
     * Extract a server ID from an address string.
     * 
     * @param address Address in format "host:port" or "serverN:host:port"
     * @return The server ID
     */
    private String extractServerIdFromAddress(String address) {
        // For simplicity, we assume server ID is serverN where N is the port number
        String[] parts = address.split(":");
        if (parts.length >= 2) {
            return "server" + parts[parts.length - 1];
        }
        return null;
    }
    
    @Override
    public void stop() {
        running = false;
        
        // Stop Raft if enabled
        if (RAFT_ENABLED && raftServer != null) {
            raftServer.stop();
        }
        
        // Close peer server connections
        for (ServerConnection connection : serverConnections.values()) {
            connection.close();
        }
        serverConnections.clear();
        
        // Close client connections
        for (ClientConnection connection : clientConnections.values()) {
            connection.close();
        }
        clientConnections.clear();
        
        // Close server socket
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.error("Error closing server socket: {}", e.getMessage(), e);
        }
        
        // Shutdown executors gracefully
        try {
            // First attempt orderly shutdown
            logger.info("Shutting down thread pools gracefully");
            scheduledExecutor.shutdown();
            connectionExecutor.shutdown();
            
            // Wait for tasks to complete
            boolean schedulerTerminated = scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);
            boolean workerTerminated = connectionExecutor.awaitTermination(5, TimeUnit.SECONDS);
            
            // If orderly shutdown fails, force shutdown
            if (!schedulerTerminated) {
                logger.warn("Scheduler did not terminate in time, forcing shutdown");
                List<Runnable> droppedTasks = scheduledExecutor.shutdownNow();
                logger.warn("Dropped {} tasks from scheduler", droppedTasks.size());
            }
            
            if (!workerTerminated) {
                logger.warn("Connection executor did not terminate in time, forcing shutdown");
                List<Runnable> droppedTasks = connectionExecutor.shutdownNow();
                logger.warn("Dropped {} tasks from connection executor", droppedTasks.size());
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for thread pools to terminate", e);
            Thread.currentThread().interrupt();
            
            // Force shutdown if interrupted
            scheduledExecutor.shutdownNow();
            connectionExecutor.shutdownNow();
        }
        
        super.stop();
        logger.info("Server node stopped");
    }
    
    /**
     * Simulate a degraded server mode for testing fault tolerance
     */
    public void simulateDegradedMode() {
        status = NodeStatus.DEGRADED;
        logger.info("Server node entering degraded mode");
        System.out.println("Server is now in DEGRADED mode");
    }
    
    /**
     * Restore the server to a specific mode after simulation
     * 
     * @param originalStatus The status to restore to
     */
    public void restoreMode(NodeStatus originalStatus) {
        status = originalStatus;
        logger.info("Server node restored to {} mode", originalStatus);
        System.out.println("Server has been restored to " + originalStatus + " mode");
    }
    
    @Override
    public boolean sendMessage(Message message) {
        if (status != NodeStatus.RUNNING && status != NodeStatus.DEGRADED) {
            logger.warn("Cannot send message when server is not running or degraded");
            return false;
        }
        
        // If in degraded mode, there's a chance message will not be sent
        if (status == NodeStatus.DEGRADED && Math.random() < 0.3) {
            logger.warn("Message not sent due to degraded mode: {}", message.getId());
            return false;
        }
        
        // Check if we've already processed this message to avoid duplicates
        if (processedMessages.contains(message.getId())) {
            logger.debug("Message already processed, not sending again: {}", message.getId());
            return true;
        }
        
        // Mark message as processed
        processedMessages.add(message.getId());
        
        // For user messages requiring strong consistency
        if (message.getType() == MessageType.USER_MESSAGE) {
            // If Raft is enabled, use Raft consensus for user messages
            if (RAFT_ENABLED) {
                return handleUserMessageWithRaft(message);
            } else {
                // Otherwise, use primary-backup replication
                return handleUserMessageWithPrimaryBackup(message);
            }
        }
        
        // For other message types, deliver directly
        return deliverMessage(message);
    }
    
    /**
     * Handle a user message using Raft consensus.
     * 
     * @param message The message to handle
     * @return true if message is accepted or properly forwarded
     */
    private boolean handleUserMessageWithRaft(Message message) {
        if (raftServer == null) {
            logger.error("Raft server is null but Raft mode is enabled");
            return false;
        }
        
        // Track the message with a completable future
        CompletableFuture<Boolean> future = raftServer.submitMessage(message);
        pendingOperations.put(message.getId(), future);
        
        // Add exponential backoff retries to improve reliability
        int maxRetries = 3;
        long initialBackoffMs = 100;
        
        // Set up retry logic with exponential backoff
        future.exceptionally(ex -> {
            // If we're not the leader, try to forward or retry
            if (ex instanceof IllegalStateException && 
                ex.getMessage().contains("Not the leader")) {
                
                logger.info("Not the leader for message {}, initiating retry logic", message.getId());
                
                String currentLeader = raftServer.getCurrentLeader();
                if (currentLeader != null && !currentLeader.equals(id)) {
                    // Forward to current leader if known
                    forwardMessageToLeader(message, currentLeader);
                    return true;
                } else {
                    // No known leader, use retry with backoff
                    scheduleRetryWithBackoff(message, maxRetries, initialBackoffMs, 0);
                }
            }
            return false;
        });
        
        return true;
    }
    
    /**
     * Forward a message to the current Raft leader
     */
    private void forwardMessageToLeader(Message message, String leaderId) {
        logger.info("Forwarding message {} to leader {}", message.getId(), leaderId);
        
        // Check if we have a connection to the leader
        ServerConnection leaderConnection = serverConnections.get(leaderId);
        if (leaderConnection != null && leaderConnection.isConnected()) {
            // Create a forwarded message
            Message forwardedMessage = new Message(
                id,
                leaderId,
                "FORWARD:" + message.getId(),
                MessageType.FORWARD
            );
            forwardedMessage.setPayload(message);
            
            leaderConnection.sendMessage(forwardedMessage);
            logger.debug("Message {} forwarded to leader {}", message.getId(), leaderId);
        } else {
            logger.warn("No connection to leader {}, cannot forward message {}", 
                    leaderId, message.getId());
        }
    }
    
    /**
     * Schedule a retry with exponential backoff
     */
    private void scheduleRetryWithBackoff(Message message, int maxRetries, long backoffMs, int attempt) {
        if (attempt >= maxRetries) {
            logger.warn("Max retries ({}) reached for message {}", maxRetries, message.getId());
            CompletableFuture<Boolean> future = pendingOperations.remove(message.getId());
            if (future != null && !future.isDone()) {
                future.complete(false);
            }
            return;
        }
        
        // Schedule retry with exponential backoff
        long delay = backoffMs * (1L << attempt); // Exponential increase
        
        logger.debug("Scheduling retry {} for message {} with delay {}ms", 
                attempt + 1, message.getId(), delay);
        
        scheduledExecutor.schedule(() -> {
            if (!running) return;
            
            if (raftServer.isLeader()) {
                logger.info("Retry {}: We are now the leader, resubmitting message {}", 
                        attempt + 1, message.getId());
                handleUserMessageWithRaft(message);
            } else {
                String currentLeader = raftServer.getCurrentLeader();
                if (currentLeader != null && !currentLeader.equals(id)) {
                    // Try to forward to leader
                    forwardMessageToLeader(message, currentLeader);
                } else {
                    // Still no leader, retry again
                    scheduleRetryWithBackoff(message, maxRetries, backoffMs, attempt + 1);
                }
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Handle a user message using primary-backup replication.
     * 
     * @param message The message to handle
     * @return true if the message was accepted, false otherwise
     */
    private boolean handleUserMessageWithPrimaryBackup(Message message) {
        // Determine if we are the primary server
        boolean isPrimary = replicationManager.isPrimary(id);
        
        // Add server origin info if not set
        if (message.getOriginServerId() == null) {
            message.setOriginServerId(id);
        }
        
        // Assign a sequence number if not set
        if (message.getSequenceNumber() < 0) {
            long seq = localSequenceCounter.incrementAndGet();
            message.setSequenceNumber(seq);
        }
        
        // Log user message routing details
        logger.info("ROUTING USER MESSAGE: From '{}' to '{}' with content: '{}'", 
                message.getSenderId(), message.getRecipientId(), message.getContent());
        
        // Store message regardless of primary status
        messageStore.storeMessage(message);
        
        // If we're primary, we coordinate replication
        if (isPrimary) {
            // Create a tracker for acknowledgments
            CompletableFuture<Boolean> replicationFuture = new CompletableFuture<>();
            pendingOperations.put(message.getId(), replicationFuture);
            
            // Check if we have backup servers to replicate to
            boolean hasBackups = replicationManager.createAckTracker(message.getId());
            
            if (!hasBackups) {
                // No backup servers, add to pending queue and report failure
                logger.warn("Message {} could not be replicated. No backup servers available. Marking for retry once backup servers are available.", 
                        message.getId());
                pendingReplicationQueue.add(message);
                
                // Create an empty placeholder tracker for this message so that when acks come later
                // (e.g., after sync), the system has a reference for them
                replicationManager.createEmptyAckTracker(message.getId());
                
                // Complete the future with failure
                replicationFuture.complete(false);
                return false;
            }
            
            // Replicate to backup servers
            replicateToBackups(message);
            
            // Wait for replication to complete with timeout
            try {
                boolean success = replicationFuture.get(operationTimeoutMs, TimeUnit.MILLISECONDS);
                if (success) {
                    logger.info("Message {} successfully replicated", message.getId());
                    
                    // Deliver the message locally now that it's replicated
                    return deliverMessageToClient(message);
                } else {
                    logger.warn("Failed to replicate message {}. Message will be retried when backup servers are available.", message.getId());
                    
                    // We still store the message locally but don't claim success for replication
                    // We'll deliver it locally once it's properly replicated
                    return false;
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Error waiting for replication of message {}: {}", 
                        message.getId(), e.getMessage());
                pendingOperations.remove(message.getId());
                return false;
            }
            
        } else {
            // If we're not the primary, just acknowledge receipt
            // The primary will coordinate consistency
            return true;
        }
    }
    
    /**
     * Replicates a message to all backup servers with improved error handling.
     * 
     * @param message The message to replicate
     */
    private void replicateToBackups(Message message) {
        // Get replication targets excluding ourselves
        Set<String> targets = replicationManager.getReplicationTargets();
        targets.remove(id); // Remove self
        
        if (targets.isEmpty()) {
            logger.warn("Message {} could not be replicated. No backup servers available. Marking for retry once backup servers are available.", message.getId());
            // Add the message to the pending replication queue instead of completing with success
            pendingReplicationQueue.add(message);
            
            // Complete the operation with false to indicate that replication didn't succeed
            CompletableFuture<Boolean> future = pendingOperations.get(message.getId());
            if (future != null) {
                future.complete(false);
            }
            return;
        }
        
        logger.info("Replicating message {} to {} backup servers: {}", 
                message.getId(), targets.size(), targets);
        
        int successCount = 0;
        for (String serverId : targets) {
            ServerConnection connection = serverConnections.get(serverId);
            if (connection != null && connection.isConnected()) {
                try {
                    boolean sent = connection.sendMessage(message);
                    if (sent) {
                        successCount++;
                        logger.info("Successfully replicated message {} to backup server {}", 
                                message.getId(), serverId);
                    } else {
                        logger.warn("Failed to replicate message {} to backup server {}", 
                                message.getId(), serverId);
                    }
                } catch (Exception e) {
                    logger.error("Error replicating message {} to backup server {}: {}", 
                            message.getId(), serverId, e.getMessage(), e);
                    
                    // Mark connection as potentially broken
                    connection.markPotentiallyBroken();
                }
            } else {
                logger.warn("Cannot find active connection to backup server {} for message {}", 
                        serverId, message.getId());
                
                // Try to reconnect to this server if it was previously connected
                if (peerServerAddresses.stream().anyMatch(addr -> addr.contains(serverId))) {
                    scheduledExecutor.schedule(() -> {
                        logger.info("Attempting to reconnect to backup server: {}", serverId);
                        connectToPeerServer(serverId);
                    }, 5, TimeUnit.SECONDS);
                }
            }
        }
        
        // If no messages were successfully sent but we have targets, it's an error
        if (successCount == 0 && !targets.isEmpty()) {
            logger.error("Failed to replicate message {} to any backup server", message.getId());
            // We still complete the future to avoid blocking the client
            CompletableFuture<Boolean> future = pendingOperations.get(message.getId());
            if (future != null) {
                future.complete(false);
            }
        } else if (successCount < targets.size()) {
            logger.warn("Partially replicated message {} to {}/{} backup servers", 
                    message.getId(), successCount, targets.size());
        }
    }
    
    /**
     * Handles an acknowledgment message from a backup server with improved error handling.
     * 
     * @param ackMessage The acknowledgment message
     */
    private void handleAcknowledgment(Message ackMessage) {
        if (ackMessage == null || ackMessage.getAcknowledgedMessageId() == null) {
            logger.warn("Received invalid acknowledgment message");
            return;
        }
        
        UUID acknowledgedId = ackMessage.getAcknowledgedMessageId();
        String serverId = ackMessage.getSenderId();
        
        if (acknowledgedId == null || serverId == null) {
            logger.warn("Acknowledgment missing critical information: id={}, sender={}", 
                    acknowledgedId, serverId);
            return;
        }
        
        logger.info("Received acknowledgment from {} for message {}", serverId, acknowledgedId);
        
        try {
            // Record the acknowledgment
            boolean allAcknowledged = replicationManager.acknowledgeMessage(acknowledgedId, serverId);
            
            // Complete the pending operation if all acknowledgments received
            if (allAcknowledged) {
                CompletableFuture<Boolean> future = pendingOperations.get(acknowledgedId);
                if (future != null && !future.isDone()) {
                    future.complete(true);
                    logger.info("All servers acknowledged message {}, operation complete", acknowledgedId);
                } else if (future == null) {
                    logger.warn("No pending operation found for acknowledged message {}", acknowledgedId);
                }
            } else {
                logger.debug("Waiting for more acknowledgments for message {}", acknowledgedId);
            }
        } catch (Exception e) {
            logger.error("Error processing acknowledgment for message {}: {}", 
                    acknowledgedId, e.getMessage(), e);
            
            // Prevent operation from hanging by completing it with a timeout
            CompletableFuture<Boolean> future = pendingOperations.get(acknowledgedId);
            if (future != null && !future.isDone()) {
                // Complete with false to indicate failure, but allow operation to continue
                future.complete(false);
                logger.warn("Completed operation for message {} with failure due to error", acknowledgedId);
            }
        }
    }
    
    /**
     * Sends an acknowledgment for a received message with improved error handling.
     * 
     * @param message The message to acknowledge
     */
    private void sendAcknowledgment(Message message) {
        if (message == null || message.getOriginServerId() == null) {
            logger.warn("Cannot acknowledge message: null or missing origin server");
            return;
        }
        
        // Don't send acknowledgments to ourselves
        if (message.getOriginServerId().equals(id)) {
            logger.debug("Not sending acknowledgment for our own message: {}", message.getId());
            return;
        }
        
        try {
            final Message ack = Message.createAcknowledgment(message, id);
            final String originServerId = message.getOriginServerId();
            final UUID messageId = message.getId();
            
            // Get connection to origin server
            ServerConnection originConnection = serverConnections.get(originServerId);
            if (originConnection != null && originConnection.isConnected()) {
                boolean sent = originConnection.sendMessage(ack);
                if (sent) {
                    logger.info("Sent acknowledgment to {} for message {}", 
                            originServerId, messageId);
                } else {
                    logger.warn("Failed to send acknowledgment to {} for message {}", 
                            originServerId, messageId);
                    
                    // Queue for retry
                    scheduledExecutor.schedule(() -> sendAcknowledgment(message), 
                            2, TimeUnit.SECONDS);
                }
            } else {
                logger.warn("Cannot find connection to origin server {}, queuing acknowledgment", 
                        originServerId);
                
                // Store acknowledgment for later delivery when connection is established
                scheduledExecutor.schedule(() -> {
                    ServerConnection reconnectedServer = serverConnections.get(originServerId);
                    if (reconnectedServer != null && reconnectedServer.isConnected()) {
                        reconnectedServer.sendMessage(ack);
                        logger.info("Sent delayed acknowledgment to {} for message {}", 
                                originServerId, messageId);
                    } else {
                        logger.warn("Failed to send delayed acknowledgment, server {} still unavailable", 
                                originServerId);
                    }
                }, 5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            logger.error("Error creating or sending acknowledgment for message {}: {}", 
                    message.getId(), e.getMessage(), e);
        }
    }
    
    @Override
    public List<String> getConnectedNodes() {
        List<String> connectedNodes = new ArrayList<>();
        connectedNodes.addAll(clientConnections.keySet());
        connectedNodes.addAll(serverConnections.keySet());
        return connectedNodes;
    }
    
    private void acceptConnections() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                connectionExecutor.submit(() -> handleNewConnection(clientSocket));
            } catch (IOException e) {
                if (running) {
                    logger.error("Error accepting connection: {}", e.getMessage(), e);
                }
            }
        }
    }
    
    private void handleNewConnection(Socket socket) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            
            // Read the connection type (CLIENT or SERVER)
            String connectionType = (String) in.readObject();
            
            // Read the node ID
            String nodeId = (String) in.readObject();
            
            if ("CLIENT".equals(connectionType)) {
                ClientConnection clientConnection = new ClientConnection(nodeId, socket, in, out);
                clientConnections.put(nodeId, clientConnection);
                logger.info("Accepted client connection from {}", nodeId);
                
                // Check for pending messages for this client
                scheduledExecutor.submit(() -> {
                    checkPendingMessagesForClient(nodeId, clientConnection);
                });
                
                // Start reading messages from the client
                connectionExecutor.submit(() -> readMessagesFromClient(clientConnection));
            } else if ("SERVER".equals(connectionType)) {
                ServerConnection serverConnection = new ServerConnection(nodeId, socket, in, out);
                serverConnections.put(nodeId, serverConnection);
                logger.info("Accepted server connection from {}", nodeId);
                
                // Start reading messages from the server
                connectionExecutor.submit(() -> readMessagesFromServer(serverConnection));
            } else {
                logger.warn("Unknown connection type: {}", connectionType);
                socket.close();
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error handling new connection: {}", e.getMessage(), e);
            try {
                socket.close();
            } catch (IOException ex) {
                // Ignore
            }
        }
    }
    
    /**
     * Check if there are any pending messages for a client that just connected
     * and attempt to deliver them.
     * 
     * @param clientId The ID of the client
     * @param clientConnection The connection to the client
     */
    private void checkPendingMessagesForClient(String clientId, ClientConnection clientConnection) {
        try {
            logger.info("Checking pending messages for client: {}", clientId);
            
            // Get all messages for this recipient from the message store
            List<Message> pendingMessages = ((InMemoryMessageStore)messageStore).getMessagesForRecipient(clientId);
            
            // Get or create the set of delivered message IDs for this client
            Set<UUID> delivered = deliveredMessages.computeIfAbsent(clientId, 
                    k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
            
            // Filter out already delivered messages
            List<Message> undeliveredMessages = pendingMessages.stream()
                    .filter(msg -> !delivered.contains(msg.getId()))
                    .collect(Collectors.toList());
            
            if (!undeliveredMessages.isEmpty()) {
                logger.info("Found {} undelivered messages for client {} (filtered from {} total messages)", 
                        undeliveredMessages.size(), clientId, pendingMessages.size());
                
                int deliveredCount = 0;
                for (Message message : undeliveredMessages) {
                    boolean success = clientConnection.sendMessage(message);
                    if (success) {
                        // Mark as delivered
                        delivered.add(message.getId());
                        deliveredCount++;
                    }
                }
                
                logger.info("Delivered {}/{} undelivered messages to client {}", 
                        deliveredCount, undeliveredMessages.size(), clientId);
            } else {
                logger.info("No new messages for client {} ({} messages already delivered)", 
                        clientId, delivered.size());
            }
        } catch (Exception e) {
            logger.error("Error checking pending messages for client {}: {}", clientId, e.getMessage(), e);
        }
    }
    
    private void connectToPeerServers() {
        for (String peerServer : peerServerAddresses) {
            connectionExecutor.submit(() -> connectToPeerServer(peerServer));
        }
    }
    
    private void readMessagesFromClient(ClientConnection connection) {
        try {
            while (running && connection.isConnected()) {
                try {
                    Message message = (Message) connection.getInputStream().readObject();
                    
                    if (message.getType() == MessageType.USER_MESSAGE) {
                        logger.info("Received USER MESSAGE from client '{}' to '{}': '{}'", 
                                connection.getNodeId(), message.getRecipientId(), message.getContent());
                    } else {
                        logger.debug("Received message from client {}: {}", connection.getNodeId(), message);
                    }
                    
                    // Set origin server if not already set
                    if (message.getOriginServerId() == null) {
                        message.setOriginServerId(id);
                    }
                    
                    // Process the message
                    boolean handled = processMessage(message, connection.getNodeId());
                    
                    // Forward the message if needed
                    if (!handled && message.getType() == MessageType.USER_MESSAGE) {
                        logger.info("Message not handled locally, forwarding to network");
                        sendMessage(message);
                    }
                } catch (ClassNotFoundException e) {
                    logger.error("Error deserializing message: {}", e.getMessage(), e);
                } catch (IOException e) {
                    logger.error("Error reading from client: {}", e.getMessage(), e);
                    break;
                }
            }
        } finally {
            connection.close();
            clientConnections.remove(connection.getNodeId());
            logger.info("Client disconnected: {}", connection.getNodeId());
        }
    }
    
    private void readMessagesFromServer(ServerConnection connection) {
        try {
            while (running && connection.isConnected()) {
                try {
                    Message message = (Message) connection.getInputStream().readObject();
                    
                    if (message.getType() == MessageType.USER_MESSAGE) {
                        logger.info("Received USER MESSAGE from server '{}': From '{}' to '{}': '{}'", 
                                connection.getNodeId(), message.getSenderId(), 
                                message.getRecipientId(), message.getContent());
                        
                        // Update our knowledge of this server's sequence
                        if (message.getSequenceNumber() > 0 && message.getOriginServerId() != null) {
                            serverSequences.put(message.getOriginServerId(), message.getSequenceNumber());
                        }
                        
                        // Send acknowledgment for user messages
                        sendAcknowledgment(message);
                    } else if (message.getType() == MessageType.ACK) {
                        logger.info("Received ACK from server '{}' for message: {}", 
                                connection.getNodeId(), message.getAcknowledgedMessageId());
                        
                        // Handle acknowledgment
                        if (message.isAcknowledgment() && message.getAcknowledgedMessageId() != null) {
                            handleAcknowledgment(message);
                        }
                    } else if (message.getType() == MessageType.SYNC_DATA) {
                        logger.info("Received SYNC_DATA from {}", connection.getNodeId());
                        // Handle SYNC_DATA message
                        // Extract the expected message count
                        int expectedMessages = 0;
                        try {
                            String content = message.getContent();
                            if (content.startsWith("SYNC_DATA:")) {
                                expectedMessages = Integer.parseInt(content.substring("SYNC_DATA:".length()));
                                logger.info("Expecting {} messages in SYNC_DATA from {}", expectedMessages, connection.getNodeId());
                            }
                        } catch (NumberFormatException e) {
                            logger.error("Invalid message count in SYNC_DATA: {}", message.getContent());
                        }
                        
                        // We'll handle the actual messages in the readMessagesFromServer method
                        // Just acknowledge receipt here
                        return;
                    } else {
                        logger.debug("Received message from server {}: {}", connection.getNodeId(), message);
                    }
                    
                    // Check if we've already processed this message to avoid duplicates
                    if (processedMessages.contains(message.getId())) {
                        logger.debug("Message already processed, ignoring: {}", message.getId());
                        continue;
                    }
                    
                    // Mark message as processed
                    processedMessages.add(message.getId());
                    
                    // Store the message for fault tolerance
                    messageStore.storeMessage(message);
                    
                    // Special handling for USER_MESSAGE type
                    if (message.getType() == MessageType.USER_MESSAGE) {
                        // Try to deliver to a local client first
                        String recipientId = message.getRecipientId();
                        
                        // Try to deliver using our central delivery method which handles delivery tracking
                        boolean delivered = deliverMessageToClient(message);
                        
                        if (delivered) {
                            logger.info("Successfully delivered message to client: {}", recipientId);
                        } else {
                            // Client is not connected to this server - log this clearly
                            logger.info("Client {} not connected to this server, message will be stored", 
                                    recipientId);
                        }
                    }
                    
                    // Process the message with handlers
                    boolean handled = processMessage(message, connection.getNodeId());
                    
                    // No need to forward messages - they're already being replicated by the primary
                    
                } catch (ClassNotFoundException e) {
                    logger.error("Error deserializing message: {}", e.getMessage(), e);
                } catch (IOException e) {
                    logger.error("Error reading from server: {}", e.getMessage(), e);
                    break;
                }
            }
        } finally {
            connection.close();
            serverConnections.remove(connection.getNodeId());
            logger.info("Server disconnected: {}", connection.getNodeId());
            
            // Unregister from replication manager
            replicationManager.unregisterServer(connection.getNodeId());
            
            // Try to reconnect to the server
            if (running) {
                scheduledExecutor.schedule(() -> {
                    for (String peerServer : peerServerAddresses) {
                        if (peerServer.equals(connection.getNodeId()) && !serverConnections.containsKey(peerServer)) {
                            logger.info("Attempting to reconnect to server: {}", peerServer);
                            connectToPeerServers();
                            break;
                        }
                    }
                }, 5, TimeUnit.SECONDS);
            }
        }
    }
    
    private void startHeartbeat() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            if (status == NodeStatus.RUNNING || status == NodeStatus.DEGRADED) {
                Message heartbeat = new Message(id, "all", "Heartbeat", MessageType.HEARTBEAT);
                
                // Send heartbeat to all servers
                for (ServerConnection serverConnection : serverConnections.values()) {
                    try {
                        serverConnection.sendMessage(heartbeat);
                    } catch (Exception e) {
                        logger.warn("Failed to send heartbeat to server {}: {}", 
                                serverConnection.getNodeId(), e.getMessage());
                    }
                }
            }
        }, 5, 5, TimeUnit.SECONDS);
        
        // Clean up processed message IDs periodically to prevent memory leaks
        scheduledExecutor.scheduleAtFixedRate(() -> {
            // In a real implementation, we would use a TTL-based approach or LRU cache
            if (processedMessages.size() > 10000) {
                logger.info("Clearing processed message cache");
                processedMessages.clear();
            }
            
            // Limit the number of delivered messages we track per client to prevent memory issues
            final int MAX_DELIVERED_MESSAGES_PER_CLIENT = 1000;
            final int MAX_INACTIVE_TIME_HOURS = 24; // Only keep tracking for clients active within 24h
            
            // Get the current time
            long now = System.currentTimeMillis();
            long cutoffTime = now - TimeUnit.HOURS.toMillis(MAX_INACTIVE_TIME_HOURS);
            
            // Track clients to remove entirely
            Set<String> clientsToRemove = new HashSet<>();
            
            // Check each client's delivered messages
            for (Map.Entry<String, Set<UUID>> entry : deliveredMessages.entrySet()) {
                String clientId = entry.getKey();
                Set<UUID> messages = entry.getValue();
                
                // Check if client has been active recently
                ClientConnection clientConnection = clientConnections.get(clientId);
                if (clientConnection == null) {
                    // If client isn't connected and we have too many messages, trim
                    if (messages.size() > MAX_DELIVERED_MESSAGES_PER_CLIENT) {
                        logger.info("Trimming delivered message history for disconnected client {}: {} -> {}",
                                clientId, messages.size(), MAX_DELIVERED_MESSAGES_PER_CLIENT);
                        
                        // Remove oldest messages - this is a simple approach
                        // In a real implementation, we would track timestamps per message
                        List<UUID> messageList = new ArrayList<>(messages);
                        if (messageList.size() > MAX_DELIVERED_MESSAGES_PER_CLIENT) {
                            int toRemove = messageList.size() - MAX_DELIVERED_MESSAGES_PER_CLIENT;
                            for (int i = 0; i < toRemove; i++) {
                                messages.remove(messageList.get(i));
                            }
                        }
                    }
                    
                    // If client has been inactive for too long, remove tracking entirely
                    // For now, we'll use a simplistic approach
                    // In a real implementation, we would track last activity time per client
                    if (Math.random() < 0.05) { // 5% chance of cleaning up old clients
                        clientsToRemove.add(clientId);
                    }
                }
            }
            
            // Remove inactive clients
            for (String clientId : clientsToRemove) {
                deliveredMessages.remove(clientId);
                logger.info("Removed delivery tracking for inactive client: {}", clientId);
            }
            
            if (!clientsToRemove.isEmpty()) {
                logger.info("Cleaned up delivery tracking for {} inactive clients", clientsToRemove.size());
            }
            
        }, 1, 10, TimeUnit.MINUTES); // Check every 10 minutes
    }
    
    /**
     * Starts the synchronization mechanism for message recovery.
     */
    private void startSyncMechanism() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            if (status != NodeStatus.RUNNING) {
                return;
            }
            
            // Request synchronization from other servers if needed
            for (ServerConnection connection : serverConnections.values()) {
                String serverId = connection.getNodeId();
                
                // Skip synchronization with disconnected servers
                if (!connection.isConnected()) {
                    continue;
                }
                
                // Get our last known sequence for this server
                long lastKnownSequence = serverSequences.getOrDefault(serverId, -1L);
                
                // Create a sync request message
                Message syncRequest = new Message(
                        id, 
                        serverId, 
                        "SYNC_REQUEST:" + lastKnownSequence, 
                        MessageType.SYNC_REQUEST
                );
                
                // Send the sync request
                connection.sendMessage(syncRequest);
                logger.debug("Sent sync request to server {} for sequences after {}", 
                        serverId, lastKnownSequence);
            }
        }, 30, 30, TimeUnit.SECONDS); // Check every 30 seconds
    }
    
    /**
     * Enhanced registerMessageHandlers method including sync handlers
     */
    private void registerMessageHandlers() {
        // Register enhanced handler for sync request messages
        registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceId) {
                if (message.getType() != MessageType.SYNC_REQUEST) {
                    return false;
                }
                
                logger.info("Received SYNC_REQUEST from {}", sourceId);
                
                // Find the server connection
                ServerConnection serverConnection = serverConnections.get(sourceId);
                if (serverConnection != null) {
                    handleSyncRequest(message, serverConnection.out);
                } else {
                    logger.warn("Cannot find connection to server {} for sync response", sourceId);
                }
                
                return true;
            }
        });
        
        // Register handler for SYNC_DATA messages
        registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceId) {
                if (message.getType() != MessageType.SYNC_DATA) {
                    return false;
                }
                
                logger.info("Received SYNC_DATA from {}", sourceId);
                
                // Extract the expected message count
                int expectedMessages = 0;
                try {
                    String content = message.getContent();
                    if (content.startsWith("SYNC_DATA:")) {
                        expectedMessages = Integer.parseInt(content.substring("SYNC_DATA:".length()));
                        logger.info("Expecting {} messages in SYNC_DATA from {}", expectedMessages, sourceId);
                    }
                } catch (NumberFormatException e) {
                    logger.error("Invalid message count in SYNC_DATA: {}", message.getContent());
                }
                
                // We'll handle the actual messages in the readMessagesFromServer method
                // Just acknowledge receipt here
                return true;
            }
        });
        
        // Register enhanced handler for sync response messages
        registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceId) {
                if (message.getType() != MessageType.SYNC_RESPONSE) {
                    return false;
                }
                
                logger.info("Received SYNC_RESPONSE from {}", sourceId);
                
                // Direct processing of sync response isn't needed here
                // The processSyncResponse method handles this during the synchronizeWithServer call
                
                return true;
            }
        });
        
        // Register handler for heartbeat messages
        registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceId) {
                if (message.getType() != MessageType.HEARTBEAT) {
                    return false;
                }
                
                logger.debug("Received heartbeat from {}", sourceId);
                return true;
            }
        });
        
        // Register handler for acknowledgment messages
        registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceId) {
                if (message.getType() != MessageType.ACK) {
                    return false;
                }
                
                if (message.isAcknowledgment() && message.getAcknowledgedMessageId() != null) {
                    handleAcknowledgment(message);
                }
                return true;
            }
        });
        
        // If Raft is enabled, add the Raft message handler
        if (RAFT_ENABLED && raftServer != null) {
            registerMessageHandler(new MessageHandler() {
                @Override
                public boolean handleMessage(Message message, String sourceNodeId) {
                    // Check if this is a Raft-related message
                    MessageType type = message.getType();
                    if (type == MessageType.ELECTION || 
                        type == MessageType.VOTE || 
                        type == MessageType.LOG_REPLICATION || 
                        type == MessageType.ACK || 
                        type == MessageType.SYNC_REQUEST || 
                        type == MessageType.SYNC_RESPONSE) {
                        
                        return raftServer.handleMessage(message, sourceNodeId);
                    }
                    
                    return false;
                }
            });
        }
        
        // Handle forwarded messages (for Raft consensus)
        registerMessageHandler(MessageType.FORWARD, (message, sourceNodeId) -> {
            if (RAFT_ENABLED && raftServer != null && raftServer.isLeader()) {
                logger.debug("Received forwarded message from {}", sourceNodeId);
                
                // Extract original message from payload
                if (message.getPayload() instanceof Message) {
                    Message originalMessage = (Message) message.getPayload();
                    logger.info("Handling forwarded message {} from {}", 
                            originalMessage.getId(), sourceNodeId);
                    
                    // Process the original message through Raft
                    return handleUserMessageWithRaft(originalMessage);
                } else {
                    logger.warn("Forwarded message does not contain valid payload");
                }
            } else {
                logger.warn("Received FORWARD message but we are not the leader");
            }
            return false;
        });
    }
    
    /**
     * Simple class to represent server information for synchronization.
     */
    private static class ServerInfo {
        private final String id;
        private final String host;
        private final int port;
        private boolean active;
        private long lastHeartbeat;
        
        public ServerInfo(String id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.active = true;
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public String getId() {
            return id;
        }
        
        public String getHost() {
            return host;
        }
        
        public int getPort() {
            return port;
        }
        
        public boolean isActive() {
            return active;
        }
        
        public void setActive(boolean active) {
            this.active = active;
        }
        
        public long getLastHeartbeat() {
            return lastHeartbeat;
        }
        
        public void setLastHeartbeat(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }
    }
    
    /**
     * Utility class to manage server connections for synchronization
     */
    private static class ServerRegistry {
        public static final long SERVER_TIMEOUT = 30000; // 30 seconds
    }

    /**
     * Synchronizes with a server with enhanced retry logic and error handling.
     * 
     * @param server The server to synchronize with
     */
    private void synchronizeWithServer(ServerInfo server) {
        if (server == null || !server.isActive()) {
            logger.debug("Cannot synchronize with inactive server: {}", server != null ? server.getId() : "null");
            return;
        }

        int maxRetries = 3;
        int retryCount = 0;
        boolean success = false;
        long lastKnownSequence = ((InMemoryMessageStore)messageStore).getCurrentSequence();

        while (!success && retryCount < maxRetries) {
            Socket socket = null;
            ObjectOutputStream out = null;
            ObjectInputStream in = null;
            
            try {
                logger.info("Attempting to synchronize with server {} (retry {}/{})", 
                        server.getId(), retryCount, maxRetries);
                
                // Establish connection with timeout
                socket = new Socket();
                socket.connect(new InetSocketAddress(server.getHost(), server.getPort()), 5000);
                socket.setSoTimeout(15000); // 15 seconds timeout for reads
                
                out = new ObjectOutputStream(socket.getOutputStream());
                in = new ObjectInputStream(socket.getInputStream());

                // Send server identification
                out.writeObject("SERVER");
                out.writeObject(id);
                out.flush();

                // Create a sync request message with our last known sequence
                Message syncRequest = new Message(
                        id,
                        server.getId(),
                        "SYNC_REQUEST:" + lastKnownSequence,
                        MessageType.SYNC_REQUEST
                );

                logger.info("Sending sync request to server {} with last sequence: {}", 
                        server.getId(), lastKnownSequence);
                
                // Send the sync request
                out.writeObject(syncRequest);
                out.flush();
                
                // Wait for the response with timeout handling
                Object response = in.readObject();
                if (response instanceof Message) {
                    Message syncResponse = (Message) response;
                    if (syncResponse.getType() == MessageType.SYNC_RESPONSE) {
                        // Process the messages in the sync response
                        int messagesProcessed = processSyncResponse(syncResponse, in);
                        logger.info("Successfully processed {} messages during sync with server {}", 
                                messagesProcessed, server.getId());
                        success = true;
                    } else {
                        logger.warn("Received unexpected message type during sync: {}", syncResponse.getType());
                        retryCount++;
                    }
                } else {
                    logger.warn("Received non-message object during sync: {}", response);
                    retryCount++;
                }
            } catch (ConnectException ce) {
                // Server is not available, mark as inactive
                logger.warn("Failed to connect to server {} for sync: {}", server.getId(), ce.getMessage());
                server.setActive(false);
                retryCount = maxRetries; // Stop retrying
            } catch (SocketTimeoutException ste) {
                logger.warn("Sync timed out with server {}: {}", server.getId(), ste.getMessage());
                retryCount++;
                // Exponential backoff
                try {
                    Thread.sleep(1000 * (1 << retryCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Error during sync with server {}: {}", server.getId(), e.getMessage());
                retryCount++;
                // Exponential backoff
                try {
                    Thread.sleep(1000 * (1 << retryCount));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } finally {
                // Close resources
                closeQuietly(in);
                closeQuietly(out);
                closeQuietly(socket);
            }
        }
        
        if (!success) {
            logger.error("Failed to synchronize with server {} after {} attempts", 
                    server.getId(), maxRetries);
        }
    }
    
    /**
     * Helper method to close resources quietly
     */
    private static void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException e) {
                logger.warn("Error closing resource: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Helper method to close socket quietly
     */
    private static void closeQuietly(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.warn("Error closing socket: {}", e.getMessage());
            }
        }
    }

    /**
     * Process the sync response and the following messages
     * 
     * @param syncResponse The initial sync response message
     * @param in The input stream to read messages from
     * @return The number of messages processed
     */
    private int processSyncResponse(Message syncResponse, ObjectInputStream in) {
        int messageCount = 0;
        int receivedCount = 0;
        
        try {
            // Extract the number of messages to expect
            String content = syncResponse.getContent();
            if (content.startsWith("SYNC_RESPONSE:")) {
                messageCount = Integer.parseInt(content.substring("SYNC_RESPONSE:".length()));
            } else {
                logger.warn("Invalid sync response format: {}", content);
                return 0;
            }
            
            logger.info("Expecting {} messages in sync response from {}", 
                    messageCount, syncResponse.getSenderId());
            
            if (messageCount == 0) {
                logger.info("No messages to sync from server {}", syncResponse.getSenderId());
                return 0;
            }
            
            // Read all the messages with timeout handling
            for (int i = 0; i < messageCount; i++) {
                int retries = 0;
                int maxRetries = 3;
                boolean received = false;
                
                while (!received && retries < maxRetries) {
                    try {
                        Object obj = in.readObject();
                        if (obj instanceof Message) {
                            Message message = (Message) obj;
                            
                            // Skip if we've already processed this message
                            if (processedMessages.contains(message.getId())) {
                                logger.debug("Skipping already processed message: {}", message.getId());
                                received = true;
                                continue;
                            }
                            
                            // Mark as processed and store
                            processedMessages.add(message.getId());
                            messageStore.storeMessage(message);
                            
                            // Update sequence knowledge if applicable
                            if (message.getSequenceNumber() > 0 && message.getOriginServerId() != null) {
                                serverSequences.put(message.getOriginServerId(), 
                                        Math.max(message.getSequenceNumber(), 
                                                serverSequences.getOrDefault(message.getOriginServerId(), -1L)));
                            }
                            
                            receivedCount++;
                            received = true;
                            
                            // Send acknowledgment if needed
                            if (message.getType() == MessageType.USER_MESSAGE) {
                                sendAcknowledgment(message);
                            }
                        } else {
                            logger.warn("Non-message object received during sync: {}", obj);
                            retries++;
                        }
                    } catch (ClassNotFoundException e) {
                        logger.error("Error deserializing sync message: {}", e.getMessage());
                        retries++;
                    } catch (EOFException eof) {
                        logger.warn("Connection closed prematurely during sync after receiving {} messages", receivedCount);
                        return receivedCount; // Return what we have so far
                    } catch (SocketTimeoutException ste) {
                        logger.warn("Timeout reading sync message {}/{}, retrying: {}", 
                                i + 1, messageCount, ste.getMessage());
                        retries++;
                    } catch (IOException e) {
                        logger.error("IO error during sync message read: {}", e.getMessage());
                        retries++;
                    }
                    
                    // Backoff between retries
                    if (!received && retries < maxRetries) {
                        try {
                            Thread.sleep(500 * (1 << retries));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                
                if (!received) {
                    logger.warn("Failed to receive message {}/{} after {} retries", 
                            i + 1, messageCount, maxRetries);
                }
            }
            
            logger.info("Sync complete: received {}/{} messages from server {}", 
                    receivedCount, messageCount, syncResponse.getSenderId());
            
        } catch (NumberFormatException e) {
            logger.error("Invalid message count in sync response: {}", syncResponse.getContent());
        }
        
        return receivedCount;
    }

    /**
     * Handles an incoming sync request from another server.
     * 
     * @param syncRequest The sync request message
     * @param out The output stream to send responses to
     */
    private void handleSyncRequest(Message syncRequest, ObjectOutputStream out) {
        String serverId = syncRequest.getSenderId();
        logger.info("Handling sync request from server {}", serverId);
        
        try {
            // Extract the last known sequence from the request content
            String content = syncRequest.getContent();
            long lastKnownSequence = -1;
            
            if (content.startsWith("SYNC_REQUEST:")) {
                try {
                    lastKnownSequence = Long.parseLong(content.substring("SYNC_REQUEST:".length()));
                } catch (NumberFormatException e) {
                    logger.error("Invalid sequence number in sync request: {}", content);
                    sendErrorResponse(out, serverId, "Invalid sequence number format");
                    return;
                }
            } else {
                logger.warn("Invalid sync request format: {}", content);
                sendErrorResponse(out, serverId, "Invalid request format");
                return;
            }
            
            // Get messages that need to be synchronized (limit batch size)
            int maxSyncMessages = 100;
            List<Message> syncMessages = ((InMemoryMessageStore)messageStore).getSyncMessages(lastKnownSequence, maxSyncMessages);
            
            // Send sync response with message count
            Message syncResponse = new Message(
                id,
                serverId,
                "SYNC_RESPONSE:" + syncMessages.size(),
                MessageType.SYNC_RESPONSE
            );
            
            logger.info("Sending {} messages for sync to server {}", syncMessages.size(), serverId);
            
            // Send the response header
            out.writeObject(syncResponse);
            out.flush();
            
            // If no messages, we're done
            if (syncMessages.isEmpty()) {
                logger.debug("No messages to synchronize for server {}", serverId);
                return;
            }
            
            // Send each message with error handling
            int sentCount = 0;
            for (Message message : syncMessages) {
                try {
                    out.writeObject(message);
                    out.flush();
                    sentCount++;
                    
                    // Add a small delay to prevent overwhelming the network
                    if (sentCount % 10 == 0) {
                        Thread.sleep(50);
                    }
                } catch (IOException e) {
                    logger.error("Error sending sync message to {}: {}", serverId, e.getMessage());
                    break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Sync interrupted while sending messages to {}", serverId);
                    break;
                }
            }
            
            logger.info("Successfully sent {}/{} messages to server {} during sync", 
                    sentCount, syncMessages.size(), serverId);
            
        } catch (IOException e) {
            logger.error("Error handling sync request from {}: {}", serverId, e.getMessage());
        }
    }
    
    /**
     * Send an error response for a sync request
     */
    private void sendErrorResponse(ObjectOutputStream out, String serverId, String errorMessage) {
        try {
            Message errorResponse = new Message(
                id,
                serverId,
                "SYNC_ERROR:" + errorMessage,
                MessageType.SYNC_RESPONSE
            );
            out.writeObject(errorResponse);
            out.flush();
        } catch (IOException e) {
            logger.error("Failed to send error response to {}: {}", serverId, e.getMessage());
        }
    }
    
    /**
     * Represents a connection to a client.
     */
    private static class ClientConnection {
        private final String nodeId;
        private final Socket socket;
        private final ObjectInputStream in;
        private final ObjectOutputStream out;
        private boolean connected;
        
        public ClientConnection(String nodeId, Socket socket, ObjectInputStream in, ObjectOutputStream out) {
            this.nodeId = nodeId;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.connected = true;
        }
        
        public String getNodeId() {
            return nodeId;
        }
        
        public ObjectInputStream getInputStream() {
            return in;
        }
        
        public boolean isConnected() {
            return connected && !socket.isClosed();
        }
        
        public boolean sendMessage(Message message) {
            try {
                out.writeObject(message);
                out.flush();
                if (message.getType() == MessageType.USER_MESSAGE) {
                    logger.info("Message delivered to client {}: {} -> {}: '{}'", 
                            nodeId, message.getSenderId(), message.getRecipientId(), message.getContent());
                }
                return true;
            } catch (IOException e) {
                logger.error("Error sending message to client {}: {}", nodeId, e.getMessage());
                return false;
            }
        }
        
        public void close() {
            connected = false;
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Error closing client connection: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Represents a connection to another server.
     */
    private static class ServerConnection {
        private final String nodeId;
        private final Socket socket;
        private final ObjectInputStream in;
        private final ObjectOutputStream out;
        private volatile boolean connected;
        private long lastAttemptedWrite = 0;
        private int consecutiveErrors = 0;
        
        public ServerConnection(String nodeId, Socket socket, ObjectInputStream in, ObjectOutputStream out) {
            this.nodeId = nodeId;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.connected = true;
        }
        
        public String getNodeId() {
            return nodeId;
        }
        
        public ObjectInputStream getInputStream() {
            return in;
        }
        
        public boolean isConnected() {
            return connected && !socket.isClosed() && consecutiveErrors < 3;
        }
        
        public void markPotentiallyBroken() {
            consecutiveErrors++;
            if (consecutiveErrors >= 3) {
                logger.warn("Connection to server {} marked as potentially broken after {} errors", 
                        nodeId, consecutiveErrors);
            }
        }
        
        public boolean sendMessage(Message message) {
            // Throttle sending to avoid overwhelming the connection
            long now = System.currentTimeMillis();
            if (now - lastAttemptedWrite < 10) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            lastAttemptedWrite = System.currentTimeMillis();
            
            try {
                // Check socket status before attempting to write
                if (!isConnected()) {
                    logger.warn("Cannot send message: connection to server {} is not active", nodeId);
                    return false;
                }
                
                out.writeObject(message);
                out.flush();
                consecutiveErrors = 0; // Reset error counter on success
                
                if (message.getType() == MessageType.USER_MESSAGE) {
                    logger.info("Message forwarded to server {}: {} -> {}: '{}'", 
                            nodeId, message.getSenderId(), message.getRecipientId(), message.getContent());
                }
                return true;
            } catch (IOException e) {
                logger.error("Error sending message to server {}: {}", nodeId, e.getMessage());
                markPotentiallyBroken();
                return false;
            }
        }
        
        public void close() {
            connected = false;
            closeQuietly(in);
            closeQuietly(out);
            closeQuietly(socket);
        }
    }

    /**
     * Connect to a specific peer server with improved error handling and retry logic.
     * 
     * @param peerServer The peer server address in format "host:port" or server ID
     */
    private void connectToPeerServer(String peerServer) {
        // Find the actual server address if we were given a server ID
        final String serverAddress;
        if (!peerServer.contains(":")) {
            Optional<String> address = peerServerAddresses.stream()
                    .filter(addr -> addr.startsWith(peerServer + ":") || addr.equals(peerServer))
                    .findFirst();
            
            if (!address.isPresent()) {
                logger.warn("Cannot find address for server ID: {}", peerServer);
                return;
            }
            serverAddress = address.get();
        } else {
            serverAddress = peerServer;
        }
        
        // Don't try to connect if we already have a connection
        if (serverConnections.values().stream()
                .anyMatch(conn -> conn.getNodeId().equals(serverAddress) && conn.isConnected())) {
            logger.debug("Already connected to peer server: {}", serverAddress);
            return;
        }
        
        String[] parts = serverAddress.split(":");
        if (parts.length != 2) {
            logger.warn("Invalid server address format: {}", serverAddress);
            return;
        }
        
        final String host = parts[0];
        final int port;
        try {
            port = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            logger.error("Invalid port in server address {}: {}", serverAddress, e.getMessage());
            return;
        }
        
        logger.info("Connecting to peer server at {}:{}", host, port);
        
        final String finalServerAddress = serverAddress;
        connectionExecutor.submit(() -> {
            Socket socket = null;
            
            try {
                // Connect with timeout
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), 5000);
                socket.setSoTimeout(10000); // 10 second read timeout
                
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                
                // Identify as a server
                out.writeObject("SERVER");
                out.writeObject(id);
                out.flush();
                
                // Create a connection
                ServerConnection serverConnection = new ServerConnection(finalServerAddress, socket, in, out);
                ServerConnection previousConnection = serverConnections.put(finalServerAddress, serverConnection);
                
                // Close previous connection if it exists
                if (previousConnection != null) {
                    previousConnection.close();
                    logger.info("Closed previous connection to server: {}", finalServerAddress);
                }
                
                // Start reading messages
                connectionExecutor.submit(() -> readMessagesFromServer(serverConnection));
                
                logger.info("Successfully connected to peer server: {}", finalServerAddress);
                
                // Send any delayed acknowledgments or pending messages
                synchronizeWithPeer(finalServerAddress);
                
                // If we're the primary server, try to replicate any pending messages
                if (replicationManager.isPrimary(id) && !pendingReplicationQueue.isEmpty()) {
                    logger.info("Attempting to replicate pending messages to newly connected backup server: {}", finalServerAddress);
                    // Schedule with a slight delay to allow the connection to fully establish
                    scheduledExecutor.schedule(this::retryPendingReplications, 2, TimeUnit.SECONDS);
                }
                
            } catch (IOException e) {
                logger.error("Error connecting to peer server {}: {}", finalServerAddress, e.getMessage());
                closeQuietly(socket);
                
                // Schedule retry
                scheduledExecutor.schedule(() -> connectToPeerServer(finalServerAddress), 
                        10, TimeUnit.SECONDS);
            }
        });
    }
    
    /**
     * Synchronize with a peer after establishing a connection
     */
    private void synchronizeWithPeer(String serverId) {
        try {
            logger.info("Synchronizing with peer server: {}", serverId);
            
            // Check server connection
            ServerConnection connection = serverConnections.get(serverId);
            if (connection == null || !connection.isConnected()) {
                logger.warn("Cannot synchronize with server {}: no active connection", serverId);
                return;
            }
            
            // Create a sync request
            long lastKnownSequence = serverSequences.getOrDefault(serverId, -1L);
            Message syncRequest = new Message(
                    id,
                    serverId,
                    "SYNC_REQUEST:" + lastKnownSequence,
                    MessageType.SYNC_REQUEST
            );
            
            // Send sync request
            connection.sendMessage(syncRequest);
            logger.info("Sent sync request to server {} for sequences after {}", 
                    serverId, lastKnownSequence);
                    
            // Schedule a full message sync in case the initial sync misses any messages
            scheduledExecutor.schedule(() -> {
                if (running && connection.isConnected()) {
                    logger.info("Performing full message sync with server: {}", serverId);
                    
                    // Send all messages in our store that might be needed by the peer
                    List<Message> allMessages = ((InMemoryMessageStore)messageStore).getRecentMessages(1000);
                    List<Message> userMessages = allMessages.stream()
                            .filter(m -> m.getType() == MessageType.USER_MESSAGE)
                            .collect(Collectors.toList());
                    
                    if (!userMessages.isEmpty()) {
                        logger.info("Syncing {} user messages with server {}", userMessages.size(), serverId);
                        
                        // Create a sync data message
                        Message syncData = new Message(
                                id,
                                serverId,
                                "SYNC_DATA:" + userMessages.size(),
                                MessageType.SYNC_DATA
                        );
                        
                        // Send each message
                        connection.sendMessage(syncData);
                        for (Message msg : userMessages) {
                            connection.sendMessage(msg);
                        }
                        
                        logger.info("Completed full message sync with server {}", serverId);
                    } else {
                        logger.info("No user messages to sync with server {}", serverId);
                    }
                }
            }, 5, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            logger.error("Error during synchronization with peer {}: {}", 
                    serverId, e.getMessage(), e);
        }
    }

    /**
     * Deliver a message to a client connected to this server.
     * 
     * @param message The message to deliver
     * @return true if the message was delivered, false otherwise
     */
    private boolean deliverMessageToClient(Message message) {
        String recipientId = message.getRecipientId();
        
        // Check if we have a connection to the recipient
        ClientConnection clientConnection = clientConnections.get(recipientId);
        if (clientConnection != null) {
            logger.info("Delivering message to client '{}' (local connection)", recipientId);
            
            // Get or create the set of delivered message IDs for this client
            Set<UUID> delivered = deliveredMessages.computeIfAbsent(recipientId, 
                    k -> Collections.newSetFromMap(new ConcurrentHashMap<>()));
            
            // Check if this message has already been delivered
            if (delivered.contains(message.getId())) {
                logger.info("Message {} has already been delivered to client {}, skipping", 
                        message.getId(), recipientId);
                return true; // Consider it success since client already has it
            }
            
            // Send the message
            boolean success = clientConnection.sendMessage(message);
            
            // If successfully delivered, record it
            if (success) {
                delivered.add(message.getId());
                logger.debug("Marked message {} as delivered to client {}", 
                        message.getId(), recipientId);
            }
            
            return success;
        } else {
            logger.warn("Cannot find client connection for recipient: {}", recipientId);
            return false;
        }
    }

    /**
     * Deliver a message based on its type and destination.
     * This handles system messages and messages to other servers.
     * 
     * @param message The message to deliver
     * @return true if the message was handled, false otherwise
     */
    private boolean deliverMessage(Message message) {
        // Handle different message types
        if (message.getType() == MessageType.ACK) {
            // Handle acknowledgment message
            if (message.isAcknowledgment() && message.getAcknowledgedMessageId() != null) {
                handleAcknowledgment(message);
            }
            return true;
        } 
        else if (message.getType() == MessageType.USER_MESSAGE) {
            // For user messages, we should route to the recipient
            return deliverMessageToClient(message);
        }
        else {
            // System messages - store locally and forward to all servers
            messageStore.storeMessage(message);
            
            // Forward to all servers except the sender
            for (ServerConnection serverConnection : serverConnections.values()) {
                if (!serverConnection.getNodeId().equals(message.getSenderId())) {
                    serverConnection.sendMessage(message);
                }
            }
            return true;
        }
    }

    /**
     * Sets up time synchronization based on the node's role
     */
    private void setupTimeSync() {
        String timeSyncServerId = null;
        
        if (RAFT_ENABLED) {
            // In Raft mode, the leader is the time server
            if (raftServer != null && raftServer.isLeader()) {
                // This server is the time server
                logger.info("This server is the Raft leader and will act as the time server");
            } else {
                // Use the Raft leader as the time server
                timeSyncServerId = raftServer.getCurrentLeader();
                logger.info("Using Raft leader {} as the time server", timeSyncServerId);
            }
        } else {
            // In primary-backup mode, the primary server is the time server
            String primaryServer = replicationManager.getPrimaryServer();
            
            if (id.equals(primaryServer)) {
                // This server is the time server
                logger.info("This server is the primary and will act as the time server");
            } else {
                // Use the primary server as the time server
                timeSyncServerId = primaryServer;
                logger.info("Using primary server {} as the time server", timeSyncServerId);
            }
        }
        
        // Initialize time synchronization
        setupTimeSync(timeSyncServerId);
        logger.info("Time synchronization initialized. Server role: {}", 
                timeSyncServerId == null ? "Time Server" : "Time Client");
    }

    // Update a timestamp on a message to use the synchronized time
    private Message applyTimeSynchronization(Message message) {
        if (message instanceof TimestampedMessage) {
            TimestampedMessage timestampedMessage = (TimestampedMessage) message;
            long originalTimestamp = timestampedMessage.getTimestamp();
            long adjustedTimestamp = adjustTimestamp(originalTimestamp);
            
            // If there's a significant difference, log it
            if (Math.abs(adjustedTimestamp - originalTimestamp) > 100) {
                logger.debug("Adjusted message timestamp from {} to {} (delta: {}ms)", 
                        originalTimestamp, adjustedTimestamp, 
                        adjustedTimestamp - originalTimestamp);
            }
            
            timestampedMessage.setTimestamp(adjustedTimestamp);
        }
        return message;
    }

    // Updates the heartbeat timestamp for a server
    private void updateHeartbeat(String serverId) {
        // ... existing implementation ...
    }

    /**
     * Check if this server is the leader in consensus mode.
     * 
     * @return true if this server is the leader
     */
    public boolean isLeader() {
        if (RAFT_ENABLED && raftServer != null) {
            return raftServer.isLeader();
        } else {
            // In primary-backup mode, check if this is the primary server
            String primaryServer = replicationManager.getPrimaryServer();
            return id.equals(primaryServer);
        }
    }

    /**
     * Attempts to replicate pending messages that couldn't be replicated earlier due to lack of backup servers.
     * This method should be called when a new backup server connects.
     */
    private void retryPendingReplications() {
        if (pendingReplicationQueue.isEmpty()) {
            return; // No pending messages to replicate
        }
        
        Set<String> targets = replicationManager.getReplicationTargets();
        targets.remove(id); // Remove self
        
        if (targets.isEmpty()) {
            logger.warn("Cannot retry pending replications, still no backup servers available");
            return;
        }
        
        logger.info("Attempting to replicate {} pending messages to newly connected backup servers", 
                pendingReplicationQueue.size());
        
        // Process all pending messages
        List<Message> pendingMessages = new ArrayList<>();
        Message message;
        while ((message = pendingReplicationQueue.poll()) != null) {
            pendingMessages.add(message);
        }
        
        int successCount = 0;
        for (Message pendingMessage : pendingMessages) {
            // Create a new future for tracking this replication
            CompletableFuture<Boolean> newFuture = new CompletableFuture<>();
            pendingOperations.put(pendingMessage.getId(), newFuture);
            
            // Try to replicate to all available backup servers
            replicateToBackups(pendingMessage);
            
            try {
                boolean success = newFuture.get(operationTimeoutMs, TimeUnit.MILLISECONDS);
                if (success) {
                    successCount++;
                    
                    // Now that it's replicated, we can deliver it locally if needed
                    deliverMessageToClient(pendingMessage);
                } else {
                    // If it failed again, we'll have already put it back in the pending queue
                    logger.warn("Failed to replicate pending message {} on retry", pendingMessage.getId());
                }
            } catch (Exception e) {
                logger.error("Error retrying replication for message {}: {}", 
                        pendingMessage.getId(), e.getMessage());
                
                // Put it back in the queue
                pendingReplicationQueue.add(pendingMessage);
            }
        }
        
        logger.info("Completed retry of pending replications. Success: {}/{}", 
                successCount, pendingMessages.size());
    }
} 