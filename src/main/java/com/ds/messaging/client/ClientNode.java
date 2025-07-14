package com.ds.messaging.client;

import com.ds.messaging.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Client implementation of a node in the distributed messaging system.
 * Connects to a server and allows sending/receiving messages.
 */
public class ClientNode extends AbstractNode {
    private static final Logger logger = LoggerFactory.getLogger(ClientNode.class);
    
    private final String serverHost;
    private final int serverPort;
    private final List<String> backupServers;
    private final Queue<Message> outgoingQueue;
    private final Set<UUID> sentMessages;
    
    // Set to track received message IDs to avoid duplicates
    private final Set<UUID> receivedMessageIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    // Map to track pending acknowledgments with completion futures
    private final Map<UUID, CompletableFuture<Boolean>> pendingAcks = new ConcurrentHashMap<>();
    
    // Timeout for acknowledgments (ms) - increased from 10s to 30s to accommodate consensus
    private final long ackTimeoutMs = 30000; // 30 seconds
    
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutor;
    private String currentServerAddress;
    private boolean running;
    
    private final ReentrantLock connectionLock = new ReentrantLock();
    private volatile boolean isConnecting = false;
    
    /**
     * Creates a new client node.
     * 
     * @param id The client ID
     * @param serverHost The primary server host
     * @param serverPort The primary server port
     * @param backupServers List of backup servers in format "host:port"
     */
    public ClientNode(String id, String serverHost, int serverPort, List<String> backupServers) {
        super(id);
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.backupServers = new ArrayList<>(backupServers);
        this.outgoingQueue = new ConcurrentLinkedQueue<>();
        this.sentMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.running = false;
    }
    
    @Override
    public void start() throws IOException {
        super.start();
        
        // Load received message IDs from persistent storage
        loadReceivedMessageIds();
        
        this.executorService = Executors.newCachedThreadPool();
        this.scheduledExecutor = Executors.newScheduledThreadPool(1);
        this.running = true;
        
        // Connect to the primary server
        connectToServer(serverHost, serverPort);
        
        // Setup time synchronization with the connected server
        setupTimeSynchronization();
        
        // Start message processor
        executorService.submit(this::processOutgoingQueue);
        
        // Schedule periodic reconnection attempts if disconnected
        scheduleReconnection();
        
        // Schedule periodic check for primary server availability
        schedulePrimaryServerCheck();
        
        // Schedule periodic saving of received message IDs
        scheduledExecutor.scheduleAtFixedRate(this::saveReceivedMessageIds, 5, 30, TimeUnit.MINUTES);
        
        status = NodeStatus.RUNNING;
        logger.info("Client node started and connected to server {}:{}", serverHost, serverPort);
    }
    
    /**
     * Sets up time synchronization with the currently connected server
     */
    private void setupTimeSynchronization() {
        if (currentServerAddress != null) {
            // Extract the server ID - for simplicity, we'll use the server's address as its ID
            String serverIdForSync = currentServerAddress;
            
            // Set up time synchronization with the current server
            setupTimeSync(serverIdForSync);
            logger.info("Time synchronization set up with server: {}", serverIdForSync);
        } else {
            logger.warn("Cannot set up time synchronization, not connected to a server");
        }
    }
    
    @Override
    public void stop() {
        running = false;
        
        // Save received message IDs to persistent storage
        saveReceivedMessageIds();
        
        // Close the socket
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.info("Minor issue while closing connection: {}", e.getMessage());
        }
        
        // Shutdown executors
        if (executorService != null) {
            executorService.shutdownNow();
        }
        
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdownNow();
        }
        
        super.stop();
        logger.info("Client node stopped");
    }
    
    /**
     * Saves the set of received message IDs to a file for persistence across restarts.
     */
    private void saveReceivedMessageIds() {
        File file = new File("received_messages_" + id + ".dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
            // First clean up by removing older messages if the set gets too large
            if (receivedMessageIds.size() > 10000) {
                logger.info("Trimming received message IDs cache from {} to 5000 entries", receivedMessageIds.size());
                List<UUID> sortedIds = new ArrayList<>(receivedMessageIds);
                // Simple approach: just keep the most recent 5000 messages
                // In a real implementation, we would track timestamps for each message ID
                int toRemove = sortedIds.size() - 5000;
                for (int i = 0; i < toRemove; i++) {
                    receivedMessageIds.remove(sortedIds.get(i));
                }
            }
            
            // Save the set to a file
            oos.writeObject(new ArrayList<>(receivedMessageIds));
            logger.info("Saved {} received message IDs to {}", receivedMessageIds.size(), file.getName());
        } catch (IOException e) {
            logger.info("Could not save message history: {}. This won't affect messaging functionality.", e.getMessage());
        }
    }
    
    /**
     * Loads the set of received message IDs from a file.
     */
    @SuppressWarnings("unchecked")
    private void loadReceivedMessageIds() {
        File file = new File("received_messages_" + id + ".dat");
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                List<UUID> loadedIds = (List<UUID>) ois.readObject();
                if (loadedIds != null) {
                    receivedMessageIds.addAll(loadedIds);
                    logger.info("Loaded {} received message IDs from {}", loadedIds.size(), file.getName());
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.info("Could not load message history: {}. This won't affect messaging functionality.", e.getMessage());
            }
        } else {
            logger.info("No saved message history found. This is normal for first-time use.");
        }
    }
    
    /**
     * Sends a message and returns a future that completes when the message is acknowledged.
     * 
     * @param message The message to send
     * @return A CompletableFuture that completes with true when acknowledged, or false on failure/timeout
     */
    public CompletableFuture<Boolean> sendMessageWithAck(Message message) {
        if (status != NodeStatus.RUNNING) {
            logger.warn("Cannot send message when client is not running");
            return CompletableFuture.completedFuture(false);
        }
        
        // Apply time synchronization to outgoing messages if they have timestamps
        if (message instanceof TimestampedMessage) {
            TimestampedMessage timestampedMessage = (TimestampedMessage) message;
            long adjustedTime = adjustTimestamp(timestampedMessage.getTimestamp());
            
            if (Math.abs(adjustedTime - timestampedMessage.getTimestamp()) > 100) {
                logger.debug("Adjusted outgoing message timestamp from {} to {} (offset: {}ms)",
                        timestampedMessage.getTimestamp(), adjustedTime, 
                        adjustedTime - timestampedMessage.getTimestamp());
            }
            
            timestampedMessage.setTimestamp(adjustedTime);
        }
        
        // Create a future for this message
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        pendingAcks.put(message.getId(), future);
        
        // Add to the outgoing queue
        outgoingQueue.add(message);
        logger.debug("Message added to outgoing queue: {}", message.getId());
        
        // Schedule a timeout for the acknowledgment
        scheduledExecutor.schedule(() -> {
            CompletableFuture<Boolean> pendingFuture = pendingAcks.remove(message.getId());
            if (pendingFuture != null && !pendingFuture.isDone()) {
                logger.warn("Acknowledgment timeout for message: {}", message.getId());
                pendingFuture.complete(false);
            }
        }, ackTimeoutMs, TimeUnit.MILLISECONDS);
        
        return future;
    }
    
    @Override
    public boolean sendMessage(Message message) {
        CompletableFuture<Boolean> future = sendMessageWithAck(message);
        
        // This method still returns immediately for backward compatibility
        return true;
    }
    
    @Override
    public List<String> getConnectedNodes() {
        if (currentServerAddress != null && socket != null && !socket.isClosed()) {
            return Collections.singletonList(currentServerAddress);
        }
        return Collections.emptyList();
    }
    
    /**
     * Connect to a server.
     * 
     * @param host The server host
     * @param port The server port
     * @throws IOException If connection fails
     */
    private void connectToServer(String host, int port) throws IOException {
        try {
            socket = new Socket(host, port);
            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            
            // Identify as a client
            out.writeObject("CLIENT");
            out.writeObject(id);
            out.flush();
            
            currentServerAddress = host + ":" + port;
            
            // Start reading messages from the server
            executorService.submit(this::readMessagesFromServer);
            
            // Log connection success
            logger.info("Connected to server: {}:{}", host, port);
            
            // Determine if this is the primary server or a backup
            boolean isPrimary = (host.equals(serverHost) && port == serverPort);
            
            // Show an appropriate message
            if (isPrimary) {
                if (status == NodeStatus.DEGRADED) {
                    System.out.println("\nReconnected to primary server successfully!");
                } else {
                    System.out.println(" Connected to primary server: " + host + ":" + port);
                }
            } else {
                System.out.println("\n Connected to backup server: " + host + ":" + port);
            }
            
            status = NodeStatus.RUNNING;
        } catch (IOException e) {
            logger.info("Could not connect to server {}:{}: {}", host, port, e.getMessage());
            System.out.println("Could not connect to server " + host + ":" + port + " - will try backup servers if available");
            throw e;
        }
    }
    
    /**
     * Try to connect to one of the available servers.
     * 
     * @return true if successfully connected, false otherwise
     */
    private boolean tryConnectToAnyServer() {
        // Prevent multiple simultaneous connection attempts
        if (!connectionLock.tryLock()) {
            logger.debug("Connection attempt already in progress");
            return false;
        }
        
        try {
            if (isConnecting) {
                logger.debug("Already attempting to connect");
                return false;
            }
            
            isConnecting = true;
            
            // Make sure any existing connections are properly cleaned up
            cleanupConnection();
            
            // First try the primary server
            try {
                connectToServer(serverHost, serverPort);
                return true;
            } catch (IOException e) {
                logger.info("Could not connect to primary server: {}", e.getMessage());
                System.out.println(" Trying backup servers...");
            }
            
            // Try backup servers
            for (String serverAddress : backupServers) {
                try {
                    String[] parts = serverAddress.split(":");
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    
                    connectToServer(host, port);
                    logger.info("SUCCESSFULLY CONNECTED TO BACKUP SERVER: {}", serverAddress);
                    return true;
                } catch (IOException e) {
                    logger.info("Could not connect to backup server {}: {}", serverAddress, e.getMessage());
                    System.out.println("   ↪ Backup server " + serverAddress + " unavailable");
                }
            }
            
            logger.warn("Could not connect to any server. Will keep trying in the background...");
            System.out.println("\nNo servers available right now. System will automatically reconnect when a server becomes available.");
            status = NodeStatus.DEGRADED;
            return false;
        } finally {
            isConnecting = false;
            connectionLock.unlock();
        }
    }
    
    /**
     * Schedule reconnection attempts if disconnected.
     */
    private void scheduleReconnection() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            if (status != NodeStatus.RUNNING && running && !isConnecting) {
                logger.info("Attempting to reconnect to a server...");
                System.out.println("\nAttempting to reconnect to messaging network...");
                tryConnectToAnyServer();
            }
        }, 10, 10, TimeUnit.SECONDS); // Increased interval from 5 to 10 seconds
    }
    
    /**
     * Schedule periodic check for primary server availability
     */
    private void schedulePrimaryServerCheck() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            if (running && status == NodeStatus.RUNNING && !isConnecting) {
                // If we're connected to a backup server, check if primary is available
                if (currentServerAddress != null && !currentServerAddress.equals(serverHost + ":" + serverPort)) {
                    logger.info("Checking if primary server is available...");
                    try {
                        // Try to connect to primary server
                        Socket testSocket = new Socket();
                        testSocket.connect(new InetSocketAddress(serverHost, serverPort), 2000); // 2 second timeout
                        testSocket.close();
                        
                        // If we get here, primary server is available
                        logger.info("Primary server is available, attempting to reconnect...");
                        System.out.println("\nPrimary server is available, attempting to reconnect...");
                        
                        // Try to connect to primary server
                        if (tryConnectToPrimaryServer()) {
                            logger.info("Successfully reconnected to primary server");
                            System.out.println("Successfully reconnected to primary server!");
                        }
                    } catch (IOException e) {
                        // Primary server is still not available, continue with backup
                        logger.debug("Primary server not yet available: {}", e.getMessage());
                    }
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    /**
     * Try to connect to the primary server
     * @return true if successfully connected, false otherwise
     */
    private boolean tryConnectToPrimaryServer() {
        if (!connectionLock.tryLock()) {
            logger.debug("Connection attempt already in progress");
            return false;
        }
        
        try {
            if (isConnecting) {
                logger.debug("Already attempting to connect");
                return false;
            }
            
            isConnecting = true;
            
            // Make sure any existing connections are properly cleaned up
            cleanupConnection();
            
            try {
                connectToServer(serverHost, serverPort);
                return true;
            } catch (IOException e) {
                logger.info("Could not connect to primary server: {}", e.getMessage());
                return false;
            }
        } finally {
            isConnecting = false;
            connectionLock.unlock();
        }
    }
    
    /**
     * Process messages in the outgoing queue.
     */
    private void processOutgoingQueue() {
        while (running) {
            try {
                Message message = outgoingQueue.poll();
                if (message != null) {
                    boolean isConnected = (status == NodeStatus.RUNNING && socket != null && !socket.isClosed());
                    if (isConnected) {
                        try {
                            out.writeObject(message);
                            out.flush();
                            sentMessages.add(message.getId());
                            logger.debug("Message transmitted to server: {}", message.getId());
                            // Note: We don't complete the future here - we wait for ACK
                        } catch (IOException e) {
                            logger.info("Connection issue while sending message: {}. Will retry.", e.getMessage());
                            System.out.println("\nConnection issue while sending message. Will retry automatically.");
                            // Put the message back in the queue
                            outgoingQueue.add(message);
                            // Clean up the connection and try to reconnect
                            cleanupConnection();
                            status = NodeStatus.DEGRADED;
                            // Immediate reconnection attempt instead of waiting for the scheduled one
                            tryConnectToAnyServer();
                        }
                    } else {
                        // If not connected, put the message back in the queue
                        outgoingQueue.add(message);
                        // Attempt to reconnect if we're not already in a reconnection cycle
                        if (status != NodeStatus.RUNNING && socket == null) {
                            tryConnectToAnyServer();
                        }
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing outgoing message: {}", e.getMessage(), e);
            }
        }
    }
    
    /**
     * Process a received message, including adjusting its timestamp
     * 
     * @param message The message to process
     */
    private void processReceivedMessage(Message message) {
        // Apply time synchronization to incoming messages if they have timestamps
        if (message instanceof TimestampedMessage) {
            TimestampedMessage timestampedMessage = (TimestampedMessage) message;
            long originalTime = timestampedMessage.getTimestamp();
            long adjustedTime = adjustTimestamp(originalTime);
            
            if (Math.abs(adjustedTime - originalTime) > 100) {
                logger.debug("Adjusted incoming message timestamp from {} to {} (offset: {}ms)",
                        originalTime, adjustedTime, adjustedTime - originalTime);
            }
            
            timestampedMessage.setTimestamp(adjustedTime);
        }
        
        // Check if this is an acknowledgment message
        if (message.getType() == MessageType.ACK && message.isAcknowledgment()) {
            UUID ackMessageId = message.getAcknowledgedMessageId();
            CompletableFuture<Boolean> future = pendingAcks.remove(ackMessageId);
            if (future != null && !future.isDone()) {
                future.complete(true);
                logger.debug("Received acknowledgment for message: {}", ackMessageId);
            }
        } else {
            // Check if we've already received this message to avoid duplicates
            if (receivedMessageIds.contains(message.getId())) {
                // Skip processing for duplicate messages
                logger.debug("Received duplicate message, ignoring: {}", message.getId());
                return;
            }
            
            // Add to received set
            receivedMessageIds.add(message.getId());
            
            // Process regular message (for display or other handling)
            processMessage(message, message.getSenderId());
            
            // For user messages, print to console
            if (message.getType() == MessageType.USER_MESSAGE) {
                String sender = message.getSenderId();
                String content = message.getContent();
                System.out.println("\nMessage from " + sender + ": " + content);
            }
        }
    }
    
    /**
     * Reads messages from the connected server.
     */
    private void readMessagesFromServer() {
        try {
            while (running && socket != null && !socket.isClosed()) {
                try {
                    Object obj = in.readObject();
                    if (obj instanceof Message) {
                        Message message = (Message) obj;
                        processReceivedMessage(message);
                    } else {
                        logger.warn("Received unknown object type: {}", obj.getClass());
                    }
                } catch (ClassNotFoundException e) {
                    logger.error("Error deserializing message: {}", e.getMessage());
                }
            }
        } catch (IOException e) {
            if (running) {
                // Check if this is just a socket closed exception (normal disconnect)
                if (e.getMessage() != null && e.getMessage().contains("Socket closed")) {
                    logger.info("Connection to server was closed. This might be due to server shutdown or network issues. Will attempt to reconnect...");
                    System.out.println("\n Server connection lost. Attempting to reconnect automatically...");
                } else {
                    // For other IO exceptions, log the error but still with a user-friendly message
                    logger.info("Lost connection to server: {}. Will attempt to reconnect...", e.getMessage());
                    System.out.println("\n Connection issue detected. Attempting to reconnect automatically...");
                }
                
                // Clean up the current connection resources
                cleanupConnection();
                
                // Immediate reconnection attempt
                tryConnectToAnyServer();
                
                // Schedule periodic reconnection attempts
                scheduledExecutor.scheduleAtFixedRate(() -> {
                    if (status != NodeStatus.RUNNING && running && !isConnecting) {
                        logger.info("Attempting to reconnect to a server...");
                        System.out.println("\nAttempting to reconnect to messaging network...");
                        tryConnectToAnyServer();
                    }
                }, 5, 5, TimeUnit.SECONDS);
            }
        }
    }
    
    /**
     * Clean up the current connection resources
     */
    private void cleanupConnection() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            // Reset connection variables
            socket = null;
            in = null;
            out = null;
            currentServerAddress = null; // Reset the current server address
            status = NodeStatus.DEGRADED; // Ensure status is updated
        } catch (IOException e) {
            logger.info("Minor issue while cleaning up connection: {}", e.getMessage());
        }
    }
} 