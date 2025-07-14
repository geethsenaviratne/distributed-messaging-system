package com.ds.messaging;

import com.ds.messaging.client.ClientNode;
import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dedicated test class for testing log replication correctness in consensus algorithm
 * by killing a leader after a specific number of messages and verifying logs are consistent
 * after recovery.
 */
public class ReplicationTest {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationTest.class);
    
    // Test configuration
    private static final int NUM_SERVERS = 3;
    private static final int NUM_CLIENTS = 3;
    private static final int BASE_PORT = 9500;
    private static final int MESSAGES_BEFORE_FAILURE = 100;
    
    private final List<ServerNode> servers = new ArrayList<>();
    private final List<ClientNode> clients = new ArrayList<>();
    
    public static void main(String[] args) {
        ReplicationTest test = new ReplicationTest();
        try {
            boolean result = test.runReplicationTest();
            System.out.println("\n==================================");
            System.out.println("LOG REPLICATION TEST RESULT: " + (result ? "PASSED" : "FAILED"));
            System.out.println("==================================\n");
            System.exit(result ? 0 : 1);
        } catch (Exception e) {
            logger.error("Error running replication test: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    /**
     * Run the log replication correctness test
     * 
     * @return true if logs are consistent after recovery, false otherwise
     */
    public boolean runReplicationTest() throws IOException, InterruptedException {
        try {
            // Setup test environment
            setupTestEnvironment();
            
            // Run the log replication test
            return testLogReplicationCorrectness();
        } finally {
            // Clean up
            cleanupTestEnvironment();
        }
    }
    
    /**
     * Initialize the test environment with multiple servers and clients
     */
    private void setupTestEnvironment() throws IOException, InterruptedException {
        logger.info("Setting up test environment with {} servers and {} clients", NUM_SERVERS, NUM_CLIENTS);
        
        // Create and start servers
        for (int i = 0; i < NUM_SERVERS; i++) {
            // Prepare peer server addresses
            List<String> peerAddresses = new ArrayList<>();
            for (int j = 0; j < NUM_SERVERS; j++) {
                if (j != i) {
                    peerAddresses.add("localhost:" + (BASE_PORT + j));
                }
            }
            
            // Enable Raft consensus
            System.setProperty("raft.enabled", "true");
            ServerNode server = new ServerNode("server" + (i + 1), BASE_PORT + i, peerAddresses);
            server.start();
            servers.add(server);
            logger.info("Started server{} on port {}", i + 1, BASE_PORT + i);
        }
        
        // Wait for servers to initialize and elect a leader
        Thread.sleep(5000);
        
        // Create and start clients
        for (int i = 0; i < NUM_CLIENTS; i++) {
            // Create all server addresses for failover
            List<String> serverAddresses = new ArrayList<>();
            for (int j = 1; j < NUM_SERVERS; j++) {
                serverAddresses.add("localhost:" + (BASE_PORT + j));
            }
            
            // Create client with connection to first server
            ClientNode client = new ClientNode("client" + (i + 1), "localhost", BASE_PORT, serverAddresses);
            client.start();
            clients.add(client);
            logger.info("Started client{} connected to server1", i + 1);
        }
        
        // Additional wait for all connections to establish
        Thread.sleep(2000);
    }
    
    /**
     * Test log replication correctness by killing the leader after a specific number of messages
     * and verifying that all followers have identical logs after recovery.
     * 
     * @return true if the logs are consistent after recovery, false otherwise
     */
    private boolean testLogReplicationCorrectness() throws InterruptedException {
        logger.info("Starting log replication correctness test");
        
        // Find the current leader
        ServerNode leader = null;
        for (ServerNode server : servers) {
            if (server.isLeader()) {
                leader = server;
                break;
            }
        }
        
        if (leader == null) {
            logger.warn("No leader found, skipping log replication test");
            return false;
        }
        
        logger.info("Found leader: {}", leader.getId());
        
        // Number of messages to send before killing the leader
        final int messagesToSend = MESSAGES_BEFORE_FAILURE;
        final CountDownLatch messageSentLatch = new CountDownLatch(messagesToSend);
        final List<String> sentMessageIds = Collections.synchronizedList(new ArrayList<>());
        
        // Send messages through the system
        logger.info("Sending {} messages before killing leader", messagesToSend);
        
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(3, clients.size()));
        
        // Get random client to send messages
        for (int i = 0; i < messagesToSend; i++) {
            final int messageIndex = i;
            executor.submit(() -> {
                try {
                    // Choose a random client as sender
                    ClientNode sender = clients.get(ThreadLocalRandom.current().nextInt(clients.size()));
                    
                    // Choose a different client as recipient
                    int recipientIndex = ThreadLocalRandom.current().nextInt(clients.size());
                    if (clients.get(recipientIndex).getId().equals(sender.getId())) {
                        recipientIndex = (recipientIndex + 1) % clients.size();
                    }
                    ClientNode recipient = clients.get(recipientIndex);
                    
                    // Create a message with a specific payload we can verify later
                    String content = "REPLICATION_TEST_" + messageIndex;
                    Message message = new Message(
                        sender.getId(),
                        recipient.getId(),
                        content,
                        MessageType.USER_MESSAGE
                    );
                    
                    // Track the message ID
                    sentMessageIds.add(message.getId().toString());
                    
                    // Send the message
                    sender.sendMessage(message);
                    
                    // Decrement latch
                    messageSentLatch.countDown();
                    
                    // Add small delay to avoid overwhelming the system
                    Thread.sleep(10);
                } catch (Exception e) {
                    logger.error("Error sending test message: {}", e.getMessage(), e);
                    messageSentLatch.countDown();
                }
            });
        }
        
        // Wait for all messages to be sent
        logger.info("Waiting for all test messages to be sent...");
        boolean allSent = messageSentLatch.await(30, TimeUnit.SECONDS);
        
        if (!allSent) {
            logger.warn("Not all messages were sent within timeout (sent {}/{})", 
                    messagesToSend - messageSentLatch.getCount(), messagesToSend);
        } else {
            logger.info("All {} messages sent successfully", messagesToSend);
        }
        
        // Short delay to allow some replication to happen
        logger.info("Waiting for initial replication...");
        Thread.sleep(2000);
        
        // Kill the leader (now that we've sent the messages)
        logger.info("Killing leader: {}", leader.getId());
        leader.stop();
        
        // Shut down the executor
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        
        // Wait for recovery and new leader election
        logger.info("Waiting for recovery and new leader election...");
        Thread.sleep(5000);
        
        // Verify new leader is elected
        int leaderCount = 0;
        ServerNode newLeader = null;
        
        for (ServerNode server : servers) {
            if (server != leader && server.isLeader()) {
                leaderCount++;
                newLeader = server;
                logger.info("New leader detected: {}", server.getId());
            }
        }
        
        if (newLeader == null) {
            logger.error("No new leader elected after killing leader, test failed");
            return false;
        } else if (leaderCount > 1) {
            logger.error("Multiple leaders detected ({}), violating safety property!", leaderCount);
            return false;
        } else {
            logger.info("LEADER ELECTION TEST PASSED: Exactly one new leader elected: {}", newLeader.getId());
        }
        
        // Wait a bit longer to ensure replication completes
        logger.info("Waiting for replication to complete...");
        Thread.sleep(2000);
        
        // Now verify that all surviving servers have the same log by checking
        // the message store on each server
        
        // Create a map to store message counts by server
        Map<String, Set<String>> messagesByServer = new HashMap<>();
        
        // Check logs on all surviving servers
        for (ServerNode server : servers) {
            if (server == leader) {
                continue; // Skip the killed leader
            }
            
            Set<String> serverMessages = getMessagesFromServer(server);
            messagesByServer.put(server.getId(), serverMessages);
            
            logger.info("Server {} has {} messages in log", server.getId(), serverMessages.size());
        }
        
        // Verify all servers have identical logs
        if (messagesByServer.size() < 2) {
            logger.warn("Not enough surviving servers to compare logs");
            return false;
        }
        
        // Get reference log from the first server
        String firstServerId = messagesByServer.keySet().iterator().next();
        Set<String> referenceLog = messagesByServer.get(firstServerId);
        
        logger.info("Using server {} as reference with {} messages", firstServerId, referenceLog.size());
        
        boolean allLogsIdentical = true;
        for (Map.Entry<String, Set<String>> entry : messagesByServer.entrySet()) {
            if (!entry.getKey().equals(firstServerId)) {
                if (!entry.getValue().equals(referenceLog)) {
                    logger.error("LOG INCONSISTENCY DETECTED! Server {} log differs from reference", 
                            entry.getKey());
                    
                    // Calculate log differences
                    Set<String> onlyInReference = new HashSet<>(referenceLog);
                    onlyInReference.removeAll(entry.getValue());
                    
                    Set<String> onlyInOther = new HashSet<>(entry.getValue());
                    onlyInOther.removeAll(referenceLog);
                    
                    logger.error("Messages only in reference: {}", onlyInReference.size());
                    logger.error("Messages only in {}: {}", entry.getKey(), onlyInOther.size());
                    
                    allLogsIdentical = false;
                } else {
                    logger.info("Server {} log matches reference - CONSISTENT", entry.getKey());
                }
            }
        }
        
        // Calculate how many of our sent messages are in the logs
        int messagesFound = 0;
        for (String messageId : sentMessageIds) {
            if (referenceLog.contains(messageId)) {
                messagesFound++;
            }
        }
        
        logger.info("Found {}/{} sent messages in the logs after recovery ({}%)", 
                messagesFound, sentMessageIds.size(), 
                sentMessageIds.isEmpty() ? 0 : (messagesFound * 100 / sentMessageIds.size()));
        
        // Final result
        if (allLogsIdentical) {
            logger.info("LOG REPLICATION TEST PASSED: All surviving servers have identical logs");
            return true;
        } else {
            logger.error("LOG REPLICATION TEST FAILED: Log inconsistencies detected");
            return false;
        }
    }
    
    /**
     * Get the set of message IDs from a server's message store.
     * Uses reflection to access the private message store.
     * 
     * @param server The server to extract messages from
     * @return Set of message IDs stored in the server
     */
    private Set<String> getMessagesFromServer(ServerNode server) {
        Set<String> messageIds = new HashSet<>();
        
        try {
            // Access the messageStore field using reflection
            java.lang.reflect.Field storeField = server.getClass().getDeclaredField("messageStore");
            storeField.setAccessible(true);
            Object messageStore = storeField.get(server);
            
            // Now call the getAllMessages method on the store
            java.lang.reflect.Method getAllMessagesMethod = 
                    messageStore.getClass().getMethod("getAllMessages");
            
            @SuppressWarnings("unchecked")
            List<Message> messages = (List<Message>) getAllMessagesMethod.invoke(messageStore);
            
            // Extract IDs
            for (Message msg : messages) {
                if (msg.getType() == MessageType.USER_MESSAGE) {
                    messageIds.add(msg.getId().toString());
                }
            }
        } catch (Exception e) {
            logger.error("Error extracting messages from server {}: {}", 
                    server.getId(), e.getMessage(), e);
        }
        
        return messageIds;
    }
    
    /**
     * Cleanup test environment
     */
    private void cleanupTestEnvironment() {
        logger.info("Cleaning up test environment");
        
        // Stop all clients
        for (ClientNode client : clients) {
            try {
                client.stop();
            } catch (Exception e) {
                logger.error("Error stopping client: {}", e.getMessage());
            }
        }
        
        // Stop all servers
        for (ServerNode server : servers) {
            try {
                server.stop();
            } catch (Exception e) {
                logger.error("Error stopping server: {}", e.getMessage());
            }
        }
        
        clients.clear();
        servers.clear();
    }
} 