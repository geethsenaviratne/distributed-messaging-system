package com.ds.messaging;

import com.ds.messaging.client.ClientNode;
import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performance test for consensus algorithm under high message transmission rates.
 * This test creates a multi-server cluster using Raft consensus and measures:
 * 1. Message throughput (messages/second)
 * 2. Latency (time from send to delivery)
 * 3. Success rate (percentage of messages successfully delivered)
 * 4. Leadership transition time under load
 */
public class ConsensusPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(ConsensusPerformanceTest.class);
    
    // Test configuration
    private static final int NUM_SERVERS = 3;
    private static final int NUM_CLIENTS = 5;
    private static final int BASE_PORT = 9500;
    private static final int MESSAGE_SIZE_BYTES = 1024; // 1KB
    private static final int[] MESSAGE_RATES = {100, 500, 1000, 2000, 5000}; // messages per second
    private static final int TEST_DURATION_SECONDS = 30;
    private static final int WARMUP_DURATION_SECONDS = 10;
    
    private final List<ServerNode> servers = new ArrayList<>();
    private final List<ClientNode> clients = new ArrayList<>();
    private final ConcurrentMap<String, MessageStats> messageStats = new ConcurrentHashMap<>();
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    
    // Stats tracking
    private static class MessageStats {
        final long sendTimeMs;
        volatile long receiveTimeMs;
        
        MessageStats(long sendTimeMs) {
            this.sendTimeMs = sendTimeMs;
        }
    }
    
    private static class TestResults {
        final int messageRate;
        final int sentCount;
        final int receivedCount;
        final double avgLatencyMs;
        final double successRate;
        final double p95LatencyMs;
        final double p99LatencyMs;
        final long leaderTransitionTimeMs;
        
        TestResults(int messageRate, int sentCount, int receivedCount, double avgLatencyMs, 
                   double successRate, double p95LatencyMs, double p99LatencyMs, long leaderTransitionTimeMs) {
            this.messageRate = messageRate;
            this.sentCount = sentCount;
            this.receivedCount = receivedCount;
            this.avgLatencyMs = avgLatencyMs;
            this.successRate = successRate;
            this.p95LatencyMs = p95LatencyMs;
            this.p99LatencyMs = p99LatencyMs;
            this.leaderTransitionTimeMs = leaderTransitionTimeMs;
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
            
            // Create server node (enable Raft consensus)
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
            
            // Register message handler to track received messages
            final int clientIndex = i;
            client.registerMessageHandler((message, sourceNodeId) -> {
                if (message.getType() == MessageType.USER_MESSAGE) {
                    // Record receive time and calculate latency
                    MessageStats stats = messageStats.get(message.getId().toString());
                    if (stats != null) {
                        stats.receiveTimeMs = System.currentTimeMillis();
                    }
                    return true;
                }
                return false;
            });
            
            client.start();
            clients.add(client);
            logger.info("Started client{} connected to server1", i + 1);
        }
        
        // Additional wait for all connections to establish
        Thread.sleep(2000);
    }
    
    /**
     * Run performance test with a specific message rate
     */
    private TestResults runPerformanceTest(int messagesPerSecond) throws InterruptedException {
        logger.info("Starting performance test at {} messages/second", messagesPerSecond);
        
        // Reset test statistics
        messageStats.clear();
        
        // Calculate sending interval
        long sendIntervalNanos = TimeUnit.SECONDS.toNanos(1) / messagesPerSecond;
        
        // Create random payload of specified size
        byte[] payload = new byte[MESSAGE_SIZE_BYTES];
        ThreadLocalRandom.current().nextBytes(payload);
        String messageContent = new String(payload);
        
        AtomicInteger messagesSent = new AtomicInteger(0);
        AtomicInteger messagesReceived = new AtomicInteger(0);
        
        // Create executor for sending messages
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(NUM_CLIENTS);
        
        // Schedule message sending tasks
        for (int i = 0; i < NUM_CLIENTS; i++) {
            final int clientIndex = i;
            final ClientNode client = clients.get(clientIndex);
            
            // Calculate messages per second per client
            int clientMsgRate = messagesPerSecond / NUM_CLIENTS;
            if (i < messagesPerSecond % NUM_CLIENTS) {
                clientMsgRate++; // Distribute remainder
            }
            
            if (clientMsgRate == 0) continue;
            
            final long clientSendIntervalNanos = TimeUnit.SECONDS.toNanos(1) / clientMsgRate;
            
            executor.scheduleAtFixedRate(() -> {
                try {
                    // Create unique message
                    UUID messageId = UUID.randomUUID();
                    String recipient = "client" + (ThreadLocalRandom.current().nextInt(NUM_CLIENTS) + 1);
                    
                    // Don't send to self
                    if (recipient.equals("client" + (clientIndex + 1))) {
                        recipient = "client" + (clientIndex % NUM_CLIENTS + 1);
                    }
                    
                    Message message = new Message(client.getId(), recipient, messageContent, MessageType.USER_MESSAGE);
                    
                    // Store send time
                    messageStats.put(message.getId().toString(), new MessageStats(System.currentTimeMillis()));
                    
                    // Send message
                    client.sendMessage(message);
                    messagesSent.incrementAndGet();
                } catch (Exception e) {
                    logger.error("Error sending message: {}", e.getMessage(), e);
                }
            }, 0, clientSendIntervalNanos, TimeUnit.NANOSECONDS);
        }
        
        // Warmup period
        logger.info("Warmup period: {} seconds", WARMUP_DURATION_SECONDS);
        Thread.sleep(TimeUnit.SECONDS.toMillis(WARMUP_DURATION_SECONDS));
        
        // Reset stats after warmup
        messageStats.clear();
        messagesSent.set(0);
        messagesReceived.set(0);
        
        // Start timed test
        logger.info("Starting timed test for {} seconds", TEST_DURATION_SECONDS);
        long testStartTime = System.currentTimeMillis();
        
        // Run test for specified duration
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_DURATION_SECONDS));
        
        // Stop sending messages
        executor.shutdownNow();
        
        // Calculate test results
        long testEndTime = System.currentTimeMillis();
        long totalTestTimeMs = testEndTime - testStartTime;
        
        int sent = messagesSent.get();
        int received = 0;
        
        // Calculate latencies
        List<Long> latencies = new ArrayList<>();
        for (MessageStats stats : messageStats.values()) {
            if (stats.receiveTimeMs > 0) {
                received++;
                latencies.add(stats.receiveTimeMs - stats.sendTimeMs);
            }
        }
        
        // Sort latencies for percentile calculation
        latencies.sort(Long::compare);
        
        double avgLatency = latencies.stream().mapToLong(l -> l).average().orElse(0);
        double p95Latency = latencies.isEmpty() ? 0 : latencies.get((int)(latencies.size() * 0.95));
        double p99Latency = latencies.isEmpty() ? 0 : latencies.get((int)(latencies.size() * 0.99));
        double successRate = sent > 0 ? (double)received / sent * 100 : 0;
        
        // Simulate leader transition (stop the leader and measure time to elect new leader)
        long leaderTransitionTime = measureLeaderTransition();
        
        return new TestResults(
            messagesPerSecond,
            sent,
            received,
            avgLatency,
            successRate,
            p95Latency,
            p99Latency,
            leaderTransitionTime
        );
    }
    
    /**
     * Measure time for leader transition by stopping the current leader and measuring
     * how long it takes for a new leader to be elected and become operational
     */
    private long measureLeaderTransition() throws InterruptedException {
        // Identify the current leader
        ServerNode leader = null;
        for (ServerNode server : servers) {
            if (server.isLeader()) {
                leader = server;
                break;
            }
        }
        
        if (leader == null) {
            logger.warn("No leader found, skipping leader transition test");
            return -1;
        }
        
        logger.info("Found leader: {}", leader.getId());
        
        // Setup a test message
        final CountDownLatch messageLatch = new CountDownLatch(1);
        final AtomicInteger receivedCount = new AtomicInteger(0);
        
        // Register a handler on all clients for this test
        for (ClientNode client : clients) {
            client.registerMessageHandler((message, sourceNodeId) -> {
                if (message.getContent().equals("LEADER_TRANSITION_TEST") && 
                    message.getType() == MessageType.USER_MESSAGE) {
                    receivedCount.incrementAndGet();
                    messageLatch.countDown();
                    return true;
                }
                return false;
            });
        }
        
        // Stop the leader
        logger.info("Stopping leader: {}", leader.getId());
        long startTime = System.currentTimeMillis();
        leader.stop();
        
        // Wait for the system to stabilize (new leader election)
        Thread.sleep(2000);
        
        // Try to send a message through the new leader
        if (!clients.isEmpty()) {
            ClientNode testClient = clients.get(0);
            ClientNode recipient = clients.get(clients.size() > 1 ? 1 : 0);
            
            Message testMessage = new Message(
                testClient.getId(), 
                recipient.getId(),
                "LEADER_TRANSITION_TEST", 
                MessageType.USER_MESSAGE
            );
            
            testClient.sendMessage(testMessage);
            
            // Wait for the message to be delivered
            boolean delivered = messageLatch.await(10, TimeUnit.SECONDS);
            
            long endTime = System.currentTimeMillis();
            long transitionTime = endTime - startTime;
            
            if (delivered) {
                logger.info("New leader operational after {}ms", transitionTime);
                return transitionTime;
            } else {
                logger.warn("Message not delivered after leader transition");
                return -1;
            }
        }
        
        return -1;
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
    
    /**
     * Run the complete benchmark
     */
    public void runBenchmark() throws IOException, InterruptedException {
        try {
            // Setup test environment
            setupTestEnvironment();
            
            List<TestResults> results = new ArrayList<>();
            
            // Run tests at different message rates
            for (int rate : MESSAGE_RATES) {
                TestResults testResults = runPerformanceTest(rate);
                results.add(testResults);
                
                // Log results
                logger.info("Results for {} msg/sec:", rate);
                logger.info("  Messages sent: {}", testResults.sentCount);
                logger.info("  Messages received: {}", testResults.receivedCount);
                logger.info("  Success rate: {:.2f}%", testResults.successRate);
                logger.info("  Average latency: {:.2f}ms", testResults.avgLatencyMs);
                logger.info("  95th percentile latency: {:.2f}ms", testResults.p95LatencyMs);
                logger.info("  99th percentile latency: {:.2f}ms", testResults.p99LatencyMs);
                logger.info("  Leader transition time: {}ms", testResults.leaderTransitionTimeMs);
                
                // Break if success rate drops below 90%
                if (testResults.successRate < 90.0) {
                    logger.info("Success rate below 90%, stopping test");
                    break;
                }
                
                // Short pause between tests
                Thread.sleep(5000);
            }
            
            // Print summary
            logger.info("Performance Test Summary:");
            logger.info("=========================");
            logger.info("Rate (msg/s) | Success (%) | Avg Latency (ms) | 95p Latency (ms) | 99p Latency (ms) | Leader Trans (ms)");
            logger.info("------------ | ----------- | ---------------- | ---------------- | ---------------- | ----------------");
            
            for (TestResults r : results) {
                logger.info("{} | {:.2f} | {:.2f} | {:.2f} | {:.2f} | {}", 
                    r.messageRate, r.successRate, r.avgLatencyMs, r.p95LatencyMs, r.p99LatencyMs, r.leaderTransitionTimeMs);
            }
            
        } finally {
            // Clean up
            cleanupTestEnvironment();
        }
    }
    
    public static void main(String[] args) {
        ConsensusPerformanceTest test = new ConsensusPerformanceTest();
        try {
            test.runBenchmark();
        } catch (Exception e) {
            logger.error("Error running benchmark: {}", e.getMessage(), e);
        }
    }
} 