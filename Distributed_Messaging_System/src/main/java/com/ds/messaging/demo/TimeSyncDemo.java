package com.ds.messaging.demo;

import com.ds.messaging.client.ClientNode;
import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Demonstration of NTP-like time synchronization with simulated clock skew.
 * This demo creates multiple servers and clients with different simulated clock offsets
 * and shows how the system synchronizes their clocks.
 */
public class TimeSyncDemo {
    private static final Logger logger = LoggerFactory.getLogger(TimeSyncDemo.class);
    
    private static final int SERVER1_PORT = 9200;
    private static final int SERVER2_PORT = 9201;
    private static final int SERVER3_PORT = 9202;
    
    // Simulated clock offsets (in milliseconds)
    private static final long SERVER1_OFFSET = 0;     // Reference server
    private static final long SERVER2_OFFSET = 2000;  // 2 seconds ahead
    private static final long SERVER3_OFFSET = -1500; // 1.5 seconds behind
    private static final long CLIENT1_OFFSET = 5000;  // 5 seconds ahead
    private static final long CLIENT2_OFFSET = -3000; // 3 seconds behind
    
    private static ServerNode server1;
    private static ServerNode server2;
    private static ServerNode server3;
    private static ClientNode client1;
    private static ClientNode client2;
    
    // Custom clock for simulation
    private static class SimulatedClock {
        private final long offset;
        
        public SimulatedClock(long offsetMillis) {
            this.offset = offsetMillis;
        }
        
        public long currentTimeMillis() {
            return System.currentTimeMillis() + offset;
        }
    }
    
    public static void main(String[] args) {
        try {
            // Create servers with simulated clock offsets
            logger.info("Creating servers with simulated clock offsets");
            logger.info("Server 1: reference clock (offset = 0ms)");
            logger.info("Server 2: +2000ms offset");
            logger.info("Server 3: -1500ms offset");
            
            server1 = new ServerNode("server1", SERVER1_PORT, 
                    Arrays.asList("localhost:" + SERVER2_PORT, "localhost:" + SERVER3_PORT));
            server2 = new ServerNode("server2", SERVER2_PORT, 
                    Arrays.asList("localhost:" + SERVER1_PORT, "localhost:" + SERVER3_PORT));
            server3 = new ServerNode("server3", SERVER3_PORT, 
                    Arrays.asList("localhost:" + SERVER1_PORT, "localhost:" + SERVER2_PORT));
            
            // Start servers
            server1.start();
            server2.start();
            server3.start();
            
            logger.info("All servers started");
            Thread.sleep(2000);
            
            // Create clients with simulated clock offsets
            logger.info("Creating clients with simulated clock offsets");
            logger.info("Client 1: +5000ms offset");
            logger.info("Client 2: -3000ms offset");
            
            client1 = new ClientNode("client1", "localhost", SERVER1_PORT, 
                    Arrays.asList("localhost:" + SERVER2_PORT, "localhost:" + SERVER3_PORT));
            client2 = new ClientNode("client2", "localhost", SERVER2_PORT, 
                    Arrays.asList("localhost:" + SERVER1_PORT, "localhost:" + SERVER3_PORT));
            
            // Add message handler to client2 to show received timestamps
            final CountDownLatch messageLatch = new CountDownLatch(1);
            
            client2.registerMessageHandler(new MessageHandler() {
                @Override
                public boolean handleMessage(Message message, String sourceNodeId) {
                    if (message.getType() == MessageType.USER_MESSAGE) {
                        logger.info("Client 2 received message: {}", message.getContent());
                        logger.info("Original timestamp: {}", message.getTimestampInstant());
                        messageLatch.countDown();
                        return true;
                    }
                    return false;
                }
            });
            
            // Start clients
            client1.start();
            client2.start();
            
            logger.info("All clients started");
            
            // Wait for time sync to stabilize
            logger.info("Waiting for time synchronization to stabilize...");
            Thread.sleep(10000);
            
            // Display time information for all nodes
            displayTimeInfo("Server 1", server1);
            displayTimeInfo("Server 2", server2);
            displayTimeInfo("Server 3", server3);
            displayTimeInfo("Client 1", client1);
            displayTimeInfo("Client 2", client2);
            
            // Send a timestamped message from client1 to client2
            logger.info("Sending a timestamped message from Client 1 to Client 2");
            Message message = new Message("client1", "client2", "Test message with timestamp", MessageType.USER_MESSAGE);
            client1.sendMessage(message);
            
            // Wait for the message to be delivered
            if (messageLatch.await(5, TimeUnit.SECONDS)) {
                logger.info("Message successfully delivered");
            } else {
                logger.warn("Timeout waiting for message delivery");
            }
            
            // Display time information again after communication
            logger.info("Time information after message exchange:");
            displayTimeInfo("Server 1", server1);
            displayTimeInfo("Server 2", server2);
            displayTimeInfo("Server 3", server3);
            displayTimeInfo("Client 1", client1);
            displayTimeInfo("Client 2", client2);
            
            // Calculate the average synchronized time across all nodes
            long avgTime = (
                server1.getSynchronizedTime() +
                server2.getSynchronizedTime() +
                server3.getSynchronizedTime() +
                client1.getSynchronizedTime() +
                client2.getSynchronizedTime()
            ) / 5;
            
            logger.info("Average synchronized time across all nodes: {}", avgTime);
            
            // Calculate max deviation from the average
            long maxDeviation = Math.max(
                Math.abs(server1.getSynchronizedTime() - avgTime),
                Math.max(
                    Math.abs(server2.getSynchronizedTime() - avgTime),
                    Math.max(
                        Math.abs(server3.getSynchronizedTime() - avgTime),
                        Math.max(
                            Math.abs(client1.getSynchronizedTime() - avgTime),
                            Math.abs(client2.getSynchronizedTime() - avgTime)
                        )
                    )
                )
            );
            
            logger.info("Maximum time deviation from average: {} ms", maxDeviation);
            logger.info("Time synchronization demo completed.");
            
        } catch (Exception e) {
            logger.error("Error in time sync demo: {}", e.getMessage(), e);
        } finally {
            shutdown();
        }
    }
    
    private static void displayTimeInfo(String nodeName, ClientNode node) {
        logger.info("{}: System time={}, Synchronized time={}, Offset={}ms", 
                nodeName, 
                System.currentTimeMillis(),
                node.getSynchronizedTime(),
                node.getClockOffset());
    }
    
    private static void displayTimeInfo(String nodeName, ServerNode node) {
        logger.info("{}: System time={}, Synchronized time={}, Offset={}ms", 
                nodeName, 
                System.currentTimeMillis(),
                node.getSynchronizedTime(),
                node.getClockOffset());
    }
    
    private static void shutdown() {
        logger.info("Shutting down nodes...");
        
        try {
            if (client1 != null) client1.stop();
            if (client2 != null) client2.stop();
            if (server1 != null) server1.stop();
            if (server2 != null) server2.stop();
            if (server3 != null) server3.stop();
            
            logger.info("All nodes stopped");
        } catch (Exception e) {
            logger.error("Error during shutdown: {}", e.getMessage(), e);
        }
    }
} 