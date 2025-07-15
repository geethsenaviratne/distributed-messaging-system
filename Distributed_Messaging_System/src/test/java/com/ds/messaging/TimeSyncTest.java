package com.ds.messaging;

import com.ds.messaging.client.ClientNode;
import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.common.TimeSyncRequestMessage;
import com.ds.messaging.common.TimeSyncResponseMessage;
import com.ds.messaging.server.ServerNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test to demonstrate NTP-like time synchronization
 */
public class TimeSyncTest {
    private static final Logger logger = LoggerFactory.getLogger(TimeSyncTest.class);
    
    private ServerNode server1;
    private ServerNode server2;
    private ClientNode client1;
    private ClientNode client2;
    
    @Before
    public void setup() throws IOException, InterruptedException {
        // Create servers
        server1 = new ServerNode("server1", 9100, Arrays.asList("localhost:9101"));
        server2 = new ServerNode("server2", 9101, Arrays.asList("localhost:9100"));
        
        // Start servers
        server1.start();
        server2.start();
        
        // Wait for servers to initialize
        Thread.sleep(2000);
        
        // Create clients
        client1 = new ClientNode("client1", "localhost", 9100, Arrays.asList("localhost:9101"));
        client2 = new ClientNode("client2", "localhost", 9101, Arrays.asList("localhost:9100"));
        
        // Start clients
        client1.start();
        client2.start();
        
        // Wait for connections to establish
        Thread.sleep(2000);
    }
    
    @After
    public void teardown() {
        if (client1 != null) client1.stop();
        if (client2 != null) client2.stop();
        if (server1 != null) server1.stop();
        if (server2 != null) server2.stop();
    }
    
    @Test
    public void testTimeSynchronization() throws InterruptedException {
        // Let the time sync process run for a few seconds
        logger.info("Waiting for time synchronization to stabilize...");
        Thread.sleep(5000);
        
        // Check if clients have clock offsets
        long client1Offset = client1.getClockOffset();
        long client2Offset = client2.getClockOffset();
        
        logger.info("Client 1 time offset: {} ms", client1Offset);
        logger.info("Client 2 time offset: {} ms", client2Offset);
        
        // Get current time from each node
        long server1Time = server1.getSynchronizedTime();
        long server2Time = server2.getSynchronizedTime();
        long client1Time = client1.getSynchronizedTime();
        long client2Time = client2.getSynchronizedTime();
        
        logger.info("Server 1 time: {}", server1Time);
        logger.info("Server 2 time: {}", server2Time);
        logger.info("Client 1 time: {}", client1Time);
        logger.info("Client 2 time: {}", client2Time);
        
        // Calculate max time difference between nodes
        long maxDiff = Math.max(
            Math.max(Math.abs(server1Time - server2Time), Math.abs(server1Time - client1Time)),
            Math.max(Math.abs(server1Time - client2Time), Math.abs(server2Time - client1Time))
        );
        maxDiff = Math.max(maxDiff, Math.abs(server2Time - client2Time));
        maxDiff = Math.max(maxDiff, Math.abs(client1Time - client2Time));
        
        logger.info("Maximum time difference between nodes: {} ms", maxDiff);
        
        // The time difference should be within a reasonable tolerance
        // For this test, we'll use a larger tolerance because virtual servers run on same machine
        // and we're not introducing real clock skew
        assertTrue("Time difference should be within tolerance", maxDiff < 1000);
        
        // Send messages with timestamps between nodes
        Message message = new Message("client1", "client2", "Test message with timestamp", MessageType.USER_MESSAGE);
        client1.sendMessage(message);
        
        // Wait for message to be processed
        Thread.sleep(1000);
        
        logger.info("Time synchronization test completed successfully");
    }
} 