package com.ds.messaging.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to simulate failures in the distributed system.
 * This is used for testing fault tolerance mechanisms.
 */
public class TestFailureSimulator {
    private static final Logger logger = LoggerFactory.getLogger(TestFailureSimulator.class);
    private static final Random random = new Random();
    
    private final List<String> servers;
    private final ScheduledExecutorService executor;
    
    /**
     * Creates a new failure simulator.
     * 
     * @param servers List of servers in format "host:port"
     */
    public TestFailureSimulator(List<String> servers) {
        this.servers = new ArrayList<>(servers);
        this.executor = Executors.newScheduledThreadPool(1);
    }
    
    /**
     * Start simulating network partitions between servers.
     * 
     * @param intervalSeconds Time between simulated failures
     * @param durationSeconds Duration of each simulated failure
     */
    public void startNetworkPartitionSimulation(int intervalSeconds, int durationSeconds) {
        executor.scheduleAtFixedRate(() -> {
            if (servers.size() < 2) {
                logger.warn("Not enough servers to simulate network partition");
                return;
            }
            
            // Select two random servers to disconnect
            int index1 = random.nextInt(servers.size());
            int index2;
            do {
                index2 = random.nextInt(servers.size());
            } while (index2 == index1);
            
            String server1 = servers.get(index1);
            String server2 = servers.get(index2);
            
            logger.info("Simulating network partition between {} and {}", server1, server2);
            
            // In a real implementation, we would use network traffic control tools like tc 
            // or iptables to simulate actual network partitions. For this prototype, we can't
            // actually create network partitions, so we'll just log the simulation.
            
            // Schedule reconnection after the failure duration
            executor.schedule(() -> {
                logger.info("Network partition between {} and {} has been resolved", server1, server2);
            }, durationSeconds, TimeUnit.SECONDS);
            
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Start simulating random server crashes.
     * 
     * @param intervalSeconds Time between simulated crashes
     * @param durationSeconds Duration of each simulated crash
     */
    public void startServerCrashSimulation(int intervalSeconds, int durationSeconds) {
        executor.scheduleAtFixedRate(() -> {
            if (servers.isEmpty()) {
                logger.warn("No servers to simulate crash");
                return;
            }
            
            // Select a random server to crash
            int index = random.nextInt(servers.size());
            String server = servers.get(index);
            
            String[] parts = server.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            
            logger.info("Simulating crash of server {}", server);
            
            try {
                // Try to connect to the server and then close it immediately
                // This isn't actually crashing the server, but in a real implementation
                // we would use SSH or some other mechanism to actually stop the server process
                Socket socket = new Socket(host, port);
                socket.close();
                
                // Schedule server recovery
                executor.schedule(() -> {
                    logger.info("Server {} has been restarted", server);
                }, durationSeconds, TimeUnit.SECONDS);
                
            } catch (IOException e) {
                logger.error("Failed to connect to server {}: {}", server, e.getMessage());
            }
            
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Stop all failure simulations.
     */
    public void stop() {
        executor.shutdownNow();
        logger.info("Failure simulation stopped");
    }
    
    /**
     * Main method for standalone testing.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: TestFailureSimulator <server1> <server2> ...");
            System.out.println("Example: TestFailureSimulator localhost:8000 localhost:8001 localhost:8002");
            return;
        }
        
        List<String> servers = new ArrayList<>();
        for (String server : args) {
            servers.add(server);
        }
        
        TestFailureSimulator simulator = new TestFailureSimulator(servers);
        
        // Start network partition simulation (every 20 seconds, lasting 10 seconds)
        simulator.startNetworkPartitionSimulation(20, 10);
        
        // Start server crash simulation (every 30 seconds, lasting 15 seconds)
        simulator.startServerCrashSimulation(30, 15);
        
        System.out.println("Failure simulator started. Press Enter to stop...");
        
        try {
            System.in.read();
        } catch (IOException e) {
            // Ignore
        }
        
        simulator.stop();
    }
} 