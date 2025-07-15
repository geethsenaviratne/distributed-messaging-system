package com.ds.messaging.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Main class for starting a server node.
 */
public class ServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ServerMain.class);
    
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: ServerMain <serverID> <port> [peerServer1] [peerServer2] ...");
            System.out.println("Example: ServerMain server1 8000 localhost:8001 localhost:8002");
            return;
        }
        
        String serverId = args[0];
        int port = Integer.parseInt(args[1]);
        
        List<String> peerServers = new ArrayList<>();
        if (args.length > 2) {
            peerServers.addAll(Arrays.asList(args).subList(2, args.length));
        }
        
        try {
            ServerNode server = new ServerNode(serverId, port, peerServers);
            server.start();
            
            // Add a shutdown hook to gracefully stop the server
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
            
            System.out.println("=================================================");
            System.out.println("      DISTRIBUTED MESSAGING SYSTEM - SERVER      ");
            System.out.println("=================================================");
            System.out.println("Server started with ID: " + serverId + " on port: " + port);
            System.out.println("Connected to peer servers: " + peerServers);
            System.out.println("=================================================");
            
            // Start the menu-driven interface
            Scanner scanner = new Scanner(System.in);
            boolean running = true;
            
            while (running) {
                displayMenu();
                
                String choice = scanner.nextLine().trim();
                
                switch (choice) {
                    case "1":
                        // Check server status
                        System.out.println("Server status: " + server.getStatus());
                        break;
                        
                    case "2":
                        // List connected nodes
                        List<String> connectedNodes = server.getConnectedNodes();
                        System.out.println("Connected nodes (" + connectedNodes.size() + "):");
                        if (connectedNodes.isEmpty()) {
                            System.out.println("  No nodes connected");
                        } else {
                            for (String node : connectedNodes) {
                                System.out.println("  - " + node);
                            }
                        }
                        break;
                        
                    case "3":
                        // Simulate temporary degraded mode
                        System.out.println("Simulating temporary degraded mode for 10 seconds...");
                        // Save the current status and set to degraded
                        final var originalStatus = server.getStatus();
                        server.simulateDegradedMode();
                        
                        // Start a thread to restore status after 10 seconds
                        new Thread(() -> {
                            try {
                                Thread.sleep(10000);
                                server.restoreMode(originalStatus);
                                System.out.println("\nServer restored to normal operation.");
                                displayMenu();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }).start();
                        break;
                        
                    case "4":
                        // Exit
                        running = false;
                        System.out.println("Exiting...");
                        break;
                        
                    default:
                        System.out.println("Invalid choice. Please try again.");
                        break;
                }
                
                // Add a small delay to prevent overwhelming console output
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            server.stop();
            System.out.println("Server stopped");
            
        } catch (IOException e) {
            logger.error("Error starting server: {}", e.getMessage(), e);
            System.err.println("Error starting server: " + e.getMessage());
        }
    }
    
    private static void displayMenu() {
        System.out.println("\n-------------------------------------------------");
        System.out.println("               AVAILABLE ACTIONS                 ");
        System.out.println("-------------------------------------------------");
        System.out.println("1. Check server status");
        System.out.println("2. List connected nodes");
        System.out.println("3. Simulate temporary degraded mode (10 seconds)");
        System.out.println("4. Exit");
        System.out.println("-------------------------------------------------");
        System.out.print("Enter your choice (1-4): ");
    }
} 