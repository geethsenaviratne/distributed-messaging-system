package com.ds.messaging.demo;

import com.ds.messaging.client.ClientNode;
import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.server.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runner class to start a single server or client node.
 * Used for launching individual nodes in separate terminal windows.
 */
public class SingleNodeRunner {
    private static final Logger logger = LoggerFactory.getLogger(SingleNodeRunner.class);
    private static final Queue<Message> receivedMessages = new ConcurrentLinkedQueue<>();
    private static boolean newMessageReceived = false;

    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                System.out.println("Usage: SingleNodeRunner <nodeType> [args...]");
                System.out.println("  For server: SingleNodeRunner server <serverId> <port> <peerServers>");
                System.out.println("  For client: SingleNodeRunner client <clientId> <clientPort> <serverHost> <serverPort> <backupServers>");
                return;
            }

            String nodeType = args[0];
            
            if ("server".equalsIgnoreCase(nodeType)) {
                runServer(args);
            } else if ("client".equalsIgnoreCase(nodeType)) {
                runClient(args);
            } else {
                System.out.println("Unknown node type: " + nodeType);
                System.out.println("Valid types are 'server' or 'client'");
            }
        } catch (Exception e) {
            logger.error("Error running node: {}", e.getMessage(), e);
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runServer(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: SingleNodeRunner server <serverId> <port> <peerServers>");
            System.out.println("  <serverId>: Unique identifier for this server");
            System.out.println("  <port>: Port number to listen on");
            System.out.println("  <peerServers>: Comma-separated list of peer servers (host:port)");
            return;
        }

        String serverId = args[1];
        int port = Integer.parseInt(args[2]);
        List<String> peerServers = args.length > 3 ? 
                                   Arrays.asList(args[3].split(",")) : 
                                   Arrays.asList();

        System.out.println("Starting server " + serverId + " on port " + port);
        System.out.println("Peer servers: " + peerServers);

        // Create and start the server
        ServerNode server = new ServerNode(serverId, port, peerServers);
        server.start();

        System.out.println("\nServer " + serverId + " is now running");
        System.out.println("Press 'q' and Enter to stop the server");

        // Wait for user to stop the server
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String input;
        while ((input = reader.readLine()) != null) {
            if (input.equalsIgnoreCase("q")) {
                break;
            }
            System.out.println("Press 'q' and Enter to stop the server");
        }

        System.out.println("Stopping server...");
        server.stop();
        System.out.println("Server stopped");
    }

    private static void runClient(String[] args) throws IOException {
        if (args.length < 5) {
            System.out.println("Usage: SingleNodeRunner client <clientId> <clientPort> <serverHost> <serverPort> <backupServers>");
            System.out.println("  <clientId>: Unique identifier for this client");
            System.out.println("  <clientPort>: Local port for this client");
            System.out.println("  <serverHost>: Host of the primary server");
            System.out.println("  <serverPort>: Port of the primary server");
            System.out.println("  <backupServers>: Comma-separated list of backup servers (host:port)");
            return;
        }

        String clientId = args[1];
        int clientPort = Integer.parseInt(args[2]);
        String serverHost = args[3];
        int serverPort = Integer.parseInt(args[4]);
        List<String> backupServers = args.length > 5 ? 
                                    Arrays.asList(args[5].split(",")) : 
                                    Arrays.asList();

        printClientHeader(clientId, clientPort, serverHost, serverPort);

        // Create and start the client
        ClientNode client = new ClientNode(clientId, serverHost, serverPort, backupServers);
        
        // Add message handler to store received messages
        client.registerMessageHandler(new MessageHandler() {
            @Override
            public boolean handleMessage(Message message, String sourceNodeId) {
                if (message.getType() == MessageType.USER_MESSAGE) {
                    receivedMessages.add(message);
                    newMessageReceived = true;
                    System.out.println("\n[NEW MESSAGE] You have received a new message!");
                    System.out.print("\nEnter your choice (1-4): ");
                    return true;
                }
                return false;
            }
        });
        
        client.start();

        // Main menu loop
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        boolean running = true;
        
        while (running) {
            printClientMenu(clientId, newMessageReceived ? receivedMessages.size() : 0);
            System.out.print("\nEnter your choice (1-4): ");
            
            String input = reader.readLine();
            
            try {
                int choice = Integer.parseInt(input);
                switch (choice) {
                    case 1: // Send message
                        sendMessage(reader, client, clientId);
                        break;
                    case 2: // View received messages
                        viewReceivedMessages();
                        break;
                    case 3: // View system info
                        viewSystemInfo(client);
                        break;
                    case 4: // Exit
                        running = false;
                        break;
                    default:
                        System.out.println("Invalid choice. Please enter a number between 1 and 4.");
                        break;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a number.");
            }
            
            // Reset the new message flag after user has seen the menu
            newMessageReceived = false;
            
            // Pause before showing menu again
            if (running) {
                System.out.println("\nPress Enter to continue...");
                reader.readLine();
            }
        }

        System.out.println("Stopping client...");
        client.stop();
        System.out.println("Client stopped");
    }
    
    private static void printClientHeader(String clientId, int clientPort, String serverHost, int serverPort) {
        
        System.out.println("                  DISTRIBUTED MESSAGING SYSTEM                   ");
        System.out.println("-----------------------------------------------------------------");
        System.out.println("Client ID: " + clientId);
        System.out.println("Client Port: " + clientPort);
        System.out.println("Server: " + serverHost + ":" + serverPort);
       
    }
    
    private static void printClientMenu(String clientId, int unreadCount) {
       
        System.out.println("                         CLIENT MENU                            ");
        System.out.println("-----------------------------------------------------------------");
        System.out.println("  1. Send a message");
        
        if (unreadCount > 0) {
            System.out.println("  2. View received messages (" + unreadCount + " new)");
        } else {
            System.out.println("  2. View received messages");
        }
        
        System.out.println("  3. View system information");
        System.out.println("  4. Exit");
        System.out.println("-----------------------------------------------------------------");
    }
    
    private static void sendMessage(BufferedReader reader, ClientNode client, String clientId) throws IOException {
        System.out.println("\n-----------------------------------------------------------------");
        System.out.println("                        SEND MESSAGE                            ");
        System.out.println("-----------------------------------------------------------------");
        
        System.out.print("Enter recipient ID: ");
        String recipient = reader.readLine().trim();
        
        if (recipient.isEmpty()) {
            System.out.println("Recipient cannot be empty. Message not sent.");
            return;
        }
        
        System.out.print("Enter message: ");
        String content = reader.readLine();
        
        if (content.isEmpty()) {
            System.out.println("Message cannot be empty. Message not sent.");
            return;
        }
        
        Message message = new Message(clientId, recipient, content, MessageType.USER_MESSAGE);
        
        System.out.println("Sending message to " + recipient + "...");
        CompletableFuture<Boolean> sendFuture = client.sendMessageWithAck(message);
        
        System.out.println("\n-----------------------------------------------------------------");
        try {
            // Wait for acknowledgment with timeout
            boolean delivered = sendFuture.get(10, TimeUnit.SECONDS);
            
            if (delivered) {
                System.out.println("Message SENT successfully to " + recipient);
            } else {
                System.out.println("Message delivery to " + recipient + " could not be confirmed");
                System.out.println("The server may be processing it, but no acknowledgment was received");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Operation interrupted while waiting for acknowledgment");
        } catch (ExecutionException e) {
            System.out.println("Error waiting for acknowledgment: " + e.getCause().getMessage());
        } catch (TimeoutException e) {
            System.out.println("Timed out waiting for acknowledgment");
            System.out.println("The message may still be delivered, but confirmation is delayed");
        }
        System.out.println("-----------------------------------------------------------------");
    }
    
    private static void viewReceivedMessages() {
        System.out.println("\n-----------------------------------------------------------------");
        System.out.println("                      RECEIVED MESSAGES                         ");
        System.out.println("-----------------------------------------------------------------");
        
        if (receivedMessages.isEmpty()) {
            System.out.println("No messages received yet.");
            return;
        }
        
        int count = 1;
        for (Message message : receivedMessages) {
            System.out.println("\n-----------------------------------------------------------------");
            System.out.println("Message #" + count);
            System.out.println("From: " + message.getSenderId());
            System.out.println("Timestamp: " + message.getTimestampInstant().toString());
            System.out.println("-----------------------------------------------------------------");
            System.out.println(message.getContent());
            System.out.println("-----------------------------------------------------------------");
            count++;
        }
        
        // Reset new message notification after viewing
        newMessageReceived = false;
    }
    
    private static void viewSystemInfo(ClientNode client) {
        System.out.println("\n-----------------------------------------------------------------");
        System.out.println("                     SYSTEM INFORMATION                         ");
        System.out.println("-----------------------------------------------------------------");
        
        System.out.println("Client ID: " + client.getId());
        System.out.println("Client Status: " + client.getStatus());
        
        List<String> connectedNodes = client.getConnectedNodes();
        System.out.println("Connected to server: " + (connectedNodes.isEmpty() ? "None" : connectedNodes.get(0)));
        
        System.out.println("Clock offset: " + client.getClockOffset() + " ms");
        System.out.println("Current synchronized time: " + client.getSynchronizedTime());
        System.out.println("Number of messages received: " + receivedMessages.size());
    }
    
    // Helper method to pad strings for formatting
    private static String padRight(String s, int n) {
        if (s.length() > n) {
            return s.substring(0, n-3) + "...";
        }
        return String.format("%-" + n + "s", s);
    }
} 