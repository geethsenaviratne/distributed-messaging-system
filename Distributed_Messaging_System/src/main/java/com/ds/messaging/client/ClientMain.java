package com.ds.messaging.client;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for starting a client node.
 */
public class ClientMain {
    private static final Logger logger = LoggerFactory.getLogger(ClientMain.class);
    private static final AtomicBoolean receivedMessage = new AtomicBoolean(false);
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: ClientMain <clientID> <serverHost> <serverPort> [backupServer1] [backupServer2] ...");
            System.out.println("Example: ClientMain client1 localhost 8000 localhost:8001 localhost:8002");
            return;
        }
        
        String clientId = args[0];
        String serverHost = args[1];
        int serverPort = Integer.parseInt(args[2]);
        
        List<String> backupServers = new ArrayList<>();
        if (args.length > 3) {
            backupServers.addAll(Arrays.asList(args).subList(3, args.length));
        }
        
        try {
            ClientNode client = new ClientNode(clientId, serverHost, serverPort, backupServers);
            
            // Register a message handler to print received messages
            client.registerMessageHandler(new MessageHandler() {
                @Override
                public boolean handleMessage(Message message, String sourceNodeId) {
                    if (message.getType() == MessageType.USER_MESSAGE) {
                        System.out.println("\n\n=================================================");
                        System.out.println("  NEW MESSAGE RECEIVED ");
                        System.out.println("  From: " + message.getSenderId());
                        System.out.println("  Content: " + message.getContent());

                        receivedMessage.set(true);
                        displayMenu();
                    }
                    return true;
                }
            });
            
            client.start();
            
            // Add a shutdown hook to gracefully stop the client
            Runtime.getRuntime().addShutdownHook(new Thread(client::stop));
            
            System.out.println("=================================================");
            System.out.println("      DISTRIBUTED MESSAGING SYSTEM - CLIENT      ");
            System.out.println("=================================================");
            System.out.println("Client started with ID: " + clientId);
            System.out.println("Connected to server: " + serverHost + ":" + serverPort);
            System.out.println("Backup servers: " + backupServers);
            System.out.println("=================================================");
            
            // Start the menu-driven interface
            Scanner scanner = new Scanner(System.in);
            boolean running = true;
            
            while (running) {
                displayMenu();
                
                String choice = scanner.nextLine().trim();
                
                switch (choice) {
                    case "1":
                        // Send a message
                        System.out.print("Enter recipient ID: ");
                        String recipientId = scanner.nextLine().trim();
                        
                        System.out.print("Enter message content: ");
                        String messageContent = scanner.nextLine();
                        
                        Message message = new Message(clientId, recipientId, messageContent, MessageType.USER_MESSAGE);
                        
                        System.out.println("Sending message to " + recipientId + "...");
                        CompletableFuture<Boolean> sendFuture = client.sendMessageWithAck(message);
                        
                        try {
                            // Wait for acknowledgment with timeout
                            boolean delivered = sendFuture.get(10, TimeUnit.SECONDS);
                            
                            if (delivered) {
                                System.out.println("Message SENT successfully to " + recipientId);
                            } else {
                                System.out.println("Message delivery to " + recipientId + " could not be confirmed");
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
                        
                        break;
                        
                    case "2":
                        // Check for new messages
                        if (receivedMessage.getAndSet(false)) {
                            System.out.println("You have received new messages (displayed above).");
                        } else {
                            System.out.println("No new messages.");
                        }
                        break;
                        
                    case "3":
                        // Check status
                        System.out.println("Client status: " + client.getStatus());
                        System.out.println("Connected to: " + client.getConnectedNodes());
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
            
            client.stop();
            System.out.println("Client stopped");
            
        } catch (IOException e) {
            logger.error("Error starting client: {}", e.getMessage(), e);
            System.err.println("Error starting client: " + e.getMessage());
        }
    }
    
    private static void displayMenu() {
        System.out.println("\n-------------------------------------------------");
        System.out.println("               AVAILABLE ACTIONS                 ");
        System.out.println("-------------------------------------------------");
        System.out.println("1. Send a message");
        System.out.println("2. Check for new messages");
        System.out.println("3. Check client status");
        System.out.println("4. Exit");
        System.out.println("-------------------------------------------------");
        System.out.print("Enter your choice (1-4): ");
    }
} 