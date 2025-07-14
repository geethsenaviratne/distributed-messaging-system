package com.ds.messaging.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Manages server replication for the distributed messaging system.
 * Handles primary-backup relationships and coordinates acknowledgments.
 */
public class ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);
    
    // The singleton instance
    private static ReplicationManager instance;
    
    // Set of registered servers
    private final Set<String> servers;
    
    // Primary server ID (can change if primary fails)
    private String primaryServer;
    
    // Map of message IDs to acknowledgment trackers
    private final Map<UUID, AckTracker> ackTrackers;
    
    private final Set<UUID> recentlyAcknowledgedWithoutTracker;
    private final ScheduledExecutorService scheduledExecutorService;
    
    private ReplicationManager() {
        this.servers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.ackTrackers = new ConcurrentHashMap<>();
        this.primaryServer = null;
        this.recentlyAcknowledgedWithoutTracker = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.scheduledExecutorService = null; // Assuming a default constructor
    }
    
    /**
     * Gets the singleton instance of the replication manager.
     * 
     * @return The ReplicationManager instance
     */
    public static synchronized ReplicationManager getInstance() {
        if (instance == null) {
            instance = new ReplicationManager();
        }
        return instance;
    }
    
    /**
     * Registers a server with the replication manager.
     * 
     * @param serverId The ID of the server to register
     */
    public synchronized void registerServer(String serverId) {
        if (serverId == null || serverId.isEmpty()) {
            logger.warn("Attempted to register server with null or empty ID");
            return;
        }
        
        servers.add(serverId);
        logger.info("Registered server: {} (total servers: {})", serverId, servers.size());
        
        // If this is the first server, make it the primary
        if (primaryServer == null || primaryServer.isEmpty()) {
            primaryServer = serverId;
            logger.info("Server {} is now the primary server (first registered)", serverId);
        }
        
        // Debug state after registration
        logCurrentState();
    }
    
    /**
     * Unregisters a server from the replication manager.
     * 
     * @param serverId The ID of the server to unregister
     */
    public synchronized void unregisterServer(String serverId) {
        if (serverId == null) {
            return;
        }
        
        servers.remove(serverId);
        logger.info("Unregistered server: {} (remaining servers: {})", serverId, servers.size());
        
        // If the primary server is unregistered, elect a new one
        if (serverId.equals(primaryServer)) {
            electNewPrimary();
        }
        
        // Debug state after unregistration
        logCurrentState();
    }
    
    /**
     * Checks if a server is the primary server.
     * 
     * @param serverId The ID of the server to check
     * @return True if the server is the primary, false otherwise
     */
    public synchronized boolean isPrimary(String serverId) {
        if (serverId == null) {
            return false;
        }
        return serverId.equals(primaryServer);
    }
    
    /**
     * Gets the current primary server ID.
     * 
     * @return The ID of the primary server
     */
    public synchronized String getPrimaryServer() {
        return primaryServer;
    }
    
    /**
     * Gets the set of servers that should receive replicated messages.
     * For primary, this includes all servers except itself.
     * For backup, this includes only the primary.
     * 
     * @return Set of server IDs to replicate to
     */
    public synchronized Set<String> getReplicationTargets() {
        // Create a copy to avoid concurrent modification
        Set<String> targets = new HashSet<>(servers);
        return targets;
    }
    
    /**
     * Creates a tracker for acknowledgments for a message.
     * 
     * @param messageId The ID of the message to track acknowledgments for
     * @return true if a tracker was created, false if there are no backup servers to track
     */
    public synchronized boolean createAckTracker(UUID messageId) {
        if (messageId == null) {
            logger.warn("Cannot create acknowledgment tracker for null message ID");
            return false;
        }
        
        // Create a new tracker with all servers except the primary
        Set<String> pendingServers = new HashSet<>(servers);
        pendingServers.remove(primaryServer); // Primary doesn't need to acknowledge
        
        // If we have no other servers, don't create a tracker
        if (pendingServers.isEmpty()) {
            logger.info("No backup servers to track acknowledgments for message {}", messageId);
            return false;
        }
        
        AckTracker tracker = new AckTracker(pendingServers);
        ackTrackers.put(messageId, tracker);
        
        logger.info("Created acknowledgment tracker for message {}, waiting for {} servers: {}", 
                messageId, pendingServers.size(), pendingServers);
        
        return true;
    }
    
    /**
     * Creates an empty acknowledgment tracker for a message that initially has no backup servers.
     * This allows later acknowledgments (e.g., after synchronization) to be properly tracked.
     * 
     * @param messageId The ID of the message to track acknowledgments for
     */
    public synchronized void createEmptyAckTracker(UUID messageId) {
        if (messageId == null) {
            logger.warn("Cannot create empty acknowledgment tracker for null message ID");
            return;
        }
        
        // Skip if there's already a tracker for this message
        if (ackTrackers.containsKey(messageId)) {
            logger.debug("Acknowledgment tracker already exists for message {}", messageId);
            return;
        }
        
        // Create an empty tracker that will accept any server's acknowledgment
        AckTracker tracker = new AckTracker(new HashSet<>());
        ackTrackers.put(messageId, tracker);
        
        logger.info("Created empty placeholder acknowledgment tracker for message {}", messageId);
    }
    
    /**
     * Records an acknowledgment for a message from a server.
     * 
     * @param messageId The ID of the acknowledged message
     * @param serverId The ID of the server that sent the acknowledgment
     * @return True if all servers have acknowledged the message, false otherwise
     */
    public synchronized boolean acknowledgeMessage(UUID messageId, String serverId) {
        if (messageId == null || serverId == null) {
            logger.warn("Cannot acknowledge message with null ID or server ID");
            return false;
        }
        
        AckTracker tracker = ackTrackers.get(messageId);
        if (tracker == null) {
            // Instead of warning on every acknowledgment, store the acknowledgment for a short period
            // in case a tracker is created soon (e.g., during pending replication processing)
            if (!recentlyAcknowledgedWithoutTracker.contains(messageId)) {
                logger.warn("No acknowledgment tracker found for message {}", messageId);
                recentlyAcknowledgedWithoutTracker.add(messageId);
                
                // Create an empty tracker for this message to handle future acknowledgments
                createEmptyAckTracker(messageId);
                tracker = ackTrackers.get(messageId);
                
                // Schedule removal from the set after some time to prevent set growth
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        recentlyAcknowledgedWithoutTracker.remove(messageId);
                    }
                }, 60000); // 60 seconds
            }
            
            // If we created a tracker, use it; otherwise return true to prevent hanging operations
            if (tracker == null) {
                return true;
            }
        }
        
        logger.info("Processing acknowledgment from server {} for message {}", serverId, messageId);
        
        boolean acknowledged = tracker.acknowledge(serverId);
        if (!acknowledged) {
            logger.warn("Server {} was not in pending list for message {}", serverId, messageId);
        }
        
        boolean allAcknowledged = tracker.allAcknowledged();
        logger.info("Message {} acknowledgment status: {}/{} servers acknowledged", 
                messageId, tracker.getAcknowledgedCount(), tracker.getTotalCount());
        
        if (allAcknowledged) {
            logger.info("All servers acknowledged message {}", messageId);
            ackTrackers.remove(messageId);
        }
        
        return allAcknowledged;
    }
    
    /**
     * Cleans up the acknowledgment tracker for a message.
     * 
     * @param messageId The ID of the message to clean up
     */
    public synchronized void cleanup(UUID messageId) {
        if (messageId == null) {
            return;
        }
        
        AckTracker removed = ackTrackers.remove(messageId);
        if (removed != null) {
            logger.debug("Cleaned up acknowledgment tracker for message {}", messageId);
        }
    }
    
    /**
     * Elects a new primary server from the set of registered servers.
     * Currently uses a simple first-available policy.
     */
    private synchronized void electNewPrimary() {
        if (servers.isEmpty()) {
            primaryServer = null;
            logger.warn("No servers available to elect as primary");
        } else {
            // Simple election: choose first server in set
            primaryServer = servers.iterator().next();
            logger.info("Elected new primary server: {}", primaryServer);
        }
    }
    
    /**
     * Log the current state of the replication manager for debugging
     */
    private void logCurrentState() {
        logger.debug("ReplicationManager state: Primary={}, Servers={}, Tracking {} messages", 
                primaryServer, servers, ackTrackers.size());
    }
    
    /**
     * Tracks acknowledgments for a specific message.
     */
    private static class AckTracker {
        private final Set<String> pendingServers;
        private final Set<String> acknowledgedServers;
        private final int totalCount;
        private final boolean isEmptyTracker;
        
        public AckTracker(Set<String> servers) {
            this.pendingServers = new HashSet<>(servers);
            this.acknowledgedServers = new HashSet<>();
            this.totalCount = servers.size();
            this.isEmptyTracker = servers.isEmpty();
        }
        
        /**
         * Records an acknowledgment from a server.
         * 
         * @param serverId The ID of the server that acknowledged
         * @return True if the server was in the pending list or if this is an empty tracker, false otherwise
         */
        public synchronized boolean acknowledge(String serverId) {
            // For empty trackers (placeholder trackers), accept any server acknowledgment
            if (isEmptyTracker) {
                acknowledgedServers.add(serverId);
                return true;
            }
            
            // Normal case - check if server is in pending list
            if (pendingServers.remove(serverId)) {
                acknowledgedServers.add(serverId);
                return true;
            }
            return false;
        }
        
        /**
         * Checks if all servers have acknowledged.
         * 
         * @return True if all servers have acknowledged or if this is an empty tracker with at least one acknowledgment, false otherwise
         */
        public synchronized boolean allAcknowledged() {
            if (isEmptyTracker) {
                // For empty trackers, consider "all acknowledged" if we've received at least one ack
                return !acknowledgedServers.isEmpty();
            }
            
            // Normal case - all pending servers must have acknowledged
            return pendingServers.isEmpty() && !acknowledgedServers.isEmpty();
        }
        
        /**
         * Gets the count of servers that have acknowledged.
         * 
         * @return The number of servers that have acknowledged
         */
        public synchronized int getAcknowledgedCount() {
            return acknowledgedServers.size();
        }
        
        /**
         * Gets the total number of servers that need to acknowledge.
         * 
         * @return The total number of servers, or the number of acknowledged servers for empty trackers
         */
        public synchronized int getTotalCount() {
            // For empty trackers, report the acknowledged count as the total
            if (isEmptyTracker) {
                return Math.max(1, acknowledgedServers.size());
            }
            return totalCount;
        }
    }
} 