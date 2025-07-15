package com.ds.messaging.consensus;

/**
 * Configuration parameters for the Raft consensus algorithm.
 * This class contains tunable parameters that control Raft's behavior.
 */
public class RaftConfig {
    // Singleton instance
    private static RaftConfig instance;
    
    // Election timeout range in milliseconds (randomized between min and max)
    private final int minElectionTimeoutMs;
    private final int maxElectionTimeoutMs;
    
    // Heartbeat interval in milliseconds
    private final int heartbeatIntervalMs;
    
    // Chunk size for log entry batching
    private final int logBatchSize;
    
    // Maximum entries to send in a single AppendEntries RPC
    private final int maxEntriesPerAppend;
    
    // Snapshot configuration
    private final int snapshotThreshold;
    private final boolean enableSnapshotting;
    
    // Persistence configuration
    private final boolean enableDiskPersistence;
    
    private RaftConfig(int minElectionTimeoutMs, int maxElectionTimeoutMs, int heartbeatIntervalMs,
                      int logBatchSize, int maxEntriesPerAppend, int snapshotThreshold,
                      boolean enableSnapshotting, boolean enableDiskPersistence) {
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.logBatchSize = logBatchSize;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        this.snapshotThreshold = snapshotThreshold;
        this.enableSnapshotting = enableSnapshotting;
        this.enableDiskPersistence = enableDiskPersistence;
    }
    
    /**
     * Initialize with custom parameters.
     * 
     * @param minElectionTimeoutMs Minimum election timeout in milliseconds
     * @param maxElectionTimeoutMs Maximum election timeout in milliseconds
     * @param heartbeatIntervalMs Heartbeat interval in milliseconds
     * @param logBatchSize Batch size for log entries
     * @param maxEntriesPerAppend Maximum entries per append operation
     * @param snapshotThreshold Threshold for creating snapshots
     * @param enableSnapshotting Whether to enable snapshotting
     * @param enableDiskPersistence Whether to enable disk persistence
     */
    public static void initialize(int minElectionTimeoutMs, int maxElectionTimeoutMs, int heartbeatIntervalMs,
                                 int logBatchSize, int maxEntriesPerAppend, int snapshotThreshold,
                                 boolean enableSnapshotting, boolean enableDiskPersistence) {
        instance = new RaftConfig(minElectionTimeoutMs, maxElectionTimeoutMs, heartbeatIntervalMs,
                                 logBatchSize, maxEntriesPerAppend, snapshotThreshold,
                                 enableSnapshotting, enableDiskPersistence);
    }
    
    /**
     * Get the singleton instance.
     * 
     * @return The RaftConfig instance
     */
    public static RaftConfig getInstance() {
        if (instance == null) {
            // Default parameters
            instance = new RaftConfig(
                150, 300,    // Election timeout between 150-300ms
                100,         // Heartbeat interval of 100ms
                100,         // Log batch size of 100 entries
                20,          // Max 20 entries per append operation
                1000,        // Create snapshot after 1000 entries
                false,       // Disable snapshotting initially
                true         // Enable disk persistence by default
            );
        }
        return instance;
    }
    
    /**
     * @return The minimum election timeout in milliseconds
     */
    public int getMinElectionTimeoutMs() {
        return minElectionTimeoutMs;
    }
    
    /**
     * @return The maximum election timeout in milliseconds
     */
    public int getMaxElectionTimeoutMs() {
        return maxElectionTimeoutMs;
    }
    
    /**
     * @return The heartbeat interval in milliseconds
     */
    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }
    
    /**
     * @return The log batch size
     */
    public int getLogBatchSize() {
        return logBatchSize;
    }
    
    /**
     * @return The maximum entries per append operation
     */
    public int getMaxEntriesPerAppend() {
        return maxEntriesPerAppend;
    }
    
    /**
     * @return The snapshot threshold
     */
    public int getSnapshotThreshold() {
        return snapshotThreshold;
    }
    
    /**
     * @return Whether snapshotting is enabled
     */
    public boolean isSnapshotEnabled() {
        return enableSnapshotting;
    }
    
    /**
     * @return Whether disk persistence is enabled
     */
    public boolean isDiskPersistenceEnabled() {
        return enableDiskPersistence;
    }
} 