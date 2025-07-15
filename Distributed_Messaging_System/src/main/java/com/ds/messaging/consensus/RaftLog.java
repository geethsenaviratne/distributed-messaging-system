package com.ds.messaging.consensus;

import com.ds.messaging.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the log for a Raft node. The log contains entries that are replicated to all nodes.
 * Provides persistence and retrieval functionality for log entries.
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);
    
    private final String nodeId;
    private final List<LogEntry> entries;
    private final ReadWriteLock lock;
    private final boolean persistToDisk;
    private final Path persistencePath;
    
    // Log indexes
    private int commitIndex;  // highest index known to be committed
    private int lastApplied;  // highest index applied to state machine
    
    /**
     * Creates a new Raft log.
     * 
     * @param nodeId The ID of the node owning this log
     */
    public RaftLog(String nodeId) {
        this.nodeId = nodeId;
        this.entries = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
        this.persistToDisk = RaftConfig.getInstance().isDiskPersistenceEnabled();
        this.persistencePath = Paths.get("logs", "raft", nodeId);
        this.commitIndex = -1;
        this.lastApplied = -1;
        
        // Add a sentinel entry at index 0 with term 0
        entries.add(new LogEntry(0, 0, null));
        
        // Initialize or load log from disk if persistence is enabled
        if (persistToDisk) {
            initializeStorage();
            loadFromDisk();
        }
    }
    
    /**
     * Initialize the storage directory.
     */
    private void initializeStorage() {
        try {
            Files.createDirectories(persistencePath);
            logger.info("RaftLog storage directory initialized: {}", persistencePath);
        } catch (IOException e) {
            logger.error("Failed to initialize RaftLog storage directory: {}", e.getMessage());
            // Continue without persistence
        }
    }
    
    /**
     * Load the log from disk.
     */
    private void loadFromDisk() {
        Path logFile = persistencePath.resolve("log.dat");
        if (!Files.exists(logFile)) {
            logger.info("No log file found at {}, starting with empty log", logFile);
            return;
        }
        
        try {
            // Create a backup of the original file before reading from it
            Path backupFile = persistencePath.resolve("log.dat.bak");
            try {
                Files.copy(logFile, backupFile);
                logger.debug("Created backup of log file at {}", backupFile);
            } catch (IOException e) {
                logger.warn("Failed to create backup of log file: {}", e.getMessage());
                // Continue anyway, but log the error
            }
            
            // Try to read with a buffered stream for better error recovery
            try (ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(Files.newInputStream(logFile)))) {
                
                // Reset the entries list, keeping only the sentinel
                entries.clear();
                entries.add(new LogEntry(0, 0, null));
                
                int entryCount = in.readInt();
                logger.debug("Attempting to read {} entries from disk", entryCount);
                
                for (int i = 0; i < entryCount; i++) {
                    try {
                        LogEntry entry = (LogEntry) in.readObject();
                        if (entry == null) {
                            logger.warn("Read null entry at position {}, skipping", i);
                            continue;
                        }
                        
                        int entryIndex = entry.getIndex();
                        if (entryIndex <= 0) {
                            logger.warn("Invalid entry index {} at position {}, skipping", entryIndex, i);
                            continue;
                        }
                        
                        // Ensure we have space in the entries list
                        while (entries.size() <= entryIndex) {
                            entries.add(null);
                        }
                        entries.set(entryIndex, entry);
                        logger.debug("Loaded entry {} at index {}", entry.getId(), entryIndex);
                    } catch (Exception e) {
                        logger.warn("Error reading entry at position {}: {}", i, e.getMessage());
                        // Continue with next entry
                    }
                }
                
                // Remove any null entries (gaps in the log)
                entries.removeIf(entry -> entry == null);
                
                // Sort entries by index to ensure consistency
                entries.sort((e1, e2) -> Integer.compare(e1.getIndex(), e2.getIndex()));
                
                try {
                    this.commitIndex = in.readInt();
                    this.lastApplied = in.readInt();
                } catch (Exception e) {
                    // If we can't read commit indices, use conservative values
                    logger.warn("Could not read commit indices, using defaults: {}", e.getMessage());
                    this.commitIndex = 0;
                    this.lastApplied = 0;
                }
                
                logger.info("Loaded {} log entries from disk, commitIndex={}, lastApplied={}",
                        entries.size() - 1, commitIndex, lastApplied);
            }
        } catch (IOException e) {
            logger.error("Failed to load log from disk: {}", e.getMessage());
            // Handle recovery from backup if the main file is corrupt
            Path backupFile = persistencePath.resolve("log.dat.bak");
            if (Files.exists(backupFile)) {
                logger.info("Attempting recovery from backup file");
                try {
                    Files.move(backupFile, logFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    loadFromDisk(); // Recursive call to try again with the backup
                    return;
                } catch (IOException ex) {
                    logger.error("Failed to recover from backup: {}", ex.getMessage());
                }
            }
            // Continue with empty log if recovery fails
            logger.warn("Continuing with empty log after failed load");
        }
    }
    
    /**
     * Save the log to disk.
     */
    private void saveToDisk() {
        if (!persistToDisk) {
            return;
        }
        
        Path logFile = persistencePath.resolve("log.dat");
        Path tempFile = persistencePath.resolve("log.dat.tmp");
        
        try {
            // Write to a temporary file first
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(tempFile)))) {
                
                lock.readLock().lock();
                try {
                    List<LogEntry> validEntries = new ArrayList<>();
                    for (LogEntry entry : entries) {
                        if (entry != null) {
                            validEntries.add(entry);
                        }
                    }
                    
                    out.writeInt(validEntries.size());
                    for (LogEntry entry : validEntries) {
                        out.writeObject(entry);
                    }
                    out.writeInt(commitIndex);
                    out.writeInt(lastApplied);
                    out.flush();
                } finally {
                    lock.readLock().unlock();
                }
            }
            
            // Atomically replace the old file with the new one
            Files.move(tempFile, logFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Saved {} log entries to disk", entries.size());
        } catch (IOException e) {
            logger.error("Failed to save log to disk: {}", e.getMessage());
            try {
                Files.deleteIfExists(tempFile);
            } catch (IOException ex) {
                // Ignore cleanup errors
            }
        }
    }
    
    /**
     * Append a new entry to the log.
     * 
     * @param term The term of the entry
     * @param command The command to store
     * @return The index of the new entry
     */
    public int append(int term, Message command) {
        lock.writeLock().lock();
        try {
            int newIndex = entries.size();
            LogEntry entry = new LogEntry(term, newIndex, command);
            entries.add(entry);
            logger.debug("Appended entry {} to log at index {}", entry.getId(), newIndex);
            
            if (persistToDisk) {
                saveToDisk();
            }
            
            return newIndex;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Append entries received from the leader.
     * 
     * @param prevLogIndex Index of log entry immediately preceding new ones
     * @param prevLogTerm Term of prevLogIndex entry
     * @param entriesToAppend Entries to append (empty for heartbeat)
     * @param leaderCommit Leader's commitIndex
     * @return True if entries were appended successfully
     */
    public boolean appendEntries(int prevLogIndex, int prevLogTerm, List<LogEntry> entriesToAppend, int leaderCommit) {
        lock.writeLock().lock();
        try {
            // Step 1: Reply false if log doesn't contain an entry at prevLogIndex
            if (prevLogIndex > getLastLogIndex()) {
                logger.warn("Previous log index {} > last log index {}, requesting earlier entries", 
                    prevLogIndex, getLastLogIndex());
                return false;
            }
            
            // Step 2: Reply false if entry at prevLogIndex doesn't match prevLogTerm
            if (prevLogIndex > 0 && getEntry(prevLogIndex) != null && 
                getEntry(prevLogIndex).getTerm() != prevLogTerm) {
                logger.warn("Term mismatch at index {}: local={}, leader={}",
                        prevLogIndex, getEntry(prevLogIndex).getTerm(), prevLogTerm);
                // Delete the conflicting entry and all that follow
                while (entries.size() > prevLogIndex) {
                    entries.remove(entries.size() - 1);
                }
                if (persistToDisk) {
                    saveToDisk();
                }
                return false;
            }
            
            // Step 3: Append any new entries not already in the log
            for (int i = 0; i < entriesToAppend.size(); i++) {
                LogEntry newEntry = entriesToAppend.get(i);
                int index = prevLogIndex + 1 + i;
                
                // If we're adding beyond the current end of the log
                if (index >= entries.size()) {
                    entries.add(newEntry);
                    logger.debug("Added new entry at index {}: {}", index, newEntry.getId());
                } 
                // If we're replacing entries, check for conflicts
                else if (entries.get(index).getTerm() != newEntry.getTerm()) {
                    // Delete all entries from this index onwards
                    logger.debug("Conflict at index {}, truncating log", index);
                    while (entries.size() > index) {
                        entries.remove(entries.size() - 1);
                    }
                    entries.add(newEntry);
                }
                // Entry already exists with same term, no need to update
            }
            
            // Step 4: Update commit index
            if (leaderCommit > commitIndex) {
                int newCommitIndex = Math.min(leaderCommit, entries.size() - 1);
                setCommitIndex(newCommitIndex);
                logger.debug("Updated commit index to {}", newCommitIndex);
            }
            
            if (persistToDisk) {
                saveToDisk();
            }
            
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get entries starting from a specified index up to the end of the log.
     * 
     * @param startIndex The index to start from (inclusive)
     * @return A list of log entries
     */
    public List<LogEntry> getEntriesFrom(int startIndex) {
        lock.readLock().lock();
        try {
            if (startIndex >= entries.size()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(startIndex, entries.size()));
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get entries between a range of indices.
     * 
     * @param startIndex The start index (inclusive)
     * @param endIndex The end index (exclusive)
     * @return A list of log entries
     */
    public List<LogEntry> getEntries(int startIndex, int endIndex) {
        lock.readLock().lock();
        try {
            if (startIndex >= entries.size() || startIndex >= endIndex) {
                return Collections.emptyList();
            }
            int actualEndIndex = Math.min(endIndex, entries.size());
            return new ArrayList<>(entries.subList(startIndex, actualEndIndex));
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the entry at the specified index.
     * 
     * @param index The index of the entry
     * @return The log entry, or null if out of bounds
     */
    public LogEntry getEntry(int index) {
        lock.readLock().lock();
        try {
            if (index < 0 || index >= entries.size()) {
                return null;
            }
            return entries.get(index);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the last log entry.
     * 
     * @return The last log entry, or the sentinel if the log is empty
     */
    public LogEntry getLastEntry() {
        lock.readLock().lock();
        try {
            return entries.get(entries.size() - 1);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the last log index.
     * 
     * @return The index of the last entry in the log
     */
    public int getLastLogIndex() {
        lock.readLock().lock();
        try {
            return entries.size() - 1;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the term of the last log entry.
     * 
     * @return The term of the last entry in the log
     */
    public int getLastLogTerm() {
        lock.readLock().lock();
        try {
            return entries.get(entries.size() - 1).getTerm();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Set the commit index and apply committed entries.
     * 
     * @param index The new commit index
     */
    public void setCommitIndex(int index) {
        lock.writeLock().lock();
        try {
            if (index > getLastLogIndex()) {
                logger.warn("Attempted to set commit index {} beyond last log index {}", 
                        index, getLastLogIndex());
                index = getLastLogIndex();
            }
            
            if (index > commitIndex) {
                commitIndex = index;
                logger.debug("Updated commit index to {}", commitIndex);
                
                // Mark entries as committed
                for (int i = lastApplied + 1; i <= commitIndex; i++) {
                    entries.get(i).setCommitted(true);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get the current commit index.
     * 
     * @return The current commit index
     */
    public int getCommitIndex() {
        lock.readLock().lock();
        try {
            return commitIndex;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Set the last applied index.
     * 
     * @param index The new last applied index
     */
    public void setLastApplied(int index) {
        lock.writeLock().lock();
        try {
            if (index > commitIndex) {
                logger.warn("Attempted to set lastApplied index {} beyond commit index {}", 
                        index, commitIndex);
                index = commitIndex;
            }
            lastApplied = index;
            logger.debug("Updated lastApplied to {}", lastApplied);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Get the current last applied index.
     * 
     * @return The current last applied index
     */
    public int getLastApplied() {
        lock.readLock().lock();
        try {
            return lastApplied;
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get the total number of entries in the log.
     * 
     * @return The number of entries
     */
    public int size() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * Get entries that need to be applied to the state machine.
     * 
     * @return The list of entries to apply (between lastApplied and commitIndex)
     */
    public List<LogEntry> getEntriesToApply() {
        lock.readLock().lock();
        try {
            if (lastApplied >= commitIndex) {
                return Collections.emptyList();
            }
            return new ArrayList<>(entries.subList(lastApplied + 1, commitIndex + 1));
        } finally {
            lock.readLock().unlock();
        }
    }
} 