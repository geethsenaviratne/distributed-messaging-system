package com.ds.messaging.consensus;

import com.ds.messaging.common.Message;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents a single entry in the Raft log.
 * Each entry contains a command (Message) and term information.
 */
public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final UUID id;
    private final int term;
    private final int index;
    private final Message command;
    private boolean committed;
    
    /**
     * Creates a new log entry.
     * 
     * @param term The term in which the entry was created
     * @param index The index of this entry in the log
     * @param command The command (message) to be stored
     */
    public LogEntry(int term, int index, Message command) {
        this.id = UUID.randomUUID();
        this.term = term;
        this.index = index;
        this.command = command;
        this.committed = false;
    }
    
    /**
     * @return The unique identifier for this log entry
     */
    public UUID getId() {
        return id;
    }
    
    /**
     * @return The term in which this entry was created
     */
    public int getTerm() {
        return term;
    }
    
    /**
     * @return The index of this entry in the log
     */
    public int getIndex() {
        return index;
    }
    
    /**
     * @return The command (message) stored in this entry
     */
    public Message getCommand() {
        return command;
    }
    
    /**
     * @return Whether this entry has been committed
     */
    public boolean isCommitted() {
        return committed;
    }
    
    /**
     * Mark this entry as committed.
     */
    public void setCommitted(boolean committed) {
        this.committed = committed;
    }
    
    @Override
    public String toString() {
        return String.format("LogEntry(term=%d, index=%d, committed=%s, command=%s)",
                term, index, committed, command != null ? command.getId() : "null");
    }
} 