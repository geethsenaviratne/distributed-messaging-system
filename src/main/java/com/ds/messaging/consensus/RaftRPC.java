package com.ds.messaging.consensus;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains definitions for Remote Procedure Call (RPC) messages used in the Raft consensus algorithm.
 */
public class RaftRPC {
    
    /**
     * Base class for all RPC requests and responses.
     */
    public static abstract class RaftMessage implements Serializable {
        private static final long serialVersionUID = 1L;
        
        protected final String senderId;  // ID of the node sending the message
        protected final int term;         // Current term of the sender
        
        protected RaftMessage(String senderId, int term) {
            this.senderId = senderId;
            this.term = term;
        }
        
        public String getSenderId() {
            return senderId;
        }
        
        public int getTerm() {
            return term;
        }
    }
    
    /**
     * RequestVote RPC - Sent by candidates to gather votes.
     */
    public static class RequestVoteRPC extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        private final int lastLogIndex;   // Index of candidate's last log entry
        private final int lastLogTerm;    // Term of candidate's last log entry
        
        public RequestVoteRPC(String senderId, int term, int lastLogIndex, int lastLogTerm) {
            super(senderId, term);
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
        
        public int getLastLogIndex() {
            return lastLogIndex;
        }
        
        public int getLastLogTerm() {
            return lastLogTerm;
        }
    }
    
    /**
     * RequestVote Response - Reply to a vote request.
     */
    public static class RequestVoteResponse extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        private final boolean voteGranted; // True if the vote was granted
        
        public RequestVoteResponse(String senderId, int term, boolean voteGranted) {
            super(senderId, term);
            this.voteGranted = voteGranted;
        }
        
        public boolean isVoteGranted() {
            return voteGranted;
        }
    }
    
    /**
     * AppendEntries RPC - Used by leader to replicate log entries and as heartbeat.
     */
    public static class AppendEntriesRPC extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        private final String leaderId;        // Leader's ID so followers can redirect clients
        private final int prevLogIndex;       // Index of log entry immediately preceding new ones
        private final int prevLogTerm;        // Term of prevLogIndex entry
        private final List<LogEntry> entries; // Log entries to store (empty for heartbeat)
        private final int leaderCommit;       // Leader's commit index
        
        public AppendEntriesRPC(String senderId, int term, String leaderId, int prevLogIndex, 
                              int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
            super(senderId, term);
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = new ArrayList<>(entries);
            this.leaderCommit = leaderCommit;
        }
        
        public String getLeaderId() {
            return leaderId;
        }
        
        public int getPrevLogIndex() {
            return prevLogIndex;
        }
        
        public int getPrevLogTerm() {
            return prevLogTerm;
        }
        
        public List<LogEntry> getEntries() {
            return entries;
        }
        
        public int getLeaderCommit() {
            return leaderCommit;
        }
        
        public boolean isHeartbeat() {
            return entries.isEmpty();
        }
    }
    
    /**
     * AppendEntries Response - Reply to an append entries request.
     */
    public static class AppendEntriesResponse extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        private final boolean success;   // True if follower contained entry matching prevLogIndex and prevLogTerm
        private final int matchIndex;    // Index of highest log entry known to be replicated on server
        
        public AppendEntriesResponse(String senderId, int term, boolean success, int matchIndex) {
            super(senderId, term);
            this.success = success;
            this.matchIndex = matchIndex;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public int getMatchIndex() {
            return matchIndex;
        }
    }
    
    /**
     * InstallSnapshot RPC - Used by leader to send portions of a snapshot to a follower.
     * This is used for log compaction and is an advanced feature.
     */
    public static class InstallSnapshotRPC extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        private final String leaderId;         // Leader's ID
        private final int lastIncludedIndex;   // Index of last log entry in the snapshot
        private final int lastIncludedTerm;    // Term of lastIncludedIndex
        private final int offset;              // Byte offset where chunk is positioned in the snapshot file
        private final byte[] data;             // Raw bytes of the snapshot chunk
        private final boolean done;            // True if this is the last chunk
        
        public InstallSnapshotRPC(String senderId, int term, String leaderId, int lastIncludedIndex,
                                int lastIncludedTerm, int offset, byte[] data, boolean done) {
            super(senderId, term);
            this.leaderId = leaderId;
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.offset = offset;
            this.data = data;
            this.done = done;
        }
        
        public String getLeaderId() {
            return leaderId;
        }
        
        public int getLastIncludedIndex() {
            return lastIncludedIndex;
        }
        
        public int getLastIncludedTerm() {
            return lastIncludedTerm;
        }
        
        public int getOffset() {
            return offset;
        }
        
        public byte[] getData() {
            return data;
        }
        
        public boolean isDone() {
            return done;
        }
    }
    
    /**
     * InstallSnapshot Response - Reply to an install snapshot request.
     */
    public static class InstallSnapshotResponse extends RaftMessage {
        private static final long serialVersionUID = 1L;
        
        public InstallSnapshotResponse(String senderId, int term) {
            super(senderId, term);
        }
    }
} 