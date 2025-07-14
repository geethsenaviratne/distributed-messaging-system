package com.ds.messaging.consensus;

/**
 * Represents the possible states of a node in the Raft consensus algorithm.
 */
public enum RaftState {
    /**
     * Follower state - the default state for all nodes.
     * Responds to requests from leaders and candidates.
     * If no communication is received, transitions to candidate.
     */
    FOLLOWER,
    
    /**
     * Candidate state - used to elect a new leader.
     * Requests votes from other nodes.
     * Transitions to leader if majority votes received.
     * Transitions to follower if AppendEntries received from a new leader.
     */
    CANDIDATE,
    
    /**
     * Leader state - handles all client requests.
     * Sends periodic heartbeats to all followers.
     * Manages log replication to all followers.
     */
    LEADER
} 