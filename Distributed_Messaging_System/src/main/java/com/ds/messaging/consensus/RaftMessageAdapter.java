package com.ds.messaging.consensus;

import com.ds.messaging.common.Message;
import com.ds.messaging.common.MessageHandler;
import com.ds.messaging.common.MessageType;
import com.ds.messaging.consensus.RaftRPC.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.function.Consumer;

/**
 * Adapts Raft RPC messages to the existing messaging system.
 * This class bridges between the Raft consensus algorithm and the messaging layer.
 */
public class RaftMessageAdapter implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftMessageAdapter.class);
    
    private final RaftNode raftNode;
    private final Consumer<Message> messageSender;
    
    /**
     * Creates a new adapter.
     * 
     * @param raftNode The Raft node to adapt
     * @param messageSender A function to send messages
     */
    public RaftMessageAdapter(RaftNode raftNode, Consumer<Message> messageSender) {
        this.raftNode = raftNode;
        this.messageSender = messageSender;
    }
    
    @Override
    public boolean handleMessage(Message message, String sourceNodeId) {
        MessageType type = message.getType();
        
        if (type == MessageType.ELECTION) {
            // Handle RequestVote RPC
            try {
                RequestVoteRPC rpc = deserialize(message.getContent(), RequestVoteRPC.class);
                logger.info("RPC_TRACE: RECEIVED RequestVote: sender={}, term={}, lastLogIndex={}, lastLogTerm={}", 
                        rpc.getSenderId(), rpc.getTerm(), rpc.getLastLogIndex(), rpc.getLastLogTerm());
                raftNode.handleRequestVote(rpc);
                return true;
            } catch (Exception e) {
                logger.error("Error handling RequestVote: {}", e.getMessage(), e);
            }
        } else if (type == MessageType.VOTE) {
            // Handle RequestVote response
            try {
                RequestVoteResponse response = deserialize(message.getContent(), RequestVoteResponse.class);
                logger.info("RPC_TRACE: RECEIVED RequestVoteResponse: sender={}, term={}, voteGranted={}", 
                        response.getSenderId(), response.getTerm(), response.isVoteGranted());
                raftNode.handleRequestVoteResponse(response);
                return true;
            } catch (Exception e) {
                logger.error("Error handling RequestVote response: {}", e.getMessage(), e);
            }
        } else if (type == MessageType.LOG_REPLICATION) {
            // Handle AppendEntries RPC
            try {
                AppendEntriesRPC rpc = deserialize(message.getContent(), AppendEntriesRPC.class);
                int entriesSize = rpc.getEntries() != null ? rpc.getEntries().size() : 0;
                logger.info("RPC_TRACE: RECEIVED AppendEntries: sender={}, term={}, leaderId={}, prevLogIndex={}, prevLogTerm={}, entries={}, leaderCommit={}", 
                        rpc.getSenderId(), rpc.getTerm(), rpc.getLeaderId(), rpc.getPrevLogIndex(), 
                        rpc.getPrevLogTerm(), entriesSize, rpc.getLeaderCommit());
                raftNode.handleAppendEntries(rpc);
                return true;
            } catch (Exception e) {
                logger.error("Error handling AppendEntries: {}", e.getMessage(), e);
            }
        } else if (type == MessageType.ACK) {
            // Handle AppendEntries response
            try {
                AppendEntriesResponse response = deserialize(message.getContent(), AppendEntriesResponse.class);
                logger.info("RPC_TRACE: RECEIVED AppendEntriesResponse: sender={}, term={}, success={}, matchIndex={}", 
                        response.getSenderId(), response.getTerm(), response.isSuccess(), response.getMatchIndex());
                raftNode.handleAppendEntriesResponse(response);
                return true;
            } catch (Exception e) {
                logger.error("Error handling AppendEntries response: {}", e.getMessage(), e);
            }
        } else if (type == MessageType.SYNC_REQUEST) {
            // Handle InstallSnapshot RPC (if needed in the future)
            logger.info("RPC_TRACE: RECEIVED InstallSnapshot Request from {}", sourceNodeId);
            return true;
        } else if (type == MessageType.SYNC_RESPONSE) {
            // Handle InstallSnapshot response (if needed in the future)
            logger.info("RPC_TRACE: RECEIVED InstallSnapshot Response from {}", sourceNodeId);
            return true;
        }
        
        return false;
    }
    
    /**
     * Deserialize an object from a string.
     * 
     * @param content The serialized content
     * @param clazz The class to deserialize to
     * @return The deserialized object
     */
    private <T> T deserialize(String content, Class<T> clazz) throws IOException, ClassNotFoundException {
        byte[] data = java.util.Base64.getDecoder().decode(content);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return clazz.cast(ois.readObject());
        }
    }
    
    /**
     * Serialize an object to a string.
     * 
     * @param object The object to serialize
     * @return The serialized string
     */
    private String serialize(Object object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            return java.util.Base64.getEncoder().encodeToString(bos.toByteArray());
        }
    }
    
    /**
     * Send a RequestVote RPC.
     * 
     * @param rpc The RequestVote RPC to send
     */
    public void sendRequestVote(RequestVoteRPC rpc) {
        try {
            String content = serialize(rpc);
            Message message = new Message(
                    rpc.getSenderId(),
                    "all",  // Send to all nodes
                    content,
                    MessageType.ELECTION
            );
            logger.info("RPC_TRACE: SENDING RequestVote: sender={}, term={}, lastLogIndex={}, lastLogTerm={}", 
                    rpc.getSenderId(), rpc.getTerm(), rpc.getLastLogIndex(), rpc.getLastLogTerm());
            messageSender.accept(message);
            logger.debug("Sent RequestVote to all nodes for term {}", rpc.getTerm());
        } catch (IOException e) {
            logger.error("Error sending RequestVote: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send a RequestVote response.
     * 
     * @param response The RequestVote response to send
     */
    public void sendRequestVoteResponse(RequestVoteResponse response) {
        try {
            String content = serialize(response);
            Message message = new Message(
                    response.getSenderId(),
                    response.getSenderId(),  // Send back to the requestor
                    content,
                    MessageType.VOTE
            );
            logger.info("RPC_TRACE: SENDING RequestVoteResponse: sender={}, term={}, voteGranted={}", 
                    response.getSenderId(), response.getTerm(), response.isVoteGranted());
            messageSender.accept(message);
            logger.debug("Sent RequestVote response to {}, granted: {}", 
                    response.getSenderId(), response.isVoteGranted());
        } catch (IOException e) {
            logger.error("Error sending RequestVote response: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send an AppendEntries RPC.
     * 
     * @param rpc The AppendEntries RPC to send
     */
    public void sendAppendEntries(AppendEntriesRPC rpc) {
        try {
            String content = serialize(rpc);
            Message message = new Message(
                    rpc.getSenderId(),
                    "all",  // Send to all nodes
                    content,
                    MessageType.LOG_REPLICATION
            );
            int entriesSize = rpc.getEntries() != null ? rpc.getEntries().size() : 0;
            logger.info("RPC_TRACE: SENDING AppendEntries: sender={}, term={}, leaderId={}, prevLogIndex={}, prevLogTerm={}, entries={}, leaderCommit={}", 
                    rpc.getSenderId(), rpc.getTerm(), rpc.getLeaderId(), rpc.getPrevLogIndex(), 
                    rpc.getPrevLogTerm(), entriesSize, rpc.getLeaderCommit());
            messageSender.accept(message);
            logger.debug("Sent AppendEntries to all nodes, entries: {}", entriesSize);
        } catch (IOException e) {
            logger.error("Error sending AppendEntries: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send an AppendEntries response.
     * 
     * @param response The AppendEntries response to send
     */
    public void sendAppendEntriesResponse(AppendEntriesResponse response) {
        try {
            String content = serialize(response);
            Message message = new Message(
                    response.getSenderId(),
                    response.getSenderId(),  // Send back to the requestor
                    content,
                    MessageType.ACK
            );
            logger.info("RPC_TRACE: SENDING AppendEntriesResponse: sender={}, term={}, success={}, matchIndex={}", 
                    response.getSenderId(), response.getTerm(), response.isSuccess(), response.getMatchIndex());
            messageSender.accept(message);
            logger.debug("Sent AppendEntries response to {}, success: {}", 
                    response.getSenderId(), response.isSuccess());
        } catch (IOException e) {
            logger.error("Error sending AppendEntries response: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send an InstallSnapshot RPC.
     * 
     * @param rpc The InstallSnapshot RPC to send
     */
    public void sendInstallSnapshot(InstallSnapshotRPC rpc) {
        try {
            String content = serialize(rpc);
            Message message = new Message(
                    rpc.getSenderId(),
                    "all",  // Send to all nodes
                    content,
                    MessageType.SYNC_REQUEST
            );
            messageSender.accept(message);
            logger.debug("Sent InstallSnapshot to all nodes");
        } catch (IOException e) {
            logger.error("Error sending InstallSnapshot: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Send an InstallSnapshot response.
     * 
     * @param response The InstallSnapshot response to send
     */
    public void sendInstallSnapshotResponse(InstallSnapshotResponse response) {
        try {
            String content = serialize(response);
            Message message = new Message(
                    response.getSenderId(),
                    response.getSenderId(),  // Send back to the requestor
                    content,
                    MessageType.SYNC_RESPONSE
            );
            messageSender.accept(message);
            logger.debug("Sent InstallSnapshot response to {}", response.getSenderId());
        } catch (IOException e) {
            logger.error("Error sending InstallSnapshot response: {}", e.getMessage(), e);
        }
    }
} 