package com.ds.messaging.common;

/**
 * Message sent by nodes to request time synchronization information.
 * This is the first step in the NTP-like time synchronization process.
 */
public class TimeSyncRequestMessage extends Message {
    private static final long serialVersionUID = 1L;
    
    // Timestamp when client sends the request
    private final long clientSendTime;
    
    /**
     * Creates a new time synchronization request message
     * 
     * @param senderId The ID of the sender node
     * @param receiverId The ID of the receiver node (time server)
     * @param clientSendTime The local time when the message is sent
     */
    public TimeSyncRequestMessage(String senderId, String receiverId, long clientSendTime) {
        super(senderId, receiverId, MessageType.TIME_SYNC_REQUEST);
        this.clientSendTime = clientSendTime;
    }
    
    /**
     * Gets the timestamp when the client sent the request
     * 
     * @return The client send timestamp
     */
    public long getClientSendTime() {
        return clientSendTime;
    }
    
    @Override
    public String toString() {
        return "TimeSyncRequestMessage{" +
                "id='" + getId() + '\'' +
                ", senderId='" + getSenderId() + '\'' +
                ", recipientId='" + getRecipientId() + '\'' +
                ", clientSendTime=" + clientSendTime +
                '}';
    }
} 