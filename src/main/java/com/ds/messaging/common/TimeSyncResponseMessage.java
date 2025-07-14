package com.ds.messaging.common;

/**
 * Message sent by the time server in response to a time synchronization request.
 * Contains timestamps used to calculate clock offset and round-trip delay.
 */
public class TimeSyncResponseMessage extends Message {
    private static final long serialVersionUID = 1L;
    
    // Original timestamp from the client's request
    private final long clientSendTime;
    // Time when server received the request
    private final long serverReceiveTime;
    // Time when server sent this response
    private final long serverSendTime;
    
    /**
     * Creates a new time synchronization response message
     * 
     * @param senderId The ID of the sender node (time server)
     * @param receiverId The ID of the receiver node (client)
     * @param clientSendTime The time when client sent the request
     * @param serverReceiveTime The time when server received the request
     * @param serverSendTime The time when server sends this response
     */
    public TimeSyncResponseMessage(String senderId, String receiverId, 
                                  long clientSendTime, long serverReceiveTime, 
                                  long serverSendTime) {
        super(senderId, receiverId, MessageType.TIME_SYNC_RESPONSE);
        this.clientSendTime = clientSendTime;
        this.serverReceiveTime = serverReceiveTime;
        this.serverSendTime = serverSendTime;
    }
    
    /**
     * Gets the timestamp when the client sent the original request
     * 
     * @return The client's original send timestamp
     */
    public long getClientSendTime() {
        return clientSendTime;
    }
    
    /**
     * Gets the timestamp when the server received the request
     * 
     * @return The server receive timestamp
     */
    public long getServerReceiveTime() {
        return serverReceiveTime;
    }
    
    /**
     * Gets the timestamp when the server sent this response
     * 
     * @return The server send timestamp
     */
    public long getServerSendTime() {
        return serverSendTime;
    }
    
    @Override
    public String toString() {
        return "TimeSyncResponseMessage{" +
                "id='" + getId() + '\'' +
                ", senderId='" + getSenderId() + '\'' +
                ", recipientId='" + getRecipientId() + '\'' +
                ", clientSendTime=" + clientSendTime +
                ", serverReceiveTime=" + serverReceiveTime +
                ", serverSendTime=" + serverSendTime +
                '}';
    }
} 