package com.ds.messaging.common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a message in the distributed messaging system.
 * Messages are serializable to allow for transmission over the network.
 */
public class Message implements Serializable, TimestampedMessage {
    private static final long serialVersionUID = 4L; // Increment version number due to changes
    
    // Serialization improvements
    private static final ObjectStreamField[] serialPersistentFields = {
        new ObjectStreamField("id", UUID.class),
        new ObjectStreamField("senderId", String.class),
        new ObjectStreamField("recipientId", String.class),
        new ObjectStreamField("content", String.class),
        new ObjectStreamField("timestamp", Instant.class),
        new ObjectStreamField("replicationCount", int.class),
        new ObjectStreamField("type", MessageType.class),
        new ObjectStreamField("sequenceNumber", long.class),
        new ObjectStreamField("originServerId", String.class),
        new ObjectStreamField("isAcknowledgment", boolean.class),
        new ObjectStreamField("acknowledgedMessageId", UUID.class),
        new ObjectStreamField("payload", Message.class)
    };
    
    private final UUID id;
    private final String senderId;
    private final String recipientId;
    private final String content;
    private Instant timestamp; // Changed from final to allow modifications
    private int replicationCount;
    private final MessageType type;
    
    // Added for strong consistency
    private long sequenceNumber = -1;        // Per-server sequence number for ordering
    private String originServerId = null;    // Server that first processed the message
    private boolean isAcknowledgment = false; // If this is an acknowledgment message
    private UUID acknowledgedMessageId = null; // ID of the message being acknowledged
    // Added for message forwarding
    private Message payload = null; // Payload for forwarded messages

    /**
     * Creates a new message.
     *
     * @param senderId    The ID of the sender
     * @param recipientId The ID of the recipient
     * @param content     The message content
     * @param type        The type of message
     */
    public Message(String senderId, String recipientId, String content, MessageType type) {
        this.id = UUID.randomUUID();
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.content = content;
        this.timestamp = Instant.now();
        this.replicationCount = 0;
        this.type = type;
    }
    
    /**
     * Creates a new message with specified type (for non-user messages).
     *
     * @param senderId    The ID of the sender
     * @param recipientId The ID of the recipient
     * @param type        The type of message
     */
    public Message(String senderId, String recipientId, MessageType type) {
        this(senderId, recipientId, null, type);
    }
    
    /**
     * Creates a message with all parameters specified.
     */
    private Message(UUID id, String senderId, String recipientId, String content, 
                   Instant timestamp, int replicationCount, MessageType type,
                   long sequenceNumber, String originServerId, boolean isAcknowledgment,
                   UUID acknowledgedMessageId) {
        this.id = id;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.content = content;
        this.timestamp = timestamp;
        this.replicationCount = replicationCount;
        this.type = type;
        this.sequenceNumber = sequenceNumber;
        this.originServerId = originServerId;
        this.isAcknowledgment = isAcknowledgment;
        this.acknowledgedMessageId = acknowledgedMessageId;
    }

    /**
     * Creates an acknowledgment message for another message.
     * 
     * @param originalMessage The message being acknowledged
     * @param serverId The server sending the acknowledgment
     * @return A new acknowledgment message
     */
    public static Message createAcknowledgment(Message originalMessage, String serverId) {
        Message ack = new Message(
                serverId,
                originalMessage.getOriginServerId(),
                "ACK:" + originalMessage.getId(),
                MessageType.ACK
        );
        ack.isAcknowledgment = true;
        ack.acknowledgedMessageId = originalMessage.getId();
        return ack;
    }

    public UUID getId() {
        return id;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getRecipientId() {
        return recipientId;
    }

    public String getContent() {
        return content;
    }

    /**
     * Gets the timestamp as an Instant
     * 
     * @return The timestamp as an Instant
     */
    public Instant getTimestampInstant() {
        return timestamp;
    }
    
    /**
     * Sets the timestamp for this message
     * 
     * @param timestamp The new timestamp
     */
    public void setTimestampInstant(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public long getTimestamp() {
        return timestamp.toEpochMilli();
    }
    
    @Override
    public void setTimestamp(long epochMillis) {
        this.timestamp = Instant.ofEpochMilli(epochMillis);
    }

    public int getReplicationCount() {
        return replicationCount;
    }

    public void incrementReplicationCount() {
        this.replicationCount++;
    }

    public MessageType getType() {
        return type;
    }
    
    public long getSequenceNumber() {
        return sequenceNumber;
    }
    
    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
    
    public String getOriginServerId() {
        return originServerId;
    }
    
    public void setOriginServerId(String originServerId) {
        this.originServerId = originServerId;
    }
    
    public boolean isAcknowledgment() {
        return isAcknowledgment;
    }
    
    /**
     * Gets the ID of the message being acknowledged, if this is an acknowledgment message.
     * 
     * @return The acknowledged message ID or null
     */
    public UUID getAcknowledgedMessageId() {
        return acknowledgedMessageId;
    }
    
    /**
     * Sets a payload message for this message.
     * Used primarily for forwarding messages in Raft consensus.
     * 
     * @param payload The message to set as payload
     */
    public void setPayload(Message payload) {
        this.payload = payload;
    }
    
    /**
     * Gets the payload message, if any.
     * 
     * @return The payload message or null
     */
    public Message getPayload() {
        return payload;
    }
    
    /**
     * Creates a copy of this message with the specified sequence number and server origin.
     * 
     * @param sequenceNumber The sequence number to set
     * @param originServerId The origin server ID to set
     * @return A new copy of the message with updated fields
     */
    public Message withSequenceAndOrigin(long sequenceNumber, String originServerId) {
        Message copy = new Message(
                this.id,
                this.senderId,
                this.recipientId,
                this.content,
                this.timestamp,
                this.replicationCount,
                this.type,
                sequenceNumber,
                originServerId,
                this.isAcknowledgment,
                this.acknowledgedMessageId
        );
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return Objects.equals(id, message.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", senderId='" + senderId + '\'' +
                ", recipientId='" + recipientId + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                ", replicationCount=" + replicationCount +
                ", type=" + type +
                ", sequenceNumber=" + sequenceNumber +
                ", originServerId='" + originServerId + '\'' +
                (isAcknowledgment ? ", ackFor=" + acknowledgedMessageId : "") +
                '}';
    }
    
    /**
     * Converts a JsonMessage to a Message.
     *
     * @param jsonMessage The JsonMessage to convert
     * @return A new Message with the same content
     */
    public static Message fromJsonMessage(JsonMessage jsonMessage) {
        Message message = new Message(
                jsonMessage.getSenderId(),
                jsonMessage.getRecipientId(),
                jsonMessage.getContent(),
                jsonMessage.getType()
        );
        // Set the replication count to match
        for (int i = 0; i < jsonMessage.getReplicationCount(); i++) {
            message.incrementReplicationCount();
        }
        
        // Transfer sequence number if present
        if (jsonMessage.getSequenceNumber() != null) {
            message.setSequenceNumber(jsonMessage.getSequenceNumber());
        }
        
        return message;
    }

    /**
     * Custom serialization method to handle errors gracefully
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        try {
            ObjectOutputStream.PutField fields = out.putFields();
            fields.put("id", id);
            fields.put("senderId", senderId);
            fields.put("recipientId", recipientId);
            fields.put("content", content);
            fields.put("timestamp", timestamp);
            fields.put("replicationCount", replicationCount);
            fields.put("type", type);
            fields.put("sequenceNumber", sequenceNumber);
            fields.put("originServerId", originServerId);
            fields.put("isAcknowledgment", isAcknowledgment);
            fields.put("acknowledgedMessageId", acknowledgedMessageId);
            fields.put("payload", payload);
            out.writeFields();
        } catch (IOException e) {
            throw new IOException("Error serializing Message: " + e.getMessage(), e);
        }
    }
    
    /**
     * Custom deserialization method to handle errors gracefully
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        try {
            ObjectInputStream.GetField fields = in.readFields();
            
            // Use reflection to set final fields
            try {
                java.lang.reflect.Field idField = Message.class.getDeclaredField("id");
                idField.setAccessible(true);
                idField.set(this, fields.get("id", null));
                
                java.lang.reflect.Field senderIdField = Message.class.getDeclaredField("senderId");
                senderIdField.setAccessible(true);
                senderIdField.set(this, fields.get("senderId", null));
                
                java.lang.reflect.Field recipientIdField = Message.class.getDeclaredField("recipientId");
                recipientIdField.setAccessible(true);
                recipientIdField.set(this, fields.get("recipientId", null));
                
                java.lang.reflect.Field contentField = Message.class.getDeclaredField("content");
                contentField.setAccessible(true);
                contentField.set(this, fields.get("content", null));
                
                java.lang.reflect.Field typeField = Message.class.getDeclaredField("type");
                typeField.setAccessible(true);
                typeField.set(this, fields.get("type", null));
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new IOException("Error setting final fields: " + e.getMessage(), e);
            }
            
            // Set non-final fields directly
            this.timestamp = (Instant) fields.get("timestamp", Instant.now());
            this.replicationCount = fields.get("replicationCount", 0);
            this.sequenceNumber = fields.get("sequenceNumber", -1L);
            this.originServerId = (String) fields.get("originServerId", null);
            this.isAcknowledgment = fields.get("isAcknowledgment", false);
            this.acknowledgedMessageId = (UUID) fields.get("acknowledgedMessageId", null);
            this.payload = (Message) fields.get("payload", null);
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException("Error deserializing Message: " + e.getMessage(), e);
        }
    }
} 