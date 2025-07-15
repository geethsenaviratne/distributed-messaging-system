package com.ds.messaging.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Enhanced message implementation that uses Jackson for JSON serialization.
 * This provides better performance and interoperability than Java serialization.
 */
public class JsonMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    
    private final UUID id;
    private final String senderId;
    @JsonAlias({"receiverId"})
    private final String recipientId;
    private final String content;
    private final Instant timestamp;
    private int replicationCount;
    private final MessageType type;
    private Long sequenceNumber;
    private UUID inReplyTo;
    
    /**
     * Creates a new message with auto-generated ID and timestamp.
     *
     * @param senderId    The ID of the sender
     * @param recipientId The ID of the recipient
     * @param content     The message content
     * @param type        The type of message
     */
    public JsonMessage(String senderId, String recipientId, String content, MessageType type) {
        this(UUID.randomUUID(), senderId, recipientId, content, Instant.now(), 0, type, null);
    }
    
    /**
     * Creates a new message with the specified attributes.
     *
     * @param id               The message ID
     * @param senderId         The ID of the sender
     * @param recipientId      The ID of the recipient
     * @param content          The message content
     * @param timestamp        The message timestamp
     * @param replicationCount The number of times the message has been replicated
     * @param type             The type of message
     * @param sequenceNumber   Optional sequence number for ordering
     */
    @JsonCreator
    public JsonMessage(
            @JsonProperty("id") UUID id,
            @JsonProperty("senderId") String senderId,
            @JsonProperty("recipientId") String recipientId,
            @JsonProperty("content") String content,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("replicationCount") int replicationCount,
            @JsonProperty("type") MessageType type,
            @JsonProperty("sequenceNumber") Long sequenceNumber) {
        this.id = id;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.content = content;
        this.timestamp = timestamp;
        this.replicationCount = replicationCount;
        this.type = type;
        this.sequenceNumber = sequenceNumber;
        this.inReplyTo = null;
    }
    
    /**
     * Creates a copy of this message with an incremented replication count.
     *
     * @return A new message with incremented replication count
     */
    @JsonIgnore
    public JsonMessage incrementReplication() {
        return new JsonMessage(
                id, senderId, recipientId, content, timestamp, replicationCount + 1, type, sequenceNumber
        );
    }
    
    /**
     * Creates a copy of this message with a sequence number.
     *
     * @param sequenceNumber The sequence number to set
     * @return A new message with the specified sequence number
     */
    @JsonIgnore
    public JsonMessage withSequenceNumber(long sequenceNumber) {
        return new JsonMessage(
                id, senderId, recipientId, content, timestamp, replicationCount, type, sequenceNumber
        );
    }
    
    /**
     * Creates a copy of this message as a reply to another message.
     *
     * @param originalMessageId The ID of the message being replied to
     * @return A new message with the inReplyTo field set
     */
    @JsonIgnore
    public JsonMessage asReplyTo(UUID originalMessageId) {
        JsonMessage reply = new JsonMessage(
                id, senderId, recipientId, content, timestamp, replicationCount, type, sequenceNumber
        );
        reply.inReplyTo = originalMessageId;
        return reply;
    }
    
    // Getters
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
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public int getReplicationCount() {
        return replicationCount;
    }
    
    public MessageType getType() {
        return type;
    }
    
    public Long getSequenceNumber() {
        return sequenceNumber;
    }
    
    public UUID getInReplyTo() {
        return inReplyTo;
    }
    
    /**
     * Sets the ID of the message this is replying to.
     * 
     * @param inReplyTo The message ID this is in reply to
     */
    public void setInReplyTo(UUID inReplyTo) {
        this.inReplyTo = inReplyTo;
    }
    
    /**
     * Converts this message to a JSON string.
     *
     * @return JSON representation of this message
     * @throws IOException If serialization fails
     */
    public String toJson() throws IOException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }
    
    /**
     * Creates a message from a JSON string.
     *
     * @param json The JSON string
     * @return The deserialized message
     * @throws IOException If deserialization fails
     */
    public static JsonMessage fromJson(String json) throws IOException {
        return OBJECT_MAPPER.readValue(json, JsonMessage.class);
    }
    
    /**
     * Creates a message from a byte array.
     *
     * @param bytes The byte array
     * @return The deserialized message
     * @throws IOException If deserialization fails
     */
    public static JsonMessage fromBytes(byte[] bytes) throws IOException {
        return OBJECT_MAPPER.readValue(bytes, JsonMessage.class);
    }
    
    /**
     * Converts this message to a byte array.
     *
     * @return Byte array representation of this message
     * @throws IOException If serialization fails
     */
    public byte[] toBytes() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(this);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonMessage message = (JsonMessage) o;
        return Objects.equals(id, message.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "JsonMessage{" +
                "id=" + id +
                ", senderId='" + senderId + '\'' +
                ", recipientId='" + recipientId + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                ", replicationCount=" + replicationCount +
                ", type=" + type +
                ", sequenceNumber=" + sequenceNumber +
                ", inReplyTo=" + inReplyTo +
                '}';
    }
    
    /**
     * Convert a legacy Message to a JsonMessage.
     *
     * @param legacyMessage The legacy message
     * @return A new JsonMessage with the same content
     */
    public static JsonMessage fromLegacyMessage(Message legacyMessage) {
        return new JsonMessage(
                legacyMessage.getId(),
                legacyMessage.getSenderId(),
                legacyMessage.getRecipientId(),
                legacyMessage.getContent(),
                legacyMessage.getTimestampInstant(),
                legacyMessage.getReplicationCount(),
                legacyMessage.getType(),
                null
        );
    }
    
    /**
     * Get the recipient ID with a more clearly named getter for test compatibility.
     * 
     * @return The recipient ID
     */
    public String getReceiverId() {
        return getRecipientId();
    }
    
    /**
     * Set the ID of this message (for testing purposes).
     * 
     * @param id The ID to set
     */
    public void setId(UUID id) {
        // This is a workaround since the field is final
        // In a real implementation, we would use a builder pattern instead
        try {
            java.lang.reflect.Field idField = JsonMessage.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(this, id);
        } catch (Exception e) {
            // Silently fail in this case
        }
    }
} 