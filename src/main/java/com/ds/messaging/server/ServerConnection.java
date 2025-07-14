package com.ds.messaging.server;

import com.ds.messaging.common.Message;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class ServerConnection {
    private static final int MAX_CONSECUTIVE_ERRORS = 5; // Increased from 3 to 5
    private static final long ERROR_WINDOW_MS = 30000; // 30 second window for error counting
    private final Queue<Long> errorTimestamps = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean broken = new AtomicBoolean(false);
    private final Logger logger = Logger.getLogger(ServerConnection.class.getName());
    
    private Socket socket;
    private ObjectOutputStream out;
    private ObjectInputStream in;

    public ServerConnection(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new ObjectOutputStream(socket.getOutputStream());
        this.in = new ObjectInputStream(socket.getInputStream());
    }

    private void handleError(Exception e) {
        long now = System.currentTimeMillis();
        
        // Clean up old error timestamps
        while (!errorTimestamps.isEmpty() && now - errorTimestamps.peek() > ERROR_WINDOW_MS) {
            errorTimestamps.poll();
        }
        
        errorTimestamps.offer(now);
        
        if (errorTimestamps.size() >= MAX_CONSECUTIVE_ERRORS) {
            logger.severe(String.format("Too many errors in %d seconds, marking connection as broken", ERROR_WINDOW_MS / 1000));
            markAsBroken();
        }
    }

    private void markAsBroken() {
        if (broken.compareAndSet(false, true)) {
            logger.warning("Connection marked as broken, will attempt to reconnect");
            try {
                socket.close();
            } catch (IOException e) {
                logger.fine("Error closing broken socket: " + e.getMessage());
            }
        }
    }

    private void resetErrorCount() {
        errorTimestamps.clear();
    }

    public void sendMessage(Message message) throws IOException {
        if (broken.get()) {
            throw new IOException("Connection is broken");
        }
        
        try {
            synchronized (out) {
                out.writeObject(message);
                out.flush();
            }
            resetErrorCount();
        } catch (IOException e) {
            handleError(e);
            throw e;
        }
    }

    public Message readMessage() throws IOException, ClassNotFoundException {
        if (broken.get()) {
            throw new IOException("Connection is broken");
        }
        
        try {
            Message message = (Message) in.readObject();
            resetErrorCount();
            return message;
        } catch (IOException | ClassNotFoundException e) {
            handleError(e);
            throw e;
        }
    }
    
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
} 