# Distributed Messaging System

A high-performance, fault-tolerant distributed messaging system implemented in Java.

## Team Members

Member 1 - Perera B S K J - it23223394 -it23223394@my.sliit.lk

Member 2 - Hirunima H K G L - it23194380 - it23194380@my.sliit.lk

Member 3 - Seneviratne D M D G B - it23226128 - it23226128@my.sliit.lk

Member 4 - Linash M V M - it23442566 -it23442566@my.sliit.lk

## Overview

This distributed messaging system provides a scalable platform for real-time message exchange between client nodes, with server nodes handling message routing, persistence, and failover capabilities. The system supports various types of messages including user messages, heartbeats, and control messages.

## Prerequisites
1. Java 21 installed (JDK 21.0.5 or later)
2. Eclipse IDE (2023-12 or newer )
3. Maven plugin for Eclipse 

## Import the Project
1. Open Eclipse
2. Select File > Import...
3. Choose "Existing Maven Projects" under Maven folder
4. Browse to the directory containing the project
5. Select the project (pom.xml should be detected)
6. Click Finish

## Configure Java 21
1. Right-click on the project in Project Explorer
2. Select Properties
3. Navigate to Java Build Path > Libraries
4. Ensure JRE System Library shows [JavaSE-21]
5. If not, remove existing JRE and add Library > JRE System Library > Workspace default JRE (which should be set to Java 21)

## Build the Project (for eclipse)
1. Right-click on the project in Project Explorer
2. Select Run As > Maven clean
3. Right-click again and select Run As > Maven install

## Run the Application
### Option 1: From Eclipse
1. Right-click on the main class file (e.g., com.ds.messaging.server.ServerMain)
2. Select Run As > Java Application
3. For multiple server testing, create separate run configurations with appropriate arguments
    - server1 - arguments-server1 9000 localhost:9001
    - server2 - arguments-server2 9001 localhost:9000
    - client1 - arguments-client1 localhost 9000 localhost:9001
    - client2 - arguments-client2 localhost 9001 localhost:9000
    

### Option 2: Using Batch Files (Windows)
1. Open a command prompt in the project directory
2. Run one of the provided batch files:
  
## Basic System Operation
- **run-system.bat**: Starts the complete system with primary-backup replication
- **run-socket-with-raft.bat**: Starts the system with Raft consensus for stronger consistency
- **stop-system.bat**: Gracefully shuts down all system components

## Individual Components
- **run-server.bat**: Starts a single server node
- **run-client.bat**: Starts a client node

## Testing
- **test-replication.bat**: Runs replication tests
- **run-performance-test.bat**: Tests system performance

## Maintenance
- **cleanup.bat**: Removes logs and temporary files
  
## Troubleshooting
- If you encounter build errors:
  - Right-click project > Maven > Update Project...
  - Ensure you have Java 21 set as the default JRE in Eclipse (Window > Preferences > Java > Installed JREs)
- If Eclipse doesn't recognize Java 21:
  - Window > Preferences > Java > Installed JREs > Add... > Standard VM > Directory (select your JDK 21 installation folder)
  - Make it the default by checking the box next to it


## Building the Project

```bash
# Clean and compile the project
mvn clean compile

# Run all tests
mvn test

# Package the application without running tests
mvn package -DskipTests
```

### Starting Individual Components Manually

#### Start a Server Node
```bash
# Basic server on port 9000
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain server1 9000

# Server with peer connections
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain server2 9001 localhost:9000

# Server with Raft consensus enabled
java -Draft.enabled=true -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.server.ServerMain server3 9002 localhost:9000 localhost:9001
```

#### Start a Client Node
```bash
# Connect to a server on localhost:9000
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain client1 localhost 9000

# Connect with backup servers
java -cp target/distributed-messaging-system-1.0-SNAPSHOT-jar-with-dependencies.jar com.ds.messaging.client.ClientMain client2 localhost 9000 localhost:9001
```
## Architecture

### Key Components

1. **Common Components**
   - `Node` interface - Base interface for both clients and servers
   - `AbstractNode` - Common functionality for nodes
   - `Message` - Message objects with serialization capabilities
   - `MessageType` - Enum for different message types (USER_MESSAGE, HEARTBEAT, etc.)

2. **Client**
   - `ClientNode` - Socket-based client implementation
   - `ClientMain` - Entry point for client applications

3. **Server**
   - `ServerNode` - Socket-based server implementation
   - `ServerMain` - Entry point for server applications
   - `InMemoryMessageStore` - Message persistence

4. **Replication Strategies**
   - **Primary-Backup** - Default replication strategy
   - **Raft Consensus** - Optional strong consistency with leader election

5. **Fault Tolerance**
   - Heartbeat-based failure detection
   - Server reconnection with exponential backoff
   - Degraded mode operation

## Network Communication

The system uses Java sockets for network communication:

1. **Message Serialization**: Java Serialization for Socket communication
2. **Connections**: TCP connections for reliable message delivery
3. **Fault Handling**: Timeout mechanisms and reconnection strategies

## Implementation Details

- Built with Java 21
- Uses Java Sockets for communication
- Uses SLF4J/Logback for logging

## Server Menu Options
1. Check server status - Shows current server status (RUNNING, DEGRADED, etc.)
2. List connected nodes - Shows all connected clients and peer servers
3. Simulate temporary degraded mode - Simulates server degradation for 10 seconds
4. Exit - Stops the server gracefully

## Client Menu Options
1. Send a message - Send a message to another client
2. Check for new messages - Check if you have any new messages
3. Check client status - Shows client connection status
4. Exit - Stops the client gracefully
