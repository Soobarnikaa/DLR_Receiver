# DLR Receiver

A Java-based Delivery Report (DLR) receiver service for processing and managing message delivery reports with Redis queue management and NATS messaging integration.

## Overview

DLR Receiver is a Spring Boot application that handles delivery reports from messaging systems, processes them through Redis queues, and manages billing retry workflows. The service includes spillover handling, TTL scanning, and Griffin flag cache management.

## Features

- **DLR Processing**: Receives and processes delivery reports via REST endpoints
- **Redis Integration**: Queue management and caching with Redis
- **NATS Messaging**: Message publishing through NATS
- **Billing Retry**: Automated billing retry worker with configurable intervals
- **Spillover Management**: Plugin API spillover service for handling overflow scenarios
- **TTL Scanner**: Scheduled scanning for time-to-live management
- **Griffin Flag Cache**: Distributed cache management for feature flags

## Architecture

### Core Components

- **DlrReceiverController**: REST API endpoints for receiving DLRs
- **DlrReceiverService**: Core business logic for DLR processing
- **RedisQueueService**: Queue operations and management
- **BillingRetryWorker**: Background worker for retry logic
- **DlrWorker**: Main DLR processing worker
- **TTLScannerScheduler**: Scheduled tasks for TTL management

### Configuration

- **NatsInitializer**: NATS connection initialization
- **DbTemplateProvider**: Database template configuration
- **AppConfig**: Application-wide configuration management

## Prerequisites

- Java 8 or higher
- Redis server
- NATS server
- Maven or Gradle (build tool)

## Configuration

Application configuration is managed through `application.properties`. Key configuration areas include:

- Redis connection settings
- NATS server configuration
- Database connection parameters
- Worker thread pool settings
- TTL scanner intervals

## Building

```bash
# Using Maven
mvn clean install

# Using Gradle
gradle build
```

## Running

```bash
java -jar target/dlr-receiver.jar
```

## API Endpoints

### Health Check
```
GET /health
```

### DLR Reception
Refer to [DlrReceiverController.java](src/main/java/com/karix/dlrreceiver/controller/DlrReceiverController.java) for specific endpoint documentation.

## Exception Handling

The application includes comprehensive exception handling:

- **GlobalExceptionHandler**: Centralized exception handling
- **CriticalInitializationException**: Startup failure handling
- **QueueException**: Queue operation error handling

## Graceful Shutdown

The service implements graceful shutdown through [ShutdownHook.java](src/main/java/com/karix/dlrreceiver/lifecycle/ShutdownHook.java), ensuring proper cleanup of resources and in-flight messages.

## Development

### Project Structure

```
src/
├── main/
│   ├── java/com/karix/dlrreceiver/
│   │   ├── bootstrap/        # Worker initialization
│   │   ├── cache/            # Redis and cache management
│   │   ├── config/           # Configuration classes
│   │   ├── controller/       # REST controllers
│   │   ├── exception/        # Exception handling
│   │   ├── lifecycle/        # Application lifecycle
│   │   ├── service/          # Business logic
│   │   ├── spillover/        # Spillover handling
│   │   └── util/             # Utility classes
│   └── resources/
│       └── application.properties
└── test/                     # Test files
```

## License

[Specify your license]

## Contact

[Add contact information or link to issue tracker]
