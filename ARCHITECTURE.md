# System Architecture & Design Decisions

This document outlines the design, architectural patterns, and engineering trade-offs made during the development of the Event-Driven Notification Service.

---

## 1. Architectural Patterns

### Event-Driven Architecture (EDA)
The system separates the reception of user activities from the generation of notifications using a decoupled, publisher-subscriber model. 
*   **Decoupled Microservices**: The `api-service` (publisher) does not block or wait for notifications to be constructed. It immediately responds with `202 Accepted` to the client upon publishing to Kafka.
*   **Asynchronous Processing**: The `consumer-service` consumes events and processes them at its own pace. If traffic spikes occur, Kafka acts as a buffer, preventing the database or API service from being overwhelmed.

---

## 2. Idempotency Strategy (Exactly-Once Semantics)

In distributed event streams, network partitions or consumer restarts can cause message duplication. The consumer service guarantees **idempotency** (exactly-once processing) through a two-layered defense:

```
[Kafka Event Recieved]
         │
         ▼
[Check DB: isEventProcessed?] ──(Yes)──> [Skip Processing (Log Info)]
         │
        (No)
         ▼
[Attempt INSERT to MySQL] ────(Duplicate Key Error 1062)──> [Skip (Log Warning)]
         │
     (Success)
         ▼
[Notification Created & Committed]
```

1.  **Application-Level Check**: The consumer queries the `notifications` table for the incoming `processed_event_id`. If it exists, it safely acknowledges and skips processing.
2.  **Database Constraint Check**: A UNIQUE constraint is applied to `processed_event_id`. In a high-concurrency environment (e.g., duplicate events delivered simultaneously to different partitions or multiple consumers), this database-level constraint prevents race conditions. Any duplicate inserts trigger an `ER_DUP_ENTRY` error, which the consumer catches, logs, and handles gracefully without crashing or duplicating notifications.

---

## 3. Database Design & Performance

### Notifications Table
```sql
CREATE TABLE IF NOT EXISTS notifications (
    notification_id VARCHAR(36) PRIMARY KEY,
    recipient_user_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    message_content TEXT NOT NULL,
    status ENUM('unread', 'read') DEFAULT 'unread',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_event_id VARCHAR(36) UNIQUE NOT NULL
);
```

### Query Optimization
An index is established on the composite keys `(recipient_user_id, status)`:
```sql
CREATE INDEX idx_recipient_user_id_status ON notifications (recipient_user_id, status);
```
*   **Rationale**: The primary operation for notification services is fetching *unread* notifications for a specific user (`GET /api/users/{userId}/notifications`). The composite index ensures this lookup is extremely fast ($O(\log N)$) even with millions of records.

---

## 4. Resilience & Fault Tolerance

### Startup Dependency Ordering
Using health checks in `docker-compose.yml`, container starts are ordered:
1.  **ZooKeeper** starts and performs clients check.
2.  **Kafka** starts and validates connection with ZooKeeper.
3.  **MySQL** boots and initializes schema.
4.  **API Service** and **Consumer Service** start only when both Kafka and MySQL health checks report `healthy`.

### Database Connection Retry
The `consumer-service` implements an active retry loop for DB connections during startup:
*   It attempts connection up to 15 times with a 3-second delay, ensuring that temporary MySQL startup delays do not crash the service.

### Fail-Safe Message Handling
If the consumer encounters an unparseable message (invalid JSON) or processing fails for a specific event:
*   The error is caught and logged.
*   The offset is committed, and the consumer moves to the next message.
*   **DLQ Recommendation**: In production, these failed messages should be routed to a Dead-Letter Queue (DLQ) topic (`user-activity-dlq`) for inspection rather than being ignored.
