# NoorNote — Project Requirements

NoorNote is a note-taking application built with FastAPI, designed for fast, scalable, and searchable note management.



## 01. Service Inventory

| Service | Docker Image | Port(s) | Role |
|---      |---           |---      |---   |
| PostgreSQL | postgres:16 | 5432 | Relational store for user profiles and account data. |
| MongoDB | mongo:7 | 27017 | Document store for notes and revision logs. |
| Redis | redis:7 | 6379 | Cache layer for hot note reads and session tokens. |
| Kafka | confluentinc/cp-kafka:7.6.0 | 9092 | Event bus for activity streaming and audit events. |
| Elasticsearch | docker.elastic.co/elasticsearch/elasticsearch:8.11.1 | 9200 | Full-text search index for querying note content. |
| FastAPI (×2) | custom build | 8001, 8002 | Application layer exposing the NoorNote REST API (two instances for load distribution). |
| Nginx | nginx:1.25-alpine | 80 | Load balancer and reverse proxy routing traffic to FastAPI instances. |
| Kafka Consumer | custom build | — | Background event processor that consumes Kafka topics for async tasks. |



 
## 02. Resource Estimate


The table below provides a per-service breakdown of the minimum RAM and CPU required to run NoorNote locally in Docker. Figures are conservative development-environment estimates; production deployments would scale these upward.
 
| Service | Min RAM (MB) | Min CPU (cores) | Notes |
|---|---|---|---|
| Elasticsearch | 512 | 0.50 | JVM heap flags `-Xms256m -Xmx256m` must be set to cap memory usage. |
| PostgreSQL | 128 | 0.25 | Default `shared_buffers` configuration is sufficient for development. |
| MongoDB | 128 | 0.25 | WiredTiger cache should be explicitly capped via `wiredTigerCacheSizeGB`. |
| Kafka (KRaft) | 256 | 0.25 | Single-broker KRaft mode — adequate for development and learning purposes. |
| Redis | 64 | 0.10 | Append-only file (AOF) persistence mode enabled for durability. |
| FastAPI (×2) | 128 | 0.25 | Per-instance estimate; both instances share the same custom image. |
| Nginx | 32 | 0.10 | Alpine-based image keeps the footprint minimal. |
| Kafka Consumer | 64 | 0.10 | Single Python process consuming Kafka topics asynchronously. |
| OS + Docker overhead | 512 | 0.50 | Buffer reserved for the host OS kernel and Docker daemon. |
| **TOTAL** | **~1,824 MB (~2 GB)** | **~2.30 cores** | — |

### VM Sizing Recommendation
 
The calculated totals of **~1,824 MB RAM** and **~2.3 vCPU** indicate that a development VM should be provisioned with headroom above the bare minimum. The recommended specification is:
 
- **RAM:** 4 GB — provides ~2 GB of headroom above the calculated floor, accommodating log buffers, Docker layer caching, and spikes during note indexing.
- **vCPU:** 4 cores — ensures no single service starves under concurrent requests, and allows smooth parallel container startup.
> **Note:** Elasticsearch is the single largest consumer at 512 MB. If resources are constrained, reducing its JVM heap to `-Xms128m -Xmx128m` can lower its footprint at the cost of indexing throughput.

 ## Section C — Data Model Summary

NoorNote employs a **hybrid persistence architecture**, distributing data across three specialised stores — each chosen to match the access pattern and shape of the data it owns. PostgreSQL handles structured relational identity data, MongoDB owns the flexible document-oriented note content, and Elasticsearch maintains a derived search index projected from MongoDB. Redis and Kafka hold no durable domain data; they serve as ephemeral cache and event transport respectively.

The table below identifies every collection and table planned for the initial release, the owning service, and the key fields that define its schema.

---

### PostgreSQL — Relational Store

PostgreSQL is the **system of record for user identity**. Its strict schema, ACID guarantees, and unique-constraint enforcement make it the appropriate choice for authentication and account management, where data integrity is non-negotiable.

#### Table: `users`

| Column | Type | Constraints | Description |
|---|---|---|---|
| `id` | `INTEGER` | `PRIMARY KEY`, `INDEX` | Auto-incrementing surrogate key; used as the foreign reference in MongoDB note documents. |
| `email` | `VARCHAR(255)` | `NOT NULL`, `UNIQUE`, `INDEX` | User's login identifier; indexed for fast lookup during authentication. |
| `username` | `VARCHAR(50)` | `NOT NULL`, `UNIQUE`, `INDEX` | Public-facing display name; indexed to support profile lookups. |
| `password_hash` | `VARCHAR(255)` | `NOT NULL` | Bcrypt hash of the user's password; the plaintext is never persisted. |
| `is_active` | `BOOLEAN` | `NOT NULL`, `DEFAULT TRUE` | Soft-enable/disable flag; inactive users are denied authentication without record deletion. |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL`, `DEFAULT NOW()` | Server-side timestamp set at row insertion; used for audit trails and account age queries. |

> **Ownership note:** No other service writes to the `users` table. Downstream services (e.g., the Kafka Consumer and FastAPI instances) reference the `id` field as a logical foreign key within their own stores, but they never perform direct writes to PostgreSQL.

---

### MongoDB — Document Store

MongoDB is the **system of record for note content**. Its schema-flexible document model accommodates the variable structure of user notes — rich text, tags, nested metadata, and revision history — without requiring costly migrations as the schema evolves.

#### Collection: `notes`

| Field | BSON Type | Description |
|---|---|---|
| `_id` | `ObjectId` | MongoDB-generated primary key for the document. |
| `user_id` | `Integer` | Logical foreign key referencing `users.id` in PostgreSQL; scopes the note to its owner. |
| `title` | `String` | Short heading of the note. |
| `content` | `String` | Full body text of the note; the primary field indexed in Elasticsearch. |
| `tags` | `Array<String>` | User-defined labels for client-side filtering and categorisation. |
| `is_deleted` | `Boolean` | Soft-delete flag; deleted notes are excluded from queries but retained for recovery. |
| `created_at` | `Date` | Timestamp of initial note creation. |
| `updated_at` | `Date` | Timestamp of the most recent update; maintained by the application layer on every write. |

#### Collection: `activity_logs`

| Field | BSON Type | Description |
|---|---|---|
| `_id` | `ObjectId` | MongoDB-generated primary key. |
| `user_id` | `Integer` | References `users.id`; identifies the actor. |
| `action` | `String` | Event type (e.g., `note.created`, `note.updated`, `note.deleted`). |
| `note_id` | `ObjectId` | References the affected `notes` document. |
| `timestamp` | `Date` | Server-side time at which the event was recorded. |
| `metadata` | `Object` | Arbitrary key-value payload carrying additional event context (e.g., changed fields, client IP). |

> **Ownership note:** Activity log documents are written by the **Kafka Consumer** service after it processes events published to the Kafka topic. The FastAPI application layer writes directly to the `notes` collection.

---

### Elasticsearch — Search Index

Elasticsearch holds **no source-of-truth data**. It maintains a derived, read-optimised index projected from the `notes` collection in MongoDB. Documents are synchronised asynchronously via the Kafka pipeline: FastAPI publishes a note-change event to Kafka, the Kafka Consumer processes it, and upserts the corresponding Elasticsearch document.

#### Index: `notes`

| Field | ES Type | Description |
|---|---|---|
| `note_id` | `keyword` | Stores the MongoDB `_id` as a string; used as the document `_id` in Elasticsearch for idempotent upserts. |
| `user_id` | `integer` | Enables per-user search scoping (`term` filter on all queries). |
| `title` | `text` | Analysed field; participates in full-text `multi_match` queries. |
| `content` | `text` | Primary analysed field for full-text search across note bodies. |
| `tags` | `keyword` | Exact-match filter field; not analysed. |
| `updated_at` | `date` | Used for relevance boosting and recency-sorted result sets. |

> **Ownership note:** Elasticsearch is written to exclusively by the **Kafka Consumer**. The FastAPI layer queries Elasticsearch for search endpoints but never writes to it directly, preserving a clean unidirectional data flow.

---

### Redis — Ephemeral Cache

Redis stores **no domain model data**. It serves as a short-lived cache layer with two responsibilities:

- **Note cache:** Serialised JSON representations of frequently read notes, keyed by `note:{note_id}`, with a TTL aligned to the expected read-to-write ratio.
- **Session tokens:** JWT or opaque session tokens keyed by `session:{user_id}`, enabling fast token validation without a PostgreSQL round-trip on every request.

No Redis data requires migration planning or schema documentation; all keys are considered disposable and are reconstructible from PostgreSQL or MongoDB.

---

### Architecture Data-Flow Summary

```
[Client]
    │
    ▼
[Nginx :80]  ──────────────────────────────────────┐
    │                                              │
    ▼                                              ▼
[FastAPI :8001]                             [FastAPI :8002]
    │                                              
    |                                        
    ├── READ/WRITE ──► [PostgreSQL]  (users)
    ├── READ/WRITE ──► [MongoDB]     (notes, activity_logs)
    ├── READ       ──► [Redis]       (note cache, sessions)
    ├── READ       ──► [Elasticsearch] (full-text search)
    └── PUBLISH    ──► [Kafka :9092]
                            │
                            ▼
                   [Kafka Consumer]
                            │
                            ├── WRITE ──► [MongoDB]       (activity_logs)
                            └── UPSERT ─► [Elasticsearch] (notes index)
```
