# NoorNote — Project Requirements

NoorNote is a note-taking application built with FastAPI, designed for fast, scalable, and searchable note management.



## Section A — Service Inventory

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



 
## Section B — Resource Estimate


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

### Kafka — Event Payload Schema

Kafka holds **no persistent data** beyond its configurable retention window. However, every message published to the `note-events` topic must conform to a defined JSON schema so the Kafka Consumer can deserialise and process it deterministically before writing to MongoDB and Elasticsearch.

#### Topic: `note-events`

| Field | JSON Type | Description |
|---|---|---|
| `event_type` | `string` | Action that triggered the event — one of `note.created`, `note.updated`, `note.deleted`. |
| `user_id` | `integer` | References `users.id` in PostgreSQL; identifies the actor who triggered the event. |
| `resource_id` | `string` (ObjectId) | References the affected document in the MongoDB `notes` collection. |
| `timestamp` | `string` (ISO 8601) | UTC timestamp of when FastAPI published the event to the topic. |
| `metadata` | `object` | Arbitrary key-value payload carrying additional event context (e.g., `title`, `content`, `tags` at time of event). |

**Example message:**
```json
{
  "event_type": "note.created",
  "user_id": 42,
  "resource_id": "664f1a2b3c4d5e6f7a8b9c0d",
  "timestamp": "2025-05-22T10:34:00Z",
  "metadata": {
    "title": "Meeting Notes",
    "content": "Discussed Q3 roadmap...",
    "tags": ["work", "q3"]
  }
}
```

> **Ownership note:** FastAPI is the **sole producer** to this topic. The Kafka Consumer is the **sole consumer** — it reads each event, writes an `activity_log` document to MongoDB, and upserts the note into the Elasticsearch index. The `event_id` field enables the consumer to safely handle duplicate deliveries without creating duplicate log entries.

---

### Elasticsearch — Search Index
 
Elasticsearch holds **no source-of-truth data**. It maintains a derived, read-optimised index projected from the `notes` collection in MongoDB. Documents are indexed on `POST /notes`, re-indexed on `PUT /notes/{id}`, and removed on `DELETE /notes/{id}`, keeping the index in sync with MongoDB at all times.
 
#### Index: `notes`
 
| Field | ES Type | Search Behaviour | Lifecycle Event |
|---|---|---|---|
| `note_id` | `keyword` | Used as the document `_id` for idempotent upserts; never analysed. | Set on index; unchanged on re-index. |
| `user_id` | `integer` | `term` filter applied on every query to scope results to the requesting user. | Set on index; unchanged on re-index. |
| `title` | `text` | Participates in `multi_match` query; **boosted 3×** over `content` for relevance scoring. Highlighting enabled. | Indexed on `POST`; re-indexed on `PUT`; document deleted on `DELETE`. |
| `content` | `text` | Primary full-text analysed field. `fuzziness: AUTO` applied on all `GET /search` queries. Highlighting enabled. | Indexed on `POST`; re-indexed on `PUT`; document deleted on `DELETE`. |
| `tags` | `keyword` | Exact-match `terms` filter; not analysed. | Indexed on `POST`; re-indexed on `PUT`; document deleted on `DELETE`. |
| `created_at` | `date` | Available for range filters and recency-based result sorting. | Set once on `POST`; never mutated. |
 
**Query strategy for `GET /search?q={term}`:**
 
```json
{
  "query": {
    "multi_match": {
      "query": "<term>",
      "fields": ["title^3", "content"],
      "fuzziness": "AUTO"
    }
  },
  "highlight": {
    "fields": {
      "title": {},
      "content": {}
    }
  },
  "sort": ["_score"]
}
```
 
> **Ownership note:** Elasticsearch is written to exclusively by the **Kafka Consumer** — FastAPI publishes a `note-events` Kafka message on every write operation, and the consumer performs the corresponding index, re-index, or delete. The FastAPI layer only **reads** from Elasticsearch (via `GET /search`), preserving a clean unidirectional write flow.
 
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
    │                                               │
    ▼                                               ▼
[FastAPI :8001]                             [FastAPI :8002]
    │                                               │
    ├── READ/WRITE ──► [PostgreSQL]    (users)
    ├── READ/WRITE ──► [MongoDB]      (notes)
    ├── READ       ──► [Redis]        (note cache, sessions)
    ├── READ       ──► [Elasticsearch](full-text search)
    └── PUBLISH    ──► [Kafka :9092]
                            │
                            ▼
                   [Kafka Consumer]
                            │
                            ├── WRITE ──► [MongoDB]        (activity_logs)
                            └── UPSERT ─► [Elasticsearch]  (notes index)
```
