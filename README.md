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
    │                                              │
    ▼                                              ▼
[FastAPI :8001]                             [FastAPI :8002]
    │                                              │
    ├── READ/WRITE ──► [PostgreSQL]    (users)     ▼
    ├── READ/WRITE ──► [MongoDB]       (notes)
    ├── READ       ──► [Redis]         (note cache, sessions)
    ├── READ       ──► [Elasticsearch] (full-text search)
    └── PUBLISH    ──► [Kafka :9092]
                            │
                            ▼
                   [Kafka Consumer]
                            │
                            ├── WRITE ──► [MongoDB]        (activity_logs)
                            └── UPSERT ─► [Elasticsearch]  (notes index)
```



## Section D — Endpoint Inventory
 
NoorNote's REST API is delivered across four incremental phases. Each phase introduces new services and capabilities while preserving full backward compatibility with endpoints defined in prior phases. Endpoints marked **Protected** require a valid JWT Bearer token in the `Authorization` header.
 
---
 
### Phase 1 — Foundation: Auth, Users, and Notes
 
**Services:** PostgreSQL · MongoDB · FastAPI
**Goal:** A working authenticated API backed by two databases.
 
| Method | Path | Protection | Purpose |
|---|---|---|---|
| `POST` | `/auth/signup` | Public | Accept `email`, `username`, `password`. Hash password with bcrypt and store the new user in PostgreSQL. Return `UserOut` (no password). |
| `POST` | `/auth/login` | Public | Accept `username` and `password` via OAuth2 form data. Verify credentials against PostgreSQL. Return a signed JWT Bearer token. |
| `GET` | `/profile` | Protected | Return the authenticated user's profile record from PostgreSQL. |
| `POST` | `/notes` | Protected | Accept `title`, `content`, `tags`. Store a new note document in MongoDB scoped to the authenticated user's ID. Return `NoteOut`. |
| `GET` | `/notes` | Protected | Return all note documents belonging to the authenticated user from MongoDB. |
| `GET` | `/notes/{id}` | Protected | Return a single note by its MongoDB `ObjectId`. |
| `PUT` | `/notes/{id}` | Protected | Update one or more note fields in MongoDB. Enforces ownership — only the note's author may update. |
| `DELETE` | `/notes/{id}` | Protected | Delete a note document from MongoDB. Enforces ownership — only the note's author may delete. |
| `GET` | `/users/{user_id}/notes` | Protected | Hybrid endpoint: verify the target user exists in PostgreSQL, then fetch and return all their notes from MongoDB. |
 
---
 
### Phase 2 — Search and Caching
 
**Services added:** Elasticsearch · Redis
**Goal:** Sub-10 ms full-text search and cached hot note reads.
 
> Phase 2 extends three existing endpoints in-place and introduces one new endpoint. No Phase 1 contracts are broken.
 
| Method | Path | Protection | Purpose |
|---|---|---|---|
| `POST` | `/notes` *(extended)* | Protected | After writing to MongoDB, index the note in Elasticsearch with fields `title`, `content`, `tags`, `created_at`. |
| `PUT` | `/notes/{id}` *(extended)* | Protected | After updating MongoDB, re-index the document in Elasticsearch and invalidate the Redis cache key `note:{id}`. |
| `DELETE` | `/notes/{id}` *(extended)* | Protected | After deleting from MongoDB, remove the document from the Elasticsearch index and delete the Redis cache key `note:{id}`. |
| `GET` | `/notes/{id}` *(extended)* | Protected | Check Redis first (key `note:{id}`). On **cache miss**: query MongoDB, write result to Redis with `TTL = 3600 s`, return note. On **cache hit**: return immediately with response header `Cache: HIT`. |
| `GET` | `/search?q={term}` | Protected | Execute a `multi_match` Elasticsearch query across `title` (boosted **3×**) and `content` with `fuzziness: AUTO`. Return results sorted by `_score` with highlighted snippets. |
 
---
 
### Phase 3 — Event Streaming with Kafka
 
**Services added:** Kafka (KRaft) · Kafka Consumer
**Goal:** Decouple activity logging entirely from the synchronous request path.
 
> Phase 3 adds non-blocking Kafka publish calls to existing write endpoints and introduces one new read endpoint. All existing response contracts remain unchanged.
 
**Producer configuration:** An `aiokafka` `AIOKafkaProducer` is initialised in the FastAPI application lifespan and publishes fire-and-forget events to the topic `noornote_events`.
 
**Event payload structure** (published on every action below):
 
```json
{
  "event_type": "<action>",
  "user_id": "<integer>",
  "resource_id": "<ObjectId | null>",
  "timestamp": "<ISO 8601 UTC>",
  "metadata": {}
}
```
 
| Method | Path | Protection | Kafka Event Published | Purpose |
|---|---|---|---|---|
| `POST` | `/auth/signup` *(extended)* | Public | `user_signup` | Publish signup event after user creation in PostgreSQL. |
| `POST` | `/auth/login` *(extended)* | Public | `user_login` | Publish login event after successful credential verification. |
| `POST` | `/notes` *(extended)* | Protected | `note_created` | Publish note creation event after MongoDB write and Elasticsearch index. |
| `PUT` | `/notes/{id}` *(extended)* | Protected | `note_updated` | Publish note update event after MongoDB write and Elasticsearch re-index. |
| `DELETE` | `/notes/{id}` *(extended)* | Protected | `note_deleted` | Publish note deletion event after MongoDB delete and Elasticsearch removal. |
| `GET` | `/search?q={term}` *(extended)* | Protected | `note_searched` | Publish search event after Elasticsearch query executes. |
| `GET` | `/activity` | Protected | — | Return the last 20 `activity_logs` documents for the authenticated user from MongoDB, sorted by `timestamp` descending. |
 
**Consumer:** A standalone process in `consumer/consumer.py` subscribes to `noornote_events` and writes each consumed event as a document to the MongoDB `activity_logs` collection.
 
---
 
### Phase 4 — GraphQL Interface
 
**Services:** No new services — mounts onto the existing FastAPI instance at `/graphql`.
**Goal:** Expose the full NoorNote data graph through a single unified GraphQL endpoint, mirroring and extending all Phase 1–3 capabilities.
 
#### Types
 
| Type | Fields | Resolver Notes |
|---|---|---|
| `User` | `id`, `username`, `email`, `createdAt`, `notes`, `activityLogs` | `notes` resolves to MongoDB `notes` collection; `activityLogs` resolves to MongoDB `activity_logs` collection. |
| `Note` | `id`, `userId`, `title`, `content`, `tags`, `createdAt`, `author` | `author` resolves to the PostgreSQL `users` record via `user_id`. |
| `ActivityLog` | `id`, `eventType`, `userId`, `resourceId`, `timestamp`, `metadata` | Reads directly from MongoDB `activity_logs` collection. |
 
#### Queries
 
| Query | Protection | Purpose |
|---|---|---|
| `me` | Protected | Return the authenticated user's profile by reading the JWT from GraphQL context. |
| `user(id: ID!)` | Protected | Return a single user by PostgreSQL `id`. |
| `users` | Protected | Return all registered users from PostgreSQL. |
| `note(id: ID!)` | Protected | Return a single note by its MongoDB `ObjectId`. |
| `notes` | Protected | Return all notes belonging to the authenticated user from MongoDB. |
 
#### Mutations
 
| Mutation | Signature | Protection | Purpose |
|---|---|---|---|
| `createNote` | `createNote(title: String!, content: String!, tags: [String!]!)` | Protected | Create a note with full Phase 2–3 side-effects: MongoDB write, Elasticsearch index, and Kafka `note_created` event publish. |
| `updateUser` | `updateUser(id: ID!, username: String, email: String)` | Protected | Update the authenticated user's `username` or `email` in PostgreSQL. |

---



## Section E — Architecture Decision Log (ADL)

An Architecture Decision Log records the key technical choices made during the design of NoorNote. Each entry follows the structure: **Context** (the situation that forced a choice), **Decision** (what was chosen), and **Rationale** (why this option was selected over the alternatives and what trade-offs were accepted). Entries are immutable — if a decision is reversed, a new ADR is added rather than editing the original.

---
<details>
<summary><strong>ADR-001 — Use PostgreSQL for user identity and MongoDB for note content</strong></summary>

<br>

**Status:** Accepted

**Context:** NoorNote must persist two fundamentally different data shapes. User identity records are structured, fixed-schema, and demand unique-constraint enforcement and referential integrity. Note documents are schema-flexible — variable fields, nested tags, and a structure likely to evolve — making a rigid relational model a poor fit.

**Decision:** PostgreSQL is the exclusive owner of all user identity and authentication data. MongoDB is the exclusive owner of all note content and activity logs.

**Rationale:** A single PostgreSQL database with JSONB columns for notes would sacrifice query expressiveness and horizontal write scalability on the document side. A single MongoDB database could hold users, but enforcing unique constraints on `email` and `username` would require application-layer logic, introducing race conditions that a database-native unique index eliminates. Splitting ownership by data shape gives each store the workload it is optimised for. The accepted trade-off is that hybrid queries (e.g. `/users/{user_id}/notes`) require two sequential database calls, and referential consistency across stores must be maintained by the application rather than by foreign-key constraints.

</details>

---

<details>
<summary><strong>ADR-002 — Use JWT Bearer tokens for stateless authentication</strong></summary>

<br>

**Status:** Accepted

**Context:** FastAPI runs as two parallel instances behind Nginx for load distribution. Any authentication mechanism that stores session state server-side requires either a shared session store or sticky routing — both of which add infrastructure coupling from Phase 1 onward.

**Decision:** Authentication is stateless. All protected endpoints validate a signed JWT Bearer token that encodes `user_id` and `exp` (expiry). No server-side session state is written or read during request processing.

**Rationale:** Server-side sessions in Redis would provide instant token revocation but couple every authenticated request to Redis availability and add a round-trip to the cache on the hot path. Opaque tokens stored in PostgreSQL are fully revocable but make the database a mandatory dependency on every request, turning it into a latency bottleneck under load. JWTs eliminate both concerns: each instance validates the token signature independently with no shared state. The accepted trade-off is that token revocation before expiry is not possible without an out-of-scope denylist, so the token lifetime must be kept short to bound exposure from a leaked token.

</details>

---

<details>
<summary><strong>ADR-003 — Use Elasticsearch as a derived search index, not the source of truth</strong></summary>

<br>

**Status:** Accepted

**Context:** `GET /search` requires full-text relevance scoring, field boosting, fuzzy matching, and highlighted snippets — capabilities that neither MongoDB's `$text` operator nor PostgreSQL's `tsvector`/`tsquery` provide at the required quality. However, routing all note writes through Elasticsearch would couple write durability to its availability.

**Decision:** MongoDB remains the source of truth for all note content. Elasticsearch holds a derived, read-optimised index that is populated asynchronously via the Kafka pipeline. In the event of any inconsistency, MongoDB is authoritative and the index can be fully rebuilt by replaying MongoDB documents.

**Rationale:** MongoDB Atlas Search would remove Elasticsearch entirely, but it is a managed-cloud feature unavailable in a self-hosted Docker environment. PostgreSQL full-text search lacks native fuzzy matching and per-field boost weights without significant custom query logic. Elasticsearch's `multi_match` query with `fuzziness: AUTO` and `^3` title boosting satisfies all search requirements out of the box. The accepted trade-off is eventual consistency: a note may not appear in search results for a brief window after creation. Under normal conditions this lag is sub-second; under Kafka consumer backpressure it may extend to several seconds. This is acceptable for a note-taking workload where search is a convenience feature, not a hard real-time requirement.

</details>

---

<details>
<summary><strong>ADR-004 — Decouple activity logging via Kafka (fire-and-forget)</strong></summary>

<br>

**Status:** Accepted

**Context:** NoorNote must record an activity log entry for every significant user action: signup, login, note CRUD, and search. If each of these writes a document to MongoDB synchronously within the request handler, the API response time becomes directly coupled to the latency of the logging write, and a slow or unavailable MongoDB logging path blocks the primary operation.

**Decision:** FastAPI publishes a structured JSON event to the `collabnote_events` Kafka topic non-blocking (fire-and-forget) after the primary operation completes. A standalone Kafka Consumer process subscribes to the topic and writes each event as an `activity_log` document to MongoDB asynchronously, fully outside the request path.

**Rationale:** A synchronous MongoDB write is the simplest implementation and guarantees zero log loss, but it directly adds logging latency to every API call and creates a cascading failure risk if MongoDB is under load. FastAPI's `BackgroundTasks` runs after the response is dispatched (removing latency impact) but executes in-process: a worker crash or restart loses any queued tasks. Kafka's durable message log provides at-least-once delivery guarantees — if the consumer crashes, it resumes from its last committed offset and replays unprocessed messages on restart. The accepted trade-off is that activity logs are eventually consistent: a log entry may be absent for a brief window after the corresponding action, and the system moves from a "guaranteed synchronous write" model to an "guaranteed eventual write" model.

</details>

---

<details>
<summary><strong>ADR-005 — Use Redis as a cache-aside layer for note reads</strong></summary>

<br>

**Status:** Accepted

**Context:** `GET /notes/{id}` is the highest-frequency read endpoint in NoorNote. Notes are immutable between writes, meaning the same document is often fetched many times in quick succession with no change between requests. Serving every read from MongoDB introduces avoidable round-trip latency on the hot path.

**Decision:** A cache-aside pattern is implemented on `GET /notes/{id}`: check Redis first under the key `note:{id}`. On a cache miss, fetch from MongoDB and write the result to Redis with `TTL = 3600 s`. On a cache hit, return immediately with the response header `Cache: HIT`. On `PUT /notes/{id}` or `DELETE /notes/{id}`, the corresponding Redis key is deleted immediately after the MongoDB write to prevent stale reads.

**Rationale:** A write-through cache keeps Redis always in sync with MongoDB but requires every write path to flow through the cache layer, coupling write availability to Redis. No caching at all is operationally simpler and acceptable at low traffic, but it does not scale once note reads become the dominant workload. Cache-aside is the standard pattern for this scenario: it is read-optimised, tolerates Redis unavailability on the write path (a failed invalidation degrades gracefully to a stale TTL-bounded read rather than a write failure), and is straightforward to reason about. The accepted trade-off is a stale-read window if Redis is unavailable during a `PUT` invalidation; the 3600 s TTL bounds the maximum staleness to one hour.

</details>

---

<details>
<summary><strong>ADR-006 — Use KRaft mode for Kafka — no ZooKeeper</strong></summary>

<br>

**Status:** Accepted

**Context:** Kafka traditionally requires a ZooKeeper ensemble for cluster coordination and broker metadata management. In a single-broker development environment, running ZooKeeper adds a container with its own RAM and CPU allocation purely for infrastructure bookkeeping, with no user-visible benefit.

**Decision:** Kafka is run in KRaft mode (Kafka Raft Metadata mode) using `confluentinc/cp-kafka:7.6.0`. The broker manages its own metadata via an internal Raft log, removing the ZooKeeper dependency entirely and reducing the running service count by one.

**Rationale:** Kafka with ZooKeeper is the established configuration for versions below 3.3, but it adds operational complexity and a dedicated container. Redis Streams is already in the stack and could serve as a lightweight event bus, but it lacks Kafka's consumer group offset management, replay-from-offset capability, and topic retention semantics. KRaft has been production-ready since Kafka 3.3 and is the default in all new Kafka deployments as of 3.7. The accepted trade-off is that single-broker KRaft provides no fault tolerance: a broker restart causes a brief unavailability window.

</details>

---

<details>
<summary><strong>ADR-007 — Mount GraphQL as an additional interface on the existing FastAPI instance</strong></summary>

<br>

**Status:** Accepted

**Context:** Phase 4 introduces a GraphQL API to expose NoorNote's full data graph. The GraphQL layer needs access to JWT authentication context, PostgreSQL, MongoDB, Elasticsearch, and the Kafka producer — all of which are already initialised within the FastAPI application lifespan.

**Decision:** The `/graphql` endpoint is mounted directly onto the existing FastAPI application (via Strawberry or Ariadne), sharing the application's existing dependency-injection context, database client pool, and Kafka producer.

**Rationale:** Deploying GraphQL as a standalone microservice would provide a clean service boundary and independent scalability, but it would double the custom-build container count and require inter-service HTTP calls. Replacing REST with GraphQL entirely would break all Phase 1–3 REST contracts. Mounting GraphQL onto FastAPI adds zero new infrastructure, reuses existing middleware and database connections, and preserves backward compatibility. The accepted trade-off is that the FastAPI instance becomes a dual-protocol server.

</details>

---

<details>
<summary><strong>ADR-008 — Run the Kafka Consumer as a standalone process instead of a FastAPI background task</strong></summary>

<br>

**Status:** Accepted

**Context:** NoorNote requires reliable asynchronous processing for activity logging and Elasticsearch indexing after Kafka events are published. A design decision was required between running the consumer logic inside FastAPI using `BackgroundTasks` or running it as an independent long-lived process.

**Decision:** The Kafka Consumer runs as a dedicated standalone process (`consumer/consumer.py`) in its own container rather than as an in-process FastAPI background task.

**Rationale:** FastAPI `BackgroundTasks` execute inside the API worker process and are tied to the lifecycle of that worker. If the FastAPI instance crashes, restarts, or scales down while tasks are pending, queued background work is lost permanently. In contrast, a standalone Kafka Consumer maintains its own lifecycle independent of the API servers and benefits fully from Kafka consumer-group semantics, offset tracking, and replay capability. If the consumer crashes, it resumes from the last committed offset and continues processing unhandled events after restart. Separating the consumer also prevents heavy indexing or logging workloads from competing with HTTP request handling threads and memory inside FastAPI workers. The accepted trade-off is additional operational complexity: one extra container/service must be deployed and monitored.

</details>

---


## Conlusion

NoorNote's architecture balances pragmatic engineering trade-offs with clear operational boundaries to deliver a resilient, scalable note-taking platform. By assigning each data shape to the storage technology that best fits its access patterns — PostgreSQL for identity, MongoDB for document content, Elasticsearch for search, Redis for ephemeral caching, and Kafka for durable eventing — the design reduces coupling and keeps the request path fast while preserving recoverability and auditability. The incremental delivery plan (Phases 1–4) enables rapid value delivery while adding complexity only when needed, and the Architecture Decision Log (ADL) records the reasoning behind each major choice so future contributors can understand the constraints and accepted trade-offs.

Operationally, the system prefers eventual consistency where it reduces latency or increases resilience (search indexing, activity logging) and uses stateless authentication and cache-aside patterns to minimise dependencies on any single service during request handling. Running the Kafka Consumer as a separate process and mounting GraphQL on the existing FastAPI instance are concrete choices that balance reliability, developer productivity, and deployment complexity.

Together, these decisions deliver a maintainable foundation that is easy to reason about, simple to operate at small scale, and straightforward to evolve: when any component needs to change, the ADL provides the documented context to make a safe, informed replacement or upgrade.

