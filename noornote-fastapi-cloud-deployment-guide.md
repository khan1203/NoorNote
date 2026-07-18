# Deploying NoorNote to FastAPI Cloud

## Overview

FastAPI Cloud is a managed platform built by the FastAPI team. Deployment is `fastapi deploy` — it uploads your source, installs dependencies from `pyproject.toml`/`requirements.txt`, and runs your ASGI app directly. **No Docker, no Nginx, no manual load balancing** — FastAPI Cloud handles routing, TLS, custom domains, and autoscaling itself.

**Important scope note:** FastAPI Cloud only runs your FastAPI application code. It does not host Postgres, MongoDB, Redis, Kafka, or Elasticsearch. Every stateful service NoorNote depends on becomes an **external managed service** you connect to over a URL/connection string — same as connecting to RDS from ECS, just with third-party providers instead of AWS-native ones.

| Component | Where it runs |
|---|---|
| FastAPI app | FastAPI Cloud (replaces Nginx + 2× instances with 1 autoscaled deployment) |
| PostgreSQL | **Neon** (native integration) |
| Redis | **Redis Cloud** (native integration) |
| MongoDB | **MongoDB Atlas** (external, connect via env var) |
| Elasticsearch | **Elastic Cloud** (external, connect via env var) |
| Kafka | **Confluent Cloud** (external, connect via env var) |
| Kafka Consumer | Runs separately — FastAPI Cloud expects an ASGI `app` entrypoint, not a standalone long-running script |

---

## Prerequisites

- `uv` installed
- A FastAPI Cloud account (may require joining a waitlist)
- The NoorNote repo cloned locally

---

## 1. Prepare the repo for FastAPI Cloud

FastAPI Cloud needs `fastapi[standard]` in your dependencies (this bundles the CLI):

```bash
cd NoorNote
uv add "fastapi[standard]"
```

Pin your Python version:

```toml
# pyproject.toml
[project]
requires-python = ">=3.12"
```

Confirm the entrypoint. If your app instance isn't in `main.py`, `app.py`, or `app/main.py`, declare it explicitly:

```toml
[tool.fastapi]
entrypoint = "app.main:app"
```

Verify locally before deploying:

```bash
uv run fastapi dev
```

If this runs without needing a file path, you're ready.

---

## 2. Set up external managed services

### PostgreSQL → Neon

FastAPI Cloud has a native Neon integration — connect it from the FastAPI Cloud dashboard under your app's **Integrations** tab, or create a Neon project directly:

```bash
# via Neon's own CLI/dashboard, or through FastAPI Cloud's integration flow
```

This auto-populates a `DATABASE_URL` environment variable in your deployed app — no manual connection string wiring needed.

### Redis → Redis Cloud

Same pattern — native integration, connect via the dashboard. This sets a `REDIS_URL` env var automatically.

### MongoDB → MongoDB Atlas

No native integration, so this is manual:

1. Create a free/shared cluster at [mongodb.com/cloud/atlas](https://www.mongodb.com/cloud/atlas)
2. Whitelist FastAPI Cloud's outbound IPs (or `0.0.0.0/0` for simplicity, tightened later)
3. Copy the connection string

```bash
fastapi cloud env set --secret MONGO_URL "mongodb+srv://user:pass@cluster.mongodb.net/noornote"
```

### Elasticsearch → Elastic Cloud

1. Create a deployment at [cloud.elastic.co](https://cloud.elastic.co)
2. Copy the Cloud ID and API key

```bash
fastapi cloud env set --secret ELASTIC_CLOUD_ID "noornote:xxxx"
fastapi cloud env set --secret ELASTIC_API_KEY "xxxx"
```

### Kafka → Confluent Cloud

1. Create a basic cluster at [confluent.cloud](https://confluent.cloud)
2. Create the `noornote_events` topic
3. Generate an API key/secret for the cluster

```bash
fastapi cloud env set --secret KAFKA_BOOTSTRAP_SERVERS "pkc-xxxx.region.provider.confluent.cloud:9092"
fastapi cloud env set --secret KAFKA_API_KEY "xxxx"
fastapi cloud env set --secret KAFKA_API_SECRET "xxxx"
```

Update your `aiokafka` producer config to use `SASL_SSL` security protocol, which Confluent Cloud requires (local KRaft mode didn't need this).

---

## 3. Set remaining environment variables

```bash
fastapi cloud env set --secret JWT_SECRET "your-strong-random-secret"
fastapi cloud env set ENVIRONMENT "production"
```

Manage all of these visually instead, if you prefer, in the FastAPI Cloud Dashboard under your app's **Environment Variables** section.

---

## 4. Handle the Kafka Consumer separately

Since FastAPI Cloud deploys an ASGI app, not an arbitrary script, `consumer/consumer.py` needs a home elsewhere. Options, roughly in order of effort:

- **Simplest:** run it as a small always-on process on a cheap VM (EC2 t3.micro, Fly.io, Railway) — same idea as your existing standalone-process ADR (008), just relocated.
- **Refactor:** move the consumer loop into the FastAPI app's `lifespan` as a background `asyncio` task, so it runs inside the same FastAPI Cloud deployment. This slightly weakens the isolation your ADR 008 was designed around (a FastAPI restart now also restarts the consumer), but keeps everything on one platform.

Given your ADR explicitly chose standalone-process isolation for crash resilience, keeping it on a separate small host is the more faithful migration.

---

## 5. Deploy

```bash
fastapi deploy
```

```
Deploying to FastAPI Cloud...
🚀 Preparing for liftoff! Almost there...
✅ Deployment successful!
🐔 Your app is ready at https://noornote.fastapicloud.dev
```

---

## 6. Verify

```bash
curl https://noornote.fastapicloud.dev/docs
```

Check logs from the dashboard (**Apps → noornote → Logs**), or:

```bash
fastapi cloud logs
```

---

## 7. Custom domain (optional)

Configure `api.yourdomain.com` under **Advanced Features → Custom Domains** in the dashboard, then point a CNAME at the address FastAPI Cloud gives you.

---

## 8. CI/CD

```bash
fastapi cloud setup-ci
```

This generates a GitHub Actions workflow that runs `fastapi deploy` on push to `main`, using a deploy token instead of your personal login.

---

## Validation checklist

- [ ] `uv run fastapi dev` runs locally without a file path argument
- [ ] Neon and Redis Cloud integrations show connected in the dashboard
- [ ] `MONGO_URL`, `ELASTIC_CLOUD_ID`, `ELASTIC_API_KEY`, `KAFKA_*`, `JWT_SECRET` are all set as secrets (not visible after creation)
- [ ] `fastapi deploy` completes successfully
- [ ] `/docs` loads over HTTPS at the generated `.fastapicloud.dev` URL
- [ ] Kafka Consumer is running somewhere (separate host or in-process background task) and is actually consuming `noornote_events`
- [ ] `GET /notes/{id}` shows `Cache: HIT` on a second request, confirming Redis Cloud connectivity
- [ ] `GET /search?q=...` returns results, confirming Elastic Cloud connectivity

---

## Key takeaways

- FastAPI Cloud collapses your Nginx + 2×FastAPI + Docker Compose layer into a single `fastapi deploy` — genuinely less to operate for the app tier.
- Every stateful service moves to an external managed provider — this is the same direction as your AWS roadmap (RDS/DocumentDB/ElastiCache/MSK), just realized through third-party SaaS instead of AWS-native services.
- The Kafka Consumer is the one piece that doesn't map cleanly, since FastAPI Cloud expects an ASGI entrypoint, not a standalone script — decide whether to keep it isolated on its own small host (matching your original ADR 008 reasoning) or fold it into the FastAPI app's lifespan.
- This path trades infrastructure control (which the EC2 guide maximized) for operational simplicity — a reasonable choice if the goal is shipping the API quickly rather than deepening AWS infrastructure skills, which your `AWS_Fundamentals` track is otherwise building toward.
