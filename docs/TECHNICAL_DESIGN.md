# Technical Design Document — News Feed System

Companion to [`HLD.md`](./HLD.md). This document justifies the hybrid architecture and specifies how it meets **scalability, caching/CDN, messaging, security, and reliability**.

## 1. Design justification

The assignment’s hard constraint is **feed p95 ≤ 500 ms** at **millions of users / billions of posts**, with **eventual consistency on feeds** and **strong consistency on critical writes**.

Those constraints rule out two naïve designs:

1. **Join `follows` × `posts` on every GET /feed.** At 200 followees this is hundreds of partition reads plus a merge in the request thread. It will not hold 250k QPS.
2. **Synchronous push to every follower inside POST /posts.** For a 5M-follower account this is millions of Redis writes on the author’s HTTP request. Create latency and worker saturation both fail.

Hybrid fan-out + async Kafka + ID-only timelines is the smallest design that still matches published production practice (Twitter’s timeline cache, Instagram/Facebook precomputed feeds with a celebrity exception).

Chronological order is required in v1. Ranking (affinity, ML) is an extension: the candidate set is still “pushed IDs + pulled celebrity posts”; only the sort key changes.

---

## 2. Scalability and performance

### 2.1 Read-heavy vs write-heavy

| Path | Character | Technique |
| --- | --- | --- |
| GET /feed | Dominant traffic | Precomputed Redis sorted set; hydrate from post cache; no graph walk on the hot path |
| POST /posts | Lower QPS, bursty | Durable write + Kafka; fan-out off the request |
| Likes/comments | Write-heavy, skewed to viral posts | Partition + counter shards; cache the post card |
| Follow | Low QPS, strong | Dual adjacency write; invalidate celebrity flag if threshold crossed |

Horizontal scale: stateless services behind the gateway. Redis Cluster and Cassandra scale by adding nodes; Kafka by adding partitions (hash `author_id` so one author’s posts stay ordered).

### 2.2 Latency budget (p95, home feed, cache warm)

| Step | Budget |
| --- | --- |
| Gateway + auth | 10 ms |
| Redis `ZREVRANGE` + celebrity set | 15 ms |
| Parallel celebrity `LIMIT k` | 40 ms |
| MGET post/user objects | 20 ms |
| Merge + JSON | 10 ms |
| **Total** | **~95 ms** (headroom to 500 ms on miss / GC / cross-AZ) |

Cache miss: rebuild from `precomputed_feed` + celebrity pull. Bound celebrity pull (max 20 authors, k=10) so miss stays inside 500 ms.

### 2.3 Traffic spikes and viral content

- **Auto-scale** feed and engagement pods on QPS and p95.
- **Kafka** absorbs create bursts; fan-out lag is visible (consumer lag) but creates still return 201.
- **CDN** absorbs media; origin is not on the feed path.
- **Hot keys:** viral `post:{id}` — replicate cache, lease-get to stop thundering herds (Memcached lease pattern).
- **Hot partitions:** like counters sharded; rate-limit engagement per post per user.
- **Degrade:** if fan-out lags, serve slightly stale timelines (allowed: eventual consistency). If Redis is down, read Cassandra feed table (slower, still correct).

### 2.4 Reducing latency (checklist)

- Push work to write time for the 99%+ non-celebrity authors.
- Store **IDs** in timelines; keep objects in a second cache.
- Parallel celebrity pulls.
- Connection pooling, hedged reads on Redis.
- Page size 20; never hydrate thousands of posts for one request.
- Media via CDN, not the API.

---

## 3. Caching and CDN

### 3.1 What is cached

| Cache | Key | Value | Why |
| --- | --- | --- | --- |
| Home timeline | `timeline:{user_id}` | Sorted set of `post_id` scored by time | Removes graph + merge from the hot path |
| Post object | `post:{post_id}` | Card JSON (text, media URL, counts) | Hydration without Cassandra |
| User card | `user:{user_id}` | Display name, avatar, celebrity flag | Feed rows |
| Follow lists (hot) | `following:{id}`, `followers:{id}` prefix | ID lists or count | Fan-out and pull |
| Celebrity set | `celebrities` / flag | Boolean | Skip push |
| Feed page (optional) | `feedpage:{user}:{cursor}` | Serialized page | Extra if hydrate is expensive |
| Search | Query cache short TTL | Result IDs | Repeated trending polls |

**Do not cache** passwords, refresh tokens, or full follower lists of mega-celebrities in one key.

### 3.2 Policies: LRU + TTL

- **TTL:** timelines 24 h for active users (refreshed on write); post objects 5–15 min (counts drift is OK); user cards 10 min.
- **LRU / maxmemory:** Redis `allkeys-lru` so inactive users fall out. Durable `precomputed_feed` remains source of rebuild.
- **Cap:** 800 IDs per timeline — users rarely scroll further in one session; older content is a pull/rebuild.

Demo: `LruTtlCache` with max entries + per-key TTL.

### 3.3 Invalidation

| Event | Action |
| --- | --- |
| Post created (normal author) | `ZADD` timelines; delete `feedpage:*` for those followers if used |
| Post created (celebrity) | No timeline writes; next GET /feed pulls fresh posts |
| Post edited | Update `post:{id}`; timelines still hold the ID |
| Post deleted | Tombstone in DB; drop from timelines (`ZREM`) best-effort; hydrate skips `deleted` |
| Like/comment | Update counters on post cache or delete `post:{id}` |
| Follow | If followee is non-celebrity, backfill recent posts into follower timeline; if celebrity, next read picks them up |
| Unfollow | `ZREM` that author’s IDs from timeline (scan by author metadata, or store `author_id` in the member payload) |
| User crosses 10k followers | Set celebrity flag; **stop** future push; existing copies in timelines age out |

Prefer **write-through on push** plus **short TTL** over relying on TTL alone, so followers of normal authors see posts immediately.

### 3.4 CDN

- Images/videos uploaded via **presigned URL** to object storage.
- Transcode/resize asynchronously; store variants (`s`, `m`, `hls`).
- CloudFront / Fastly in front of the bucket. Cache-Control immutable for content-hashed object keys.
- Feed API returns **HTTPS CDN URLs**, never proxy bytes through Feed service.
- Signed cookies or tokenized URLs if media is non-public.

---

## 4. Message queue usage

Kafka (or equivalent) is the **async backbone**, not a database.

| Topic | Key | Consumers |
| --- | --- | --- |
| `post.created` | `author_id` | Fan-out workers, search indexer, notification (later) |
| `post.deleted` | `author_id` | Fan-out ZREM, indexer delete |
| `engagement.updated` | `post_id` | Counter aggregators, trending |
| `media.ready` | `post_id` | Patch post with CDN URL |

**Why a queue**

- POST /posts returns after fsync + produce, not after N Redis writes.
- Fan-out workers scale independently of API pods.
- Replay from offset after a worker crash (Kafka retention 7 days).
- Ordering per author via partition key `author_id`.

**Delivery:** at-least-once. Fan-out `ZADD` is idempotent (same score/member). Likes use a unique `(post_id, user_id)` so duplicate events do not double-count if the consumer is idempotent.

**Backpressure:** if Redis is slow, lag grows; create API stays healthy. Alert on consumer lag > 30 s.

The demo has no Kafka broker: `FeedService.on_post_created` is the in-process consumer.

---

## 5. Security

| Control | Detail |
| --- | --- |
| Authn | Bcrypt/Argon2id hashes; short-lived JWT access + rotating refresh; HTTPS only |
| Authz | Only the author edits/deletes a post; like/comment require a valid user |
| Gateway | Rate limit per user and per IP on create, like, search (abuse and scrape) |
| Upload | Presigned PUT with content-type allowlist, max size, virus scan / async moderation |
| Injection | Parameterized queries; Cassandra bound values; OpenSearch sanitized query string |
| PII | Emails not in feed JSON; logs redacted; encryption at rest on Postgres/S3 |
| Privacy (v1+) | Block lists filtered at merge; future: close-friends / per-post audience |
| CSRF / XSS | Token API (no cookie CSRF for mobile); CSP on web; escape comment HTML |
| Secrets | No credentials in git; IAM for S3/Kafka |

Demo omits real JWT and stores a password hash field only to keep the User model honest.

---

## 6. Fault tolerance and reliability

### 6.1 No single point of failure

- ≥ 3 API replicas per service, multi-AZ.
- Redis Cluster + replica; Cassandra RF=3; Kafka RF=3; Postgres primary + standby.
- Gateway anycast / multi-AZ NLB.
- CDN is independently available from origin.

### 6.2 Service failure behaviour

| Failure | Behaviour |
| --- | --- |
| Fan-out worker down | Kafka retains events; another consumer takes the partition; feeds catch up |
| Redis down | Feed service reads `precomputed_feed` / pull path; higher latency, still serves |
| Cassandra quorum loss | Writes fail fast; reads may use `LOCAL_ONE` as emergency degrade |
| Search cluster down | Feed still works; `/search` and `/trending` return 503 |
| Object storage blip | Old CDN cache still serves; new uploads retry |

Circuit breakers on Redis and Cassandra. Hedged requests for feed hydrate.

### 6.3 Durability, backup, recovery

- **Posts:** Cassandra commitlog + RF=3 before 201. That is the assignment’s “no data loss” bar for content.
- **Timelines:** Redis is a **cache**. Durable copy is `precomputed_feed` (written by the same fan-out worker, or async mirror). Losing Redis is a performance incident, not data loss.
- **Postgres:** WAL shipping, PITR, daily snapshots.
- **S3:** versioning + cross-region replication for media.
- **Kafka:** min in-sync replicas = 2 for `post.created`.
- **Restore drill:** restore Postgres to staging; rebuild Redis from Cassandra feed table.

Soft-deleted posts remain recoverable until a TTL compaction job.

### 6.4 Consistency recap

| Operation | Consistency |
| --- | --- |
| Register, login, follow, create post ACK | Strong (quorum / SQL commit) |
| Follower’s home timeline | Eventual (seconds of lag OK) |
| Like count on a card | Eventual (cache TTL) |
| “Did *I* like this?” | Strong enough: like row is source of truth |

---

## 7. Extensibility

- **New media type:** add enum + media pipeline; feed cards already carry `media_type` / `media_url`.
- **New engagement:** new Cassandra table + counter field; same Kafka topic family.
- **Ranking:** insert a ranker between candidate merge and pagination; do not change fan-out.
- **Recommendations:** mix a scored candidate list into the merge (optional FR).

---

## 8. What the demo proves vs what production adds

| Demo | Production |
| --- | --- |
| In-memory maps | Postgres + Cassandra + Redis Cluster |
| In-process fan-out | Kafka consumer group |
| `LruTtlCache` | Redis TTL + LRU |
| Threshold = 3 | Threshold = 10,000 |
| Chronological merge | Same merge + optional ranker |
| Hash password field unused for HTTP | JWT at gateway |

The service interfaces stay the same.
