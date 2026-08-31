# News Feed System — High-Level Design (HLD)

UnsaidTalks advanced assignment: design a Facebook / Instagram / Twitter-style news feed that scales to millions of users and billions of posts.

**Public repository:** [https://github.com/tharune2005-tech/News-Feed-System-High-Level-Design-HLD-](https://github.com/tharune2005-tech/News-Feed-System-High-Level-Design-HLD-)

## Overview

The production design is **hybrid fan-out**:

- **Normal authors** — fan-out on write. A new post ID is pushed into each follower’s precomputed timeline (Redis sorted set).
- **Celebrities** (≥ 10,000 followers in production; **3** in the demo) — fan-out on read. Their posts are **not** pushed. The feed service pulls recent posts at read time and merges them.

That split is how the design hits **p95 ≤ 500 ms** on GET /feed without writing millions of timeline rows for a celebrity post. Feeds are **eventually consistent**; register / login / follow / “post created” are **strongly consistent**.

The Python package is a bonus in-memory implementation of the same rules (layered services, ID-only timelines, LRU+TTL cache, cursor pagination) with **no third-party dependencies**.

## Documentation (required)

| Document | What it covers |
| --- | --- |
| [`docs/HLD.md`](docs/HLD.md) | Architecture, components, feed strategy (push vs pull vs hybrid), database schema, APIs, data flows, trade-offs |
| [`docs/TECHNICAL_DESIGN.md`](docs/TECHNICAL_DESIGN.md) | Justification, scalability/performance, caching + CDN, Kafka, security, fault tolerance |

Assignment source: UnsaidTalks *News Feed System: High-Level Design (HLD) Assignment*.

## Project structure

```
.
├── README.md
├── run.sh
├── docs/
│   ├── HLD.md
│   └── TECHNICAL_DESIGN.md
├── newsfeed/
│   ├── models.py          User, Post, Comment, FeedEntry
│   ├── cache.py           LRU + TTL (Redis stand-in)
│   ├── repositories.py    In-memory stores (swap for SQL/NoSQL)
│   ├── services.py        Hybrid fan-out, engagement, search
│   ├── api.py             stdlib HTTP JSON API
│   └── main.py            Seed data + server
└── tests/
    └── test_newsfeed.py
```

## Run

Python 3.11+ (stdlib only).

```bash
chmod +x run.sh
./run.sh test          # unit tests
./run.sh               # API on :8080
./run.sh 9090          # custom port
```

Or:

```bash
PYTHONPATH=. python3 tests/test_newsfeed.py
PYTHONPATH=. python3 -m newsfeed.main 8080
```

Startup seeds `alice`, `bob`, `carol`, `dave`, and `celeb` (`celeb` already has four followers, so they are on the **pull** path). Printed IDs are what you pass to curl.

```bash
# Trending
curl http://localhost:8080/trending

# Alice's home feed (Bob pushed + Celeb pulled)
curl "http://localhost:8080/users/{aliceId}/feed?limit=10"

# Register, follow, post, like
curl -X POST http://localhost:8080/users -H 'Content-Type: application/json' \
  -d '{"username":"erin","email":"erin@example.com","password":"pass1234","displayName":"Erin"}'

curl -X POST http://localhost:8080/users/{erinId}/follow -H 'Content-Type: application/json' \
  -d '{"targetId":"{bobId}"}'

curl -X POST http://localhost:8080/posts -H 'Content-Type: application/json' \
  -d '{"authorId":"{erinId}","content":"Hello feed","mediaType":"NONE"}'

curl -X POST http://localhost:8080/posts/{postId}/like -H 'Content-Type: application/json' \
  -d '{"userId":"{aliceId}"}'
```

Full route table: [`docs/HLD.md`](docs/HLD.md) §5.

## Design in one paragraph

POST /posts writes the post durably and returns immediately. A fan-out worker (Kafka in production, in-process here) pushes the post ID to followers **unless** the author is a celebrity. GET /feed reads the precomputed ID list, pulls recent posts from celebrity followees, merges by time, hydrates objects, and paginates with an opaque cursor. Redis/CDN take the read heat; Cassandra holds posts and the graph; Postgres holds users.

## Submission checklist

- [x] HLD document — architecture, components, schema, APIs, data flow, trade-offs
- [x] Technical design document — scale, cache/CDN, queues, security, reliability
- [x] Bonus implementation + unit tests (`./run.sh test`)
- [x] README — overview, structure, run instructions, this checklist
- [x] Public GitHub repository
