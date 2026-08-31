"""Minimal stdlib HTTP API so the demo is curl-able without Flask/FastAPI."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from newsfeed.models import Comment, Post, User
from newsfeed.services import NewsFeedApp, NewsFeedError, ValidationError


def user_json(u: User) -> dict:
    return {
        "id": u.id,
        "username": u.username,
        "displayName": u.display_name,
        "followerCount": u.follower_count,
        "followingCount": u.following_count,
        "celebrity": u.is_celebrity,
    }


def post_json(p: Post) -> dict:
    return {
        "id": p.id,
        "authorId": p.author_id,
        "content": p.content,
        "mediaType": p.media_type.value,
        "mediaUrl": p.media_url,
        "createdAt": p.created_at,
        "likeCount": p.like_count,
        "commentCount": p.comment_count,
        "shareCount": p.share_count,
    }


def comment_json(c: Comment) -> dict:
    return {
        "id": c.id,
        "postId": c.post_id,
        "authorId": c.author_id,
        "content": c.content,
        "createdAt": c.created_at,
    }


def match(path: str, template: str) -> Optional[dict[str, str]]:
    ps, ts = path.strip("/").split("/"), template.strip("/").split("/")
    if len(ps) != len(ts):
        return None
    params: dict[str, str] = {}
    for a, b in zip(ps, ts):
        if b.startswith("{") and b.endswith("}"):
            params[b[1:-1]] = a
        elif a != b:
            return None
    return params


class Handler(BaseHTTPRequestHandler):
    app: NewsFeedApp

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length).decode()
        return json.loads(raw) if raw else {}

    def _send(self, status: int, body: Any) -> None:
        payload = json.dumps(body).encode() if body is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._dispatch("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        try:
            status, body = self._route(method, path, qs)
            self._send(status, body)
        except NewsFeedError as exc:
            self._send(exc.status, {"error": str(exc)})
        except KeyError as exc:
            self._send(400, {"error": f"missing field {exc}"})
        except Exception as exc:  # pragma: no cover
            self._send(500, {"error": str(exc)})

    def _route(self, method: str, path: str, qs: dict) -> tuple[int, Any]:
        app = self.app
        body = self._read_json() if method in {"POST", "PUT", "DELETE"} else {}

        if method == "POST" and path == "/users":
            user = app.user_svc.register(
                body["username"], body["email"], body["password"], body.get("displayName", "")
            )
            return 201, user_json(user)

        if method == "GET" and (m := match(path, "/users/{id}")):
            return 200, user_json(app.user_svc.get(m["id"]))

        if method == "GET" and (m := match(path, "/users/{id}/history")):
            user = app.user_svc.get(m["id"])
            posts = app.posts.by_author(user.id)
            return 200, {"user": user_json(user), "posts": [post_json(p) for p in posts]}

        if method == "POST" and (m := match(path, "/users/{id}/follow")):
            return 200, user_json(app.follow(m["id"], body["targetId"]))

        if method == "DELETE" and (m := match(path, "/users/{id}/follow/{targetId}")):
            return 200, user_json(app.unfollow(m["id"], m["targetId"]))

        if method == "GET" and (m := match(path, "/users/{id}/followers")):
            return 200, [user_json(app.user_svc.get(i)) for i in app.follows.followers(m["id"])]

        if method == "GET" and (m := match(path, "/users/{id}/following")):
            return 200, [user_json(app.user_svc.get(i)) for i in app.follows.following(m["id"])]

        if method == "POST" and path == "/posts":
            post = app.post_svc.create(
                body["authorId"],
                body.get("content", ""),
                body.get("mediaType", "NONE"),
                body.get("mediaUrl"),
            )
            return 201, post_json(post)

        if method == "GET" and (m := match(path, "/posts/{id}")):
            return 200, post_json(app.post_svc.get(m["id"]))

        if method == "PUT" and (m := match(path, "/posts/{id}")):
            return 200, post_json(app.post_svc.edit(m["id"], body["authorId"], body["content"]))

        if method == "DELETE" and (m := match(path, "/posts/{id}")):
            author = body.get("authorId") or qs.get("authorId")
            if not author:
                raise ValidationError("authorId required")
            app.post_svc.delete(m["id"], author)
            return 200, {"deleted": True}

        if method == "GET" and (m := match(path, "/users/{id}/posts")):
            return 200, [post_json(p) for p in app.posts.by_author(m["id"])]

        if method == "GET" and (m := match(path, "/users/{id}/feed")):
            page = app.feed_svc.get_feed(m["id"], cursor=qs.get("cursor"), limit=int(qs.get("limit", 20)))
            return 200, {"items": [post_json(p) for p in page.items], "nextCursor": page.next_cursor}

        if method == "POST" and (m := match(path, "/posts/{id}/like")):
            return 200, post_json(app.engage_svc.like(m["id"], body["userId"]))

        if method == "DELETE" and (m := match(path, "/posts/{id}/like/{userId}")):
            return 200, post_json(app.engage_svc.unlike(m["id"], m["userId"]))

        if method == "POST" and (m := match(path, "/posts/{id}/comments")):
            return 201, comment_json(app.engage_svc.comment(m["id"], body["userId"], body["content"]))

        if method == "GET" and (m := match(path, "/posts/{id}/comments")):
            return 200, [comment_json(c) for c in app.engage_svc.comments(m["id"])]

        if method == "POST" and (m := match(path, "/posts/{id}/share")):
            return 200, post_json(app.engage_svc.share(m["id"], body["userId"]))

        if method == "GET" and path == "/search":
            result = app.search_svc.search(qs.get("q", ""))
            return 200, {
                "users": [user_json(u) for u in result["users"]],
                "posts": [post_json(p) for p in result["posts"]],
            }

        if method == "GET" and path == "/trending":
            return 200, [post_json(p) for p in app.search_svc.trending(int(qs.get("limit", 10)))]

        return 404, {"error": "not found"}


def make_server(app: NewsFeedApp, port: int) -> ThreadingHTTPServer:
    Handler.app = app
    return ThreadingHTTPServer(("0.0.0.0", port), Handler)
