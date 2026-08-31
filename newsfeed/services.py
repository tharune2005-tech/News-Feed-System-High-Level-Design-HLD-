"""Business rules. Fan-out, celebrity detection, feed merge, engagement."""

from __future__ import annotations

import base64
import hashlib
import time
import uuid
from typing import Optional

from newsfeed.cache import LruTtlCache
from newsfeed.models import Comment, FeedEntry, FeedPage, MediaType, Post, User
from newsfeed.repositories import (
    EngagementRepository,
    FeedRepository,
    FollowRepository,
    PostRepository,
    UserRepository,
)


class NewsFeedError(Exception):
    status = 400


class NotFound(NewsFeedError):
    status = 404


class Conflict(NewsFeedError):
    status = 409


class Forbidden(NewsFeedError):
    status = 403


class ValidationError(NewsFeedError):
    status = 400


def _id() -> str:
    return uuid.uuid4().hex


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def encode_cursor(created_at: float, post_id: str) -> str:
    raw = f"{created_at:.6f}:{post_id}".encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[float, str]:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    ts, post_id = raw.split(":", 1)
    return float(ts), post_id


class UserService:
    def __init__(self, users: UserRepository):
        self.users = users

    def register(self, username: str, email: str, password: str, display_name: str) -> User:
        if not username or not email or not password:
            raise ValidationError("username, email, and password are required")
        if self.users.exists_username(username):
            raise Conflict("username already taken")
        if self.users.exists_email(email):
            raise Conflict("email already registered")
        user = User(
            id=_id(),
            username=username,
            email=email,
            password_hash=_hash_password(password),
            display_name=display_name or username,
            created_at=time.time(),
        )
        self.users.save(user)
        return user

    def get(self, user_id: str) -> User:
        user = self.users.get(user_id)
        if not user:
            raise NotFound("user not found")
        return user


class FollowService:
    """Production celebrity threshold is 10_000; demo uses 3 so tests hit both paths."""

    CELEBRITY_THRESHOLD = 3

    def __init__(self, users: UserRepository, follows: FollowRepository):
        self.users = users
        self.follows = follows

    def _require(self, user_id: str) -> User:
        user = self.users.get(user_id)
        if not user:
            raise NotFound("user not found")
        return user

    def follow(self, follower_id: str, followee_id: str) -> User:
        if follower_id == followee_id:
            raise ValidationError("cannot follow yourself")
        follower = self._require(follower_id)
        followee = self._require(followee_id)
        added = self.follows.follow(follower_id, followee_id)
        if added:
            follower.following_count = self.follows.following_count(follower_id)
            followee.follower_count = self.follows.follower_count(followee_id)
            followee.is_celebrity = followee.follower_count >= self.CELEBRITY_THRESHOLD
            self.users.save(follower)
            self.users.save(followee)
        return followee

    def unfollow(self, follower_id: str, followee_id: str) -> User:
        follower = self._require(follower_id)
        followee = self._require(followee_id)
        removed = self.follows.unfollow(follower_id, followee_id)
        if removed:
            follower.following_count = self.follows.following_count(follower_id)
            followee.follower_count = self.follows.follower_count(followee_id)
            followee.is_celebrity = followee.follower_count >= self.CELEBRITY_THRESHOLD
            self.users.save(follower)
            self.users.save(followee)
        return followee

    def is_celebrity(self, user_id: str) -> bool:
        user = self._require(user_id)
        return user.is_celebrity or user.follower_count >= self.CELEBRITY_THRESHOLD

    def celebrity_followees(self, follower_id: str) -> list[str]:
        return [uid for uid in self.follows.following(follower_id) if self.is_celebrity(uid)]


class FeedService:
    def __init__(
        self,
        posts: PostRepository,
        follows: FollowRepository,
        feeds: FeedRepository,
        follow_svc: FollowService,
        cache: LruTtlCache,
    ):
        self.posts = posts
        self.follows = follows
        self.feeds = feeds
        self.follow_svc = follow_svc
        self.cache = cache

    def on_post_created(self, post: Post) -> None:
        """In-process stand-in for a Kafka fan-out consumer."""
        if self.follow_svc.is_celebrity(post.author_id):
            return
        entry = FeedEntry(post_id=post.id, author_id=post.author_id, created_at=post.created_at)
        for follower_id in self.follows.followers(post.author_id):
            self.feeds.push(follower_id, entry)
            self.cache.delete(f"feed:{follower_id}")

    def on_follow(self, follower_id: str, followee_id: str) -> None:
        self.cache.delete(f"feed:{follower_id}")
        if self.follow_svc.is_celebrity(followee_id):
            return
        for post in reversed(self.posts.by_author(followee_id, limit=20)):
            self.feeds.push(
                follower_id,
                FeedEntry(post_id=post.id, author_id=post.author_id, created_at=post.created_at),
            )

    def on_unfollow(self, follower_id: str, followee_id: str) -> None:
        self.feeds.remove_author(follower_id, followee_id)
        self.cache.delete(f"feed:{follower_id}")

    def _candidates(self, user_id: str) -> list[FeedEntry]:
        merged: dict[str, FeedEntry] = {}
        for entry in self.feeds.get(user_id):
            merged[entry.post_id] = entry
        for celeb_id in self.follow_svc.celebrity_followees(user_id):
            for post in self.posts.by_author(celeb_id, limit=20):
                merged[post.id] = FeedEntry(post.id, post.author_id, post.created_at)
        return sorted(merged.values(), key=lambda e: (e.created_at, e.post_id), reverse=True)

    def get_feed(self, user_id: str, cursor: Optional[str] = None, limit: int = 20) -> FeedPage:
        if not self.follow_svc.users.get(user_id):
            raise NotFound("user not found")
        limit = max(1, min(limit, 50))
        entries = self._candidates(user_id)
        if cursor:
            ts, pid = decode_cursor(cursor)
            entries = [e for e in entries if (e.created_at, e.post_id) < (ts, pid)]
        page_entries = entries[:limit]
        items: list[Post] = []
        for entry in page_entries:
            post = self.posts.get(entry.post_id)
            if post and not post.deleted:
                items.append(post)
        next_cursor = None
        if len(entries) > limit and page_entries:
            last = page_entries[-1]
            next_cursor = encode_cursor(last.created_at, last.post_id)
        return FeedPage(items=items, next_cursor=next_cursor)


class PostService:
    def __init__(self, users: UserRepository, posts: PostRepository, feed_svc: FeedService):
        self.users = users
        self.posts = posts
        self.feed_svc = feed_svc

    def create(
        self,
        author_id: str,
        content: str,
        media_type: str = "NONE",
        media_url: Optional[str] = None,
    ) -> Post:
        if not self.users.get(author_id):
            raise NotFound("user not found")
        if not (content or media_url):
            raise ValidationError("content or media_url required")
        try:
            mt = MediaType[media_type]
        except KeyError as exc:
            raise ValidationError("invalid media_type") from exc
        post = Post(
            id=_id(),
            author_id=author_id,
            content=content or "",
            created_at=time.time(),
            media_type=mt,
            media_url=media_url,
        )
        self.posts.save(post)
        self.feed_svc.on_post_created(post)
        return post

    def get(self, post_id: str) -> Post:
        post = self.posts.get(post_id)
        if not post or post.deleted:
            raise NotFound("post not found")
        return post

    def edit(self, post_id: str, author_id: str, content: str) -> Post:
        post = self.get(post_id)
        if post.author_id != author_id:
            raise Forbidden("only the author can edit")
        post.content = content
        post.updated_at = time.time()
        self.posts.save(post)
        return post

    def delete(self, post_id: str, author_id: str) -> None:
        post = self.get(post_id)
        if post.author_id != author_id:
            raise Forbidden("only the author can delete")
        post.deleted = True
        self.posts.save(post)
        self.feed_svc.feeds.remove_post(post_id)


class EngagementService:
    def __init__(self, posts: PostRepository, users: UserRepository, engagement: EngagementRepository):
        self.posts = posts
        self.users = users
        self.engagement = engagement

    def _post(self, post_id: str) -> Post:
        post = self.posts.get(post_id)
        if not post or post.deleted:
            raise NotFound("post not found")
        return post

    def like(self, post_id: str, user_id: str) -> Post:
        if not self.users.get(user_id):
            raise NotFound("user not found")
        post = self._post(post_id)
        if self.engagement.like(post_id, user_id):
            post.like_count += 1
            self.posts.save(post)
        return post

    def unlike(self, post_id: str, user_id: str) -> Post:
        post = self._post(post_id)
        if self.engagement.unlike(post_id, user_id):
            post.like_count = max(0, post.like_count - 1)
            self.posts.save(post)
        return post

    def comment(self, post_id: str, user_id: str, content: str) -> Comment:
        if not content.strip():
            raise ValidationError("comment cannot be empty")
        if not self.users.get(user_id):
            raise NotFound("user not found")
        post = self._post(post_id)
        item = Comment(id=_id(), post_id=post_id, author_id=user_id, content=content, created_at=time.time())
        self.engagement.add_comment(item)
        post.comment_count += 1
        self.posts.save(post)
        return item

    def comments(self, post_id: str) -> list[Comment]:
        self._post(post_id)
        return self.engagement.comments(post_id)

    def share(self, post_id: str, user_id: str) -> Post:
        if not self.users.get(user_id):
            raise NotFound("user not found")
        post = self._post(post_id)
        post.share_count = self.engagement.share(post_id)
        self.posts.save(post)
        return post


class SearchService:
    def __init__(self, users: UserRepository, posts: PostRepository):
        self.users = users
        self.posts = posts

    def search(self, query: str) -> dict:
        q = query.lower().strip()
        if not q:
            return {"users": [], "posts": []}
        users = [
            u
            for u in self.users.all()
            if q in u.username.lower() or q in u.display_name.lower()
        ]
        posts = [p for p in self.posts.all_live() if q in p.content.lower()]
        return {"users": users[:20], "posts": posts[:20]}

    def trending(self, limit: int = 10) -> list[Post]:
        now = time.time()
        scored = []
        for post in self.posts.all_live():
            age_hours = max((now - post.created_at) / 3600.0, 0.05)
            score = (post.like_count * 1.0 + post.comment_count * 2.0 + post.share_count * 3.0) / age_hours
            scored.append((score, post))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [p for _, p in scored[:limit]]


class NewsFeedApp:
    """Composition root — mirrors production service wiring."""

    def __init__(self) -> None:
        self.users = UserRepository()
        self.posts = PostRepository()
        self.follows = FollowRepository()
        self.feeds = FeedRepository()
        self.engagement = EngagementRepository()
        self.cache = LruTtlCache(max_size=2048, default_ttl_seconds=30)
        self.user_svc = UserService(self.users)
        self.follow_svc = FollowService(self.users, self.follows)
        self.feed_svc = FeedService(self.posts, self.follows, self.feeds, self.follow_svc, self.cache)
        self.post_svc = PostService(self.users, self.posts, self.feed_svc)
        self.engage_svc = EngagementService(self.posts, self.users, self.engagement)
        self.search_svc = SearchService(self.users, self.posts)

    def follow(self, follower_id: str, followee_id: str) -> User:
        followee = self.follow_svc.follow(follower_id, followee_id)
        self.feed_svc.on_follow(follower_id, followee_id)
        return followee

    def unfollow(self, follower_id: str, followee_id: str) -> User:
        followee = self.follow_svc.unfollow(follower_id, followee_id)
        self.feed_svc.on_unfollow(follower_id, followee_id)
        return followee
