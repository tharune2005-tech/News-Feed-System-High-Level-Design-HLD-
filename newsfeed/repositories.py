"""In-memory stores. Production swaps these for Postgres / Cassandra / Redis."""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from newsfeed.models import Comment, FeedEntry, Post, User


class UserRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, User] = {}
        self._by_username: dict[str, str] = {}
        self._by_email: dict[str, str] = {}

    def save(self, user: User) -> None:
        self._by_id[user.id] = user
        self._by_username[user.username.lower()] = user.id
        self._by_email[user.email.lower()] = user.id

    def get(self, user_id: str) -> Optional[User]:
        return self._by_id.get(user_id)

    def get_by_username(self, username: str) -> Optional[User]:
        uid = self._by_username.get(username.lower())
        return self._by_id.get(uid) if uid else None

    def exists_username(self, username: str) -> bool:
        return username.lower() in self._by_username

    def exists_email(self, email: str) -> bool:
        return email.lower() in self._by_email

    def all(self) -> list[User]:
        return list(self._by_id.values())


class PostRepository:
    def __init__(self) -> None:
        self._by_id: dict[str, Post] = {}
        self._by_author: dict[str, list[str]] = defaultdict(list)

    def save(self, post: Post) -> None:
        is_new = post.id not in self._by_id
        self._by_id[post.id] = post
        if is_new:
            self._by_author[post.author_id].insert(0, post.id)

    def get(self, post_id: str) -> Optional[Post]:
        return self._by_id.get(post_id)

    def by_author(self, author_id: str, limit: int = 50) -> list[Post]:
        ids = self._by_author.get(author_id, [])
        posts = [self._by_id[i] for i in ids if i in self._by_id and not self._by_id[i].deleted]
        return posts[:limit]

    def all_live(self) -> list[Post]:
        return [p for p in self._by_id.values() if not p.deleted]


class FollowRepository:
    """Two adjacency lists: followers-of and following-of."""

    def __init__(self) -> None:
        self._followers: dict[str, set[str]] = defaultdict(set)
        self._following: dict[str, set[str]] = defaultdict(set)

    def follow(self, follower_id: str, followee_id: str) -> bool:
        if followee_id in self._following[follower_id]:
            return False
        self._following[follower_id].add(followee_id)
        self._followers[followee_id].add(follower_id)
        return True

    def unfollow(self, follower_id: str, followee_id: str) -> bool:
        if followee_id not in self._following[follower_id]:
            return False
        self._following[follower_id].discard(followee_id)
        self._followers[followee_id].discard(follower_id)
        return True

    def followers(self, user_id: str) -> set[str]:
        return set(self._followers[user_id])

    def following(self, user_id: str) -> set[str]:
        return set(self._following[user_id])

    def follower_count(self, user_id: str) -> int:
        return len(self._followers[user_id])

    def following_count(self, user_id: str) -> int:
        return len(self._following[user_id])


class FeedRepository:
    """Per-user precomputed timeline (newest first), capped."""

    def __init__(self, cap: int = 800) -> None:
        self.cap = cap
        self._feeds: dict[str, list[FeedEntry]] = defaultdict(list)

    def push(self, user_id: str, entry: FeedEntry) -> None:
        feed = self._feeds[user_id]
        feed.insert(0, entry)
        del feed[self.cap :]

    def remove_author(self, user_id: str, author_id: str) -> None:
        self._feeds[user_id] = [e for e in self._feeds[user_id] if e.author_id != author_id]

    def remove_post(self, post_id: str) -> None:
        for uid, feed in self._feeds.items():
            self._feeds[uid] = [e for e in feed if e.post_id != post_id]

    def get(self, user_id: str) -> list[FeedEntry]:
        return list(self._feeds[user_id])


class EngagementRepository:
    def __init__(self) -> None:
        self._likes: dict[str, set[str]] = defaultdict(set)
        self._comments: dict[str, list[Comment]] = defaultdict(list)
        self._shares: dict[str, int] = defaultdict(int)

    def like(self, post_id: str, user_id: str) -> bool:
        if user_id in self._likes[post_id]:
            return False
        self._likes[post_id].add(user_id)
        return True

    def unlike(self, post_id: str, user_id: str) -> bool:
        if user_id not in self._likes[post_id]:
            return False
        self._likes[post_id].discard(user_id)
        return True

    def add_comment(self, comment: Comment) -> None:
        self._comments[comment.post_id].append(comment)

    def comments(self, post_id: str) -> list[Comment]:
        return list(self._comments[post_id])

    def share(self, post_id: str) -> int:
        self._shares[post_id] += 1
        return self._shares[post_id]
