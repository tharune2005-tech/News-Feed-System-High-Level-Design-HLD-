"""Unit tests for hybrid fan-out, pagination, ownership, and engagement."""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from newsfeed.cache import LruTtlCache
from newsfeed.services import (
    Conflict,
    FollowService,
    Forbidden,
    NewsFeedApp,
    ValidationError,
)


class NewsFeedTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = NewsFeedApp()
        self.alice = self.app.user_svc.register("alice", "a@x.com", "pw", "Alice")
        self.bob = self.app.user_svc.register("bob", "b@x.com", "pw", "Bob")
        self.carol = self.app.user_svc.register("carol", "c@x.com", "pw", "Carol")
        self.dave = self.app.user_svc.register("dave", "d@x.com", "pw", "Dave")
        self.erin = self.app.user_svc.register("erin", "e@x.com", "pw", "Erin")

    def test_duplicate_username_rejected(self) -> None:
        with self.assertRaises(Conflict):
            self.app.user_svc.register("alice", "other@x.com", "pw", "A")

    def test_cannot_follow_self(self) -> None:
        with self.assertRaises(ValidationError):
            self.app.follow(self.alice.id, self.alice.id)

    def test_follow_is_idempotent(self) -> None:
        self.app.follow(self.alice.id, self.bob.id)
        self.app.follow(self.alice.id, self.bob.id)
        self.assertEqual(self.app.user_svc.get(self.bob.id).follower_count, 1)

    def test_normal_post_is_pushed_into_follower_feed(self) -> None:
        self.app.follow(self.alice.id, self.bob.id)
        post = self.app.post_svc.create(self.bob.id, "pushed")
        ids = [p.id for p in self.app.feed_svc.get_feed(self.alice.id).items]
        self.assertIn(post.id, ids)
        self.assertEqual(len(self.app.feeds.get(self.alice.id)), 1)

    def test_celebrity_post_is_not_pushed(self) -> None:
        celeb = self.app.user_svc.register("celeb", "z@x.com", "pw", "Celeb")
        for fan in (self.alice, self.bob, self.carol):
            self.app.follow(fan.id, celeb.id)
        self.assertTrue(self.app.follow_svc.is_celebrity(celeb.id))
        post = self.app.post_svc.create(celeb.id, "pulled only")
        self.assertEqual(self.app.feeds.get(self.alice.id), [])
        ids = [p.id for p in self.app.feed_svc.get_feed(self.alice.id).items]
        self.assertIn(post.id, ids)

    def test_feed_merges_push_and_pull_newest_first(self) -> None:
        celeb = self.app.user_svc.register("star", "s@x.com", "pw", "Star")
        for fan in (self.alice, self.bob, self.carol):
            self.app.follow(fan.id, celeb.id)
        self.app.follow(self.alice.id, self.dave.id)
        older = self.app.post_svc.create(self.dave.id, "from dave")
        time.sleep(0.02)
        newer = self.app.post_svc.create(celeb.id, "from star")
        items = self.app.feed_svc.get_feed(self.alice.id).items
        self.assertEqual([p.id for p in items][:2], [newer.id, older.id])

    def test_unfollow_removes_pushed_posts(self) -> None:
        self.app.follow(self.alice.id, self.bob.id)
        post = self.app.post_svc.create(self.bob.id, "gone soon")
        self.app.unfollow(self.alice.id, self.bob.id)
        ids = [p.id for p in self.app.feed_svc.get_feed(self.alice.id).items]
        self.assertNotIn(post.id, ids)

    def test_follow_backfills_recent_non_celebrity_posts(self) -> None:
        post = self.app.post_svc.create(self.bob.id, "already posted")
        self.app.follow(self.alice.id, self.bob.id)
        ids = [p.id for p in self.app.feed_svc.get_feed(self.alice.id).items]
        self.assertIn(post.id, ids)

    def test_only_author_can_edit_or_delete(self) -> None:
        post = self.app.post_svc.create(self.bob.id, "mine")
        with self.assertRaises(Forbidden):
            self.app.post_svc.edit(post.id, self.alice.id, "hack")
        with self.assertRaises(Forbidden):
            self.app.post_svc.delete(post.id, self.alice.id)
        self.app.post_svc.delete(post.id, self.bob.id)
        with self.assertRaises(Exception):
            self.app.post_svc.get(post.id)

    def test_like_is_idempotent(self) -> None:
        post = self.app.post_svc.create(self.bob.id, "like me")
        self.app.engage_svc.like(post.id, self.alice.id)
        self.app.engage_svc.like(post.id, self.alice.id)
        self.assertEqual(self.app.post_svc.get(post.id).like_count, 1)
        self.app.engage_svc.unlike(post.id, self.alice.id)
        self.assertEqual(self.app.post_svc.get(post.id).like_count, 0)

    def test_comment_and_share_update_counts(self) -> None:
        post = self.app.post_svc.create(self.bob.id, "talk")
        self.app.engage_svc.comment(post.id, self.alice.id, "nice")
        self.app.engage_svc.share(post.id, self.alice.id)
        fresh = self.app.post_svc.get(post.id)
        self.assertEqual(fresh.comment_count, 1)
        self.assertEqual(fresh.share_count, 1)

    def test_cursor_pagination(self) -> None:
        self.app.follow(self.alice.id, self.bob.id)
        ids = []
        for i in range(5):
            time.sleep(0.01)
            ids.append(self.app.post_svc.create(self.bob.id, f"p{i}").id)
        page1 = self.app.feed_svc.get_feed(self.alice.id, limit=2)
        self.assertEqual(len(page1.items), 2)
        self.assertIsNotNone(page1.next_cursor)
        page2 = self.app.feed_svc.get_feed(self.alice.id, cursor=page1.next_cursor, limit=2)
        seen = [p.id for p in page1.items + page2.items]
        self.assertEqual(len(set(seen)), 4)
        self.assertTrue(set(seen).issubset(set(ids)))

    def test_search_and_trending(self) -> None:
        post = self.app.post_svc.create(self.bob.id, "distributed systems feed design")
        self.app.engage_svc.like(post.id, self.alice.id)
        found = self.app.search_svc.search("distributed")
        self.assertTrue(any(p.id == post.id for p in found["posts"]))
        people = self.app.search_svc.search("bob")
        self.assertTrue(any(u.id == self.bob.id for u in people["users"]))
        trend = self.app.search_svc.trending()
        self.assertEqual(trend[0].id, post.id)

    def test_cache_ttl_and_lru(self) -> None:
        cache = LruTtlCache(max_size=2, default_ttl_seconds=0.05)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        self.assertIsNone(cache.get("a"))
        self.assertEqual(cache.get("c"), 3)
        time.sleep(0.06)
        self.assertIsNone(cache.get("c"))

    def test_celebrity_threshold_matches_assignment_demo(self) -> None:
        self.assertEqual(FollowService.CELEBRITY_THRESHOLD, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
