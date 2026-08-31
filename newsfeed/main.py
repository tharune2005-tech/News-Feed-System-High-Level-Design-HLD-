"""Seed a tiny graph and start the HTTP server."""

from __future__ import annotations

import sys

from newsfeed.api import make_server
from newsfeed.services import FollowService, NewsFeedApp


def seed(app: NewsFeedApp) -> None:
    alice = app.user_svc.register("alice", "alice@example.com", "pass1234", "Alice")
    bob = app.user_svc.register("bob", "bob@example.com", "pass1234", "Bob")
    carol = app.user_svc.register("carol", "carol@example.com", "pass1234", "Carol")
    dave = app.user_svc.register("dave", "dave@example.com", "pass1234", "Dave")
    celeb = app.user_svc.register("celeb", "celeb@example.com", "pass1234", "Celeb")
    for fan in (alice, bob, carol, dave):
        app.follow(fan.id, celeb.id)
    app.follow(alice.id, bob.id)
    app.post_svc.create(bob.id, "Hello from Bob — this is pushed into Alice's timeline.")
    app.post_svc.create(celeb.id, "Hello from a celebrity — pulled at read time, never fanned out.")
    print("Seeded users:")
    for user in (alice, bob, carol, dave, celeb):
        print(f"  {user.username:6} id={user.id} celebrity={user.is_celebrity} followers={user.follower_count}")
    print(f"Celebrity threshold (demo) = {FollowService.CELEBRITY_THRESHOLD}")


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    app = NewsFeedApp()
    seed(app)
    httpd = make_server(app, port)
    print(f"News feed API on http://localhost:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()
