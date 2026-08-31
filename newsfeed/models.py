"""Domain models for the news feed demo."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class MediaType(str, Enum):
    NONE = "NONE"
    IMAGE = "IMAGE"
    VIDEO = "VIDEO"
    LINK = "LINK"


@dataclass
class User:
    id: str
    username: str
    email: str
    password_hash: str
    display_name: str
    created_at: float
    follower_count: int = 0
    following_count: int = 0
    is_celebrity: bool = False


@dataclass
class Post:
    id: str
    author_id: str
    content: str
    created_at: float
    media_type: MediaType = MediaType.NONE
    media_url: Optional[str] = None
    updated_at: Optional[float] = None
    deleted: bool = False
    like_count: int = 0
    comment_count: int = 0
    share_count: int = 0


@dataclass
class Comment:
    id: str
    post_id: str
    author_id: str
    content: str
    created_at: float


@dataclass
class FeedEntry:
    """ID-only pointer stored in a follower's precomputed timeline."""

    post_id: str
    author_id: str
    created_at: float


@dataclass
class FeedPage:
    items: list[Post] = field(default_factory=list)
    next_cursor: Optional[str] = None
