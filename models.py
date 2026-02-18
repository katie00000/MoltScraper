# models.py

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

@dataclass
class Comment:
    """Kommentar-Modell"""
    comment_id: str
    author: str
    content: str
    timestamp: Optional[datetime] = None
    timestamp_precision: str = 'unknown'
    timestamp_raw: str = ''
    likes: int = 0

@dataclass
class Post:
    """Post-Modell mit separatem Titel"""
    post_id: str
    author: str
    title: str = ''  # ← NEU: Separates Titel-Feld
    content: str = ''
    author_id: str = ''
    timestamp: Optional[datetime] = None
    timestamp_precision: str = 'unknown'
    timestamp_raw: str = ''
    likes: int = 0
    comments_count: int = 0
    total_comments_count: int = 0
    comments: List[Comment] = field(default_factory=list)
    post_type: str = 'text'
    media_urls: List[str] = field(default_factory=list)
    hashtags: List[str] = field(default_factory=list)
    mentions: List[str] = field(default_factory=list)
    url: str = ''
    scraped_at: Optional[datetime] = None