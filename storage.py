# storage.py
from pathlib import Path
from typing import List
from models import Post, Comment
import chromadb
from chromadb.config import Settings
import hashlib

class DataStorage:
    """Speichert Posts und Kommentare in ChromaDB."""

    def __init__(self, db_path="../data/chroma_db"):
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)

        # ChromaDB Setup
        self.client = chromadb.PersistentClient(
            path=str(self.db_path)
        )

        # Collections
        self.post_collection = self.client.get_or_create_collection(name="moltbook_posts")
        self.comment_collection = self.client.get_or_create_collection(name="moltbook_comments")

        # Bereits existierende IDs laden, um Duplikate zu vermeiden
        self.existing_post_ids = set(self.post_collection.get()["ids"])
        self.existing_comment_ids = set(self.comment_collection.get()["ids"])

    # -----------------------------
    # POSTS
    # -----------------------------
    def _post_to_doc(self, post: Post) -> dict:
        doc_id = post.post_id
        content = f"{post.title}\n{post.content}"

        metadata = {
            "author": post.author,
            "submolt": post.submolt,
            "timestamp": str(post.timestamp),
            "likes": post.likes,
            "comments_count": post.comments_count,
            "total_comments_count": post.total_comments_count,
            "hashtags": post.hashtags if post.hashtags else ["none"],
            "mentions": post.mentions if post.mentions else ["none"],
            "url": post.url,
            "post_type": post.post_type,
        }
        return {"id": doc_id, "content": content, "metadata": metadata}

    def save_posts(self, posts: List[Post]):
        """Speichert neue Posts in ChromaDB, ohne Duplikate."""
        new_docs = []
        for post in posts:
            if post.post_id in self.existing_post_ids:
                continue
            new_docs.append(self._post_to_doc(post))
            self.existing_post_ids.add(post.post_id)

        if new_docs:
            self.post_collection.add(
                ids=[doc["id"] for doc in new_docs],
                documents=[doc["content"] for doc in new_docs],
                metadatas=[doc["metadata"] for doc in new_docs]
            )
            #self.client.persist()

        # Kommentare nach Posts speichern
        all_comments = []
        for post in posts:
            all_comments.extend(post.comments)
            self.save_comments(posts)

    # -----------------------------
    # COMMENTS
    # -----------------------------
    def _comment_to_doc(self, comment: Comment, post_id: str) -> dict:
        doc_id = comment.comment_id
        content = comment.content
        metadata = {
            "author": comment.author,
            "timestamp": str(comment.timestamp),
            "timestamp_precision": comment.timestamp_precision,
            "timestamp_raw": comment.timestamp_raw,
            "likes": comment.likes,
            "post_id": post_id
        }
        return {"id": doc_id, "content": content, "metadata": metadata}

    def save_comments(self, posts: List[Post]):
        new_docs = []

        for post in posts:
            for comment in post.comments:
                if comment.comment_id in self.existing_comment_ids:
                    continue
                new_docs.append(self._comment_to_doc(comment, post.post_id))
                self.existing_comment_ids.add(comment.comment_id)

        if new_docs:
            self.comment_collection.add(
                ids=[doc["id"] for doc in new_docs],
                documents=[doc["content"] for doc in new_docs],
                metadatas=[doc["metadata"] for doc in new_docs]
            )
    # -----------------------------
    # STATISTIK
    # -----------------------------
    def get_statistics(self) -> dict:
        all_posts = self.post_collection.get()
        total_posts = len(all_posts["ids"])
        total_comments = len(self.comment_collection.get()["ids"])

        avg_likes = (
            sum(int(md.get("likes", 0)) for md in all_posts["metadatas"]) / total_posts
            if total_posts > 0 else 0
        )
        avg_comments = (
            total_comments / total_posts if total_posts > 0 else 0
        )

        post_types = {}
        for md in all_posts["metadatas"]:
            ptype = md.get("post_type", "text")
            post_types[ptype] = post_types.get(ptype, 0) + 1

        return {
            "total_posts": total_posts,
            "total_comments": total_comments,
            "avg_likes": avg_likes,
            "avg_comments": avg_comments,
            "post_types": post_types,
        }
