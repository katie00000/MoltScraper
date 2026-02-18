# storage.py

import json
import sqlite3
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging

from models import Post, Comment
from config import Config

logger = logging.getLogger(__name__)

class DataStorage:
    """Daten-Speicherung für Moltbook Posts"""
    
    def __init__(self):
        """Storage initialisieren und Verzeichnisse erstellen"""
        # Verzeichnisse erstellen
        self.data_dir = Config.setup_directories()
        
        # Dateipfade
        self.db_path = Config.DB_PATH
        self.json_path = Config.JSON_PATH
        self.csv_dir = Config.CSV_DIR
        
        logger.info(f"💾 Storage initialisiert: {self.data_dir}")
    
    def save_to_json(self, posts: List[Post]) -> Path:
        """Posts als JSON speichern"""
        try:
            posts_data = []
            for post in posts:
                post_dict = {
                    'post_id': post.post_id,
                    'author': post.author,
                    'author_id': post.author_id,
                    'title': post.title,  # ← NEU!
                    'content': post.content,
                    'timestamp': post.timestamp.isoformat() if post.timestamp else None,
                    'timestamp_precision': post.timestamp_precision,
                    'timestamp_raw': post.timestamp_raw,
                    'likes': post.likes,
                    'comments_count': post.comments_count,
                    'total_comments_count': post.total_comments_count,
                    'post_type': post.post_type,
                    'media_urls': post.media_urls,
                    'hashtags': post.hashtags,
                    'mentions': post.mentions,
                    'url': post.url,
                    'scraped_at': post.scraped_at.isoformat() if post.scraped_at else None,
                    'comments': [
                        {
                            'comment_id': c.comment_id,
                            'author': c.author,
                            'content': c.content,
                            'timestamp': c.timestamp.isoformat() if c.timestamp else None,
                            'timestamp_precision': c.timestamp_precision,
                            'timestamp_raw': c.timestamp_raw,
                            'likes': c.likes
                        }
                        for c in post.comments
                    ]
                }
                posts_data.append(post_dict)
            
            with open(self.json_path, 'w', encoding='utf-8') as f:
                json.dump(posts_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ {len(posts)} Posts als JSON gespeichert")
            return self.json_path
            
        except Exception as e:
            logger.error(f"❌ Fehler beim Speichern als JSON: {e}")
            raise

    def save_to_sqlite(self, posts: List[Post]) -> Path:
        """Posts in SQLite speichern"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tabelle mit Titel-Feld
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS posts (
                    post_id TEXT PRIMARY KEY,
                    author TEXT,
                    author_id TEXT,
                    title TEXT,
                    content TEXT,
                    timestamp TEXT,
                    timestamp_precision TEXT,
                    timestamp_raw TEXT,
                    likes INTEGER,
                    comments_count INTEGER,
                    total_comments_count INTEGER,
                    post_type TEXT,
                    media_urls TEXT,
                    hashtags TEXT,
                    mentions TEXT,
                    url TEXT,
                    scraped_at TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS comments (
                    comment_id TEXT PRIMARY KEY,
                    post_id TEXT,
                    author TEXT,
                    content TEXT,
                    timestamp TEXT,
                    timestamp_precision TEXT,
                    timestamp_raw TEXT,
                    likes INTEGER,
                    FOREIGN KEY (post_id) REFERENCES posts (post_id)
                )
            ''')
            
            # Posts einfügen
            for post in posts:
                cursor.execute('''
                    INSERT OR REPLACE INTO posts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    post.post_id,
                    post.author,
                    post.author_id,
                    post.title,  # ← NEU!
                    post.content,
                    post.timestamp.isoformat() if post.timestamp else None,
                    post.timestamp_precision,
                    post.timestamp_raw,
                    post.likes,
                    post.comments_count,
                    post.total_comments_count,
                    post.post_type,
                    json.dumps(post.media_urls),
                    json.dumps(post.hashtags),
                    json.dumps(post.mentions),
                    post.url,
                    post.scraped_at.isoformat() if post.scraped_at else None
                ))
                
                # Kommentare einfügen
                for comment in post.comments:
                    cursor.execute('''
                        INSERT OR REPLACE INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        comment.comment_id,
                        post.post_id,
                        comment.author,
                        comment.content,
                        comment.timestamp.isoformat() if comment.timestamp else None,
                        comment.timestamp_precision,
                        comment.timestamp_raw,
                        comment.likes
                    ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ {len(posts)} Posts in SQLite gespeichert")
            return self.db_path
            
        except Exception as e:
            logger.error(f"❌ Fehler beim Speichern in SQLite: {e}")
            raise
    
    def export_to_csv(self) -> Path:
        """
        Daten aus SQLite als CSV exportieren
        
        Returns:
            Pfad zum CSV-Verzeichnis
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Posts exportieren
            posts_df = self._execute_query(conn, "SELECT * FROM posts")
            posts_csv = self.csv_dir / f"posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            with open(posts_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in posts_df['description']])
                writer.writerows(posts_df['data'])
            
            # Kommentare exportieren
            comments_df = self._execute_query(conn, "SELECT * FROM comments")
            comments_csv = self.csv_dir / f"comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            with open(comments_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([desc[0] for desc in comments_df['description']])
                writer.writerows(comments_df['data'])
            
            conn.close()
            
            logger.info(f"✅ CSV exportiert nach: {self.csv_dir}")
            return self.csv_dir
            
        except Exception as e:
            logger.error(f"❌ Fehler beim CSV-Export: {e}")
            raise
    
    def _execute_query(self, conn: sqlite3.Connection, query: str) -> Dict[str, Any]:
        """SQL-Query ausführen und Ergebnis zurückgeben"""
        cursor = conn.cursor()
        cursor.execute(query)
        return {
            'data': cursor.fetchall(),
            'description': cursor.description
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Statistiken aus der Datenbank holen"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Basis-Statistiken
            cursor.execute("SELECT COUNT(*) FROM posts")
            total_posts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM comments")
            total_comments = cursor.fetchone()[0]
            
            cursor.execute("SELECT AVG(likes) FROM posts")
            avg_likes = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT AVG(comments_count) FROM posts")
            avg_comments = cursor.fetchone()[0] or 0
            
            cursor.execute("SELECT SUM(total_comments_count) FROM posts")
            all_comments = cursor.fetchone()[0] or 0

            # Post-Typen
            cursor.execute("SELECT post_type, COUNT(*) FROM posts GROUP BY post_type")
            post_types = dict(cursor.fetchall())
            
            # Top Autoren
            cursor.execute("SELECT author, COUNT(*) as count FROM posts GROUP BY author ORDER BY count DESC LIMIT 10")
            top_authors = cursor.fetchall()
            
            conn.close()
            
            return {
                'total_posts': total_posts,
                'total_comments': total_comments,
                'avg_likes': avg_likes,
                'avg_comments': avg_comments,
                'all_comments': all_comments,
                'post_types': post_types,
                'top_authors': top_authors
            }
            
        except Exception as e:
            logger.error(f"❌ Fehler beim Abrufen der Statistiken: {e}")
            return {
                'total_posts': 0,
                'total_comments': 0,
                'avg_likes': 0,
                'avg_comments': 0,
                'post_types': {},
                'top_authors': []
            }