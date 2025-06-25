import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

class PostgresStorage:
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or os.getenv("POSTGRES_URI")
        self.conn = None
        self.connected = False
        if self.connection_string:
            self.connected = self.connect()
            if self.connected:
                self.create_tables()

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.connection_string)
            print("✅ Connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {str(e)}")
            self.conn = None
            return False

    def create_tables(self):
        if not self.conn:
            return
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(32) UNIQUE,
            channel_name TEXT,
            title TEXT,
            description TEXT,
            published_at TIMESTAMP,
            channel_id TEXT,
            thumbnail_url TEXT,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            duration TEXT,
            channel_subscriber_count BIGINT,
            scraped_at TIMESTAMP,
            batch_info JSONB
        );
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_table_sql)
                self.conn.commit()
        except Exception as e:
            print(f"⚠️ Error creating PostgreSQL tables: {str(e)}")

    def store_videos_batch(self, channel_name, videos, batch_info=None):
        if not self.conn:
            print("⚠️ PostgreSQL not connected. Skipping storage.")
            return False
        insert_sql = """
        INSERT INTO videos (
            video_id, channel_name, title, description, published_at, channel_id, thumbnail_url,
            view_count, like_count, comment_count, duration, channel_subscriber_count, scraped_at, batch_info
        ) VALUES (
            %(video_id)s, %(channel_name)s, %(title)s, %(description)s, %(published_at)s, %(channel_id)s, %(thumbnail_url)s,
            %(view_count)s, %(like_count)s, %(comment_count)s, %(duration)s, %(channel_subscriber_count)s, %(scraped_at)s, %(batch_info)s
        )
        ON CONFLICT (video_id) DO NOTHING;
        """
        data = []
        now = datetime.utcnow()
        for video in videos:
            data.append({
                "video_id": video.get("video_id"),
                "channel_name": channel_name,
                "title": video.get("title"),
                "description": video.get("description"),
                "published_at": video.get("published_at"),
                "channel_id": video.get("channel_id"),
                "thumbnail_url": video.get("thumbnail_url"),
                "view_count": video.get("view_count"),
                "like_count": video.get("like_count"),
                "comment_count": video.get("comment_count"),
                "duration": video.get("duration"),
                "channel_subscriber_count": video.get("channel_subscriber_count"),
                "scraped_at": now,
                "batch_info": batch_info or {},
            })
        try:
            with self.conn.cursor() as cur:
                execute_batch(cur, insert_sql, data)
                self.conn.commit()
            return True
        except Exception as e:
            print(f"❌ Error storing to PostgreSQL: {str(e)}")
            return False