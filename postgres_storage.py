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
    CREATE INDEX IF NOT EXISTS idx_video_id ON videos(video_id);
    CREATE INDEX IF NOT EXISTS idx_channel_id ON videos(channel_id);
    CREATE INDEX IF NOT EXISTS idx_published_at ON videos(published_at);
    CREATE INDEX IF NOT EXISTS idx_scraped_at ON videos(scraped_at);
    """
    try:
        with self.conn.cursor() as cur:
            cur.execute(create_table_sql)
            self.conn.commit()
    except Exception as e:
        print(f"⚠️ Error creating PostgreSQL tables/indexes: {str(e)}")

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
    ON CONFLICT (video_id) DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        view_count = EXCLUDED.view_count,
        like_count = EXCLUDED.like_count,
        comment_count = EXCLUDED.comment_count,
        duration = EXCLUDED.duration,
        channel_subscriber_count = EXCLUDED.channel_subscriber_count,
        scraped_at = EXCLUDED.scraped_at,
        batch_info = EXCLUDED.batch_info;
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
            "batch_info": Json(batch_info or {}),
        })

    try:
        with self.conn.cursor() as cur:
            execute_batch(cur, insert_sql, data)
            self.conn.commit()
        return True
    except Exception as e:
        print(f"❌ Error storing to PostgreSQL: {str(e)}")
        self.log_failed_data(data)
        return False

def log_failed_data(self, failed_data):
    try:
        with open(self.error_log_path, "a", encoding="utf-8") as f:
            for row in failed_data:
                row["batch_info"] = row["batch_info"].adapted  # convert back to raw JSON string
                f.write(json.dumps(row, default=str) + "\n")
        print(f"📝 Logged {len(failed_data)} failed rows to {self.error_log_path}")
    except Exception as log_err:
        print(f"⚠️ Failed to log errors: {str(log_err)}")
