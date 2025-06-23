from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd
import os

API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'

youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

def get_recent_videos(channel_id, years=3, max_results=200):
    three_years_ago = (datetime.utcnow() - timedelta(days=365*years)).isoformat("T") + "Z"
    video_data = []

    next_page_token = None
    while True:
        res = youtube.search().list(
            channelId=channel_id,
            part='id,snippet',
            maxResults=50,
            publishedAfter=three_years_ago,
            order='date',
            type='video',
            pageToken=next_page_token
        ).execute()

        video_ids = [item['id']['videoId'] for item in res['items']]
        if not video_ids:
            break

        stats_res = youtube.videos().list(
            id=','.join(video_ids),
            part='statistics,snippet',
        ).execute()

        for item in stats_res['items']:
            video_data.append({
                'video_id': item['id'],
                'title': item['snippet']['title'],
                'published_at': item['snippet']['publishedAt'],
                'view_count': item['statistics'].get('viewCount'),
                'like_count': item['statistics'].get('likeCount'),
                'comment_count': item['statistics'].get('commentCount'),
            })

        next_page_token = res.get('nextPageToken')
        if not next_page_token or len(video_data) >= max_results:
            break

    return pd.DataFrame(video_data)
