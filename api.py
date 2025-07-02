from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import os
from youtube_scraper import YouTubeScraper
from data_storage import DataStorage
from dotenv import load_dotenv
from youtube_trending import get_unique_trending_channels
import logging

app = FastAPI()

load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
TRENDING_API_KEY = os.getenv("TRENDING_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
POSTGRES_URI = os.getenv("POSTGRES_URI")

scraper = YouTubeScraper(YOUTUBE_API_KEY)
storage = DataStorage(mongodb_uri=MONGODB_URI, postgres_uri=POSTGRES_URI)  # Pass Postgres URI

class ScrapeRequest(BaseModel):
    channels: List[str]
    batch_size: int = 50
    days_back: int = 365
    max_videos: Optional[int] = 200

@app.post("/scrape")
def scrape_channels(req: ScrapeRequest):
    all_scraped = []
    for channel in req.channels:
        channel_id = scraper.get_channel_id_from_name(channel) or channel
        videos = scraper.scrape_channel(
            channel_id,
            batch_size=req.batch_size,
            days_back=req.days_back,
            max_videos=req.max_videos
        )
        storage.store_channel_data(channel, videos)
        all_scraped.extend(videos)
    return {"scraped_videos": len(all_scraped), "videos": all_scraped}

@app.post("/scrape_existing")
def scrape_all_channels():
    channels = storage.get_all_channel_names()
    all_scraped = []
    for channel in channels:
        channel_id = scraper.get_channel_id_from_name(channel) or channel
        videos = scraper.scrape_channel(channel_id=channel_id, batch_size=50, days_back=0, max_videos=None)
        storage.store_channel_data(channel, videos)
        all_scraped.extend(videos)
    return {"scraped_videos": len(all_scraped), "videos": all_scraped}

@app.get("/trending_channels")
def trending_channels():
    return get_unique_trending_channels(TRENDING_API_KEY, category="music", country="in", language="en")

@app.post("/scrape_trending")
def scrape_trending_channels(batch_size: int = 50, days_back: int = 365, max_videos: Optional[int] = 200):
    trending_channels = get_unique_trending_channels(TRENDING_API_KEY, category="music", country="in", language="en")
    if not trending_channels:
        return {"message": "No trending channels found", "scraped_videos": 0}

    req = ScrapeRequest(
        channels=trending_channels,
        batch_size=batch_size,
        days_back=days_back,
        max_videos=max_videos
    )
    return scrape_channels(req)