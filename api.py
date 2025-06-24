from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import os
from youtube_scraper import YouTubeScraper
from data_storage import DataStorage

app = FastAPI()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")  # Set this in your environment if using MongoDB

scraper = YouTubeScraper(YOUTUBE_API_KEY)
storage = DataStorage(mongodb_uri=MONGODB_URI)

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
        # Store in MongoDB/JSON/memory using your DataStorage class
        storage.store_channel_data(channel, videos)
        all_scraped.extend(videos)
    return {"scraped_videos": len(all_scraped), "videos": all_scraped}

@app.post("/scrape_existing")
def scrape_all_channels():
    # Get all unique channel names from storage
    channels = storage.get_all_channel_names()  # You need to implement this method in DataStorage
    all_scraped = []
    for channel in channels:
        channel_id = scraper.get_channel_id_from_name(channel) or channel
        videos = scraper.scrape_channel(channel_id=channel_id, batch_size=50, days_back=0, max_videos=None)
        storage.store_channel_data(channel, videos)
        all_scraped.extend(videos)
    return {"scraped_videos": len(all_scraped), "videos": all_scraped}