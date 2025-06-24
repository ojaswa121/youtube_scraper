from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import os
from youtube_scraper import YouTubeScraper

app = FastAPI()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
scraper = YouTubeScraper(YOUTUBE_API_KEY)

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
        all_scraped.extend(videos)
    return {"scraped_videos": len(all_scraped), "videos": all_scraped}