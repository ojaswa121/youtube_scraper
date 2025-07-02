#!/bin/bash

# Set the API base URL
API_URL="http://localhost:8000"

# Log file location
LOG_FILE="/workspaces/youtube_scraper/scheduled_scrape.log"

echo "[$(date)] Starting scheduled scrape..." >> "$LOG_FILE"

# Scrape trending channels
echo "[$(date)] Scraping trending channels..." >> "$LOG_FILE"
curl -X POST "$API_URL/scrape_trending" \
     -H "Content-Type: application/json" \
     -d '{"batch_size": 50, "days_back": 365, "max_videos": 200}' \
     >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"

# Scrape existing channels
echo "[$(date)] Scraping existing channels..." >> "$LOG_FILE"
curl -X POST "$API_URL/scrape_existing" \
     -H "Content-Type: application/json" \
     >> "$LOG_FILE" 2>&1

echo "" >> "$LOG_FILE"
echo "[$(date)] Scheduled scrape finished." >> "$LOG_FILE"
echo "----------------------------------------" >> "$LOG_FILE"