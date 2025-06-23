import re
import os
from typing import Optional
import streamlit as st

def extract_channel_id(channel_input: str) -> Optional[str]:
    """Extract channel ID from various YouTube channel formats"""
    if not channel_input:
        return None
    
    channel_input = channel_input.strip()
    
    # Direct channel ID (starts with UC and is 24 characters)
    if channel_input.startswith('UC') and len(channel_input) == 24:
        return channel_input
    
    # Channel URL patterns
    patterns = [
        r'youtube\.com/channel/([UC][\w-]{22})',  # youtube.com/channel/UCxxxxx
        r'youtube\.com/c/[\w-]+.*?/([UC][\w-]{22})',  # youtube.com/c/name/UCxxxxx
        r'youtube\.com/user/[\w-]+.*?/([UC][\w-]{22})',  # youtube.com/user/name/UCxxxxx
    ]
    
    for pattern in patterns:
        match = re.search(pattern, channel_input)
        if match:
            return match.group(1)
    
    # If no pattern matches, return None (will need to search by name)
    return None

def validate_api_key(api_key: str) -> bool:
    """Validate YouTube API key format"""
    if not api_key:
        return False
    
    # Basic validation - YouTube API keys are typically 39 characters
    if len(api_key) < 30:
        return False
    
    # Should contain only alphanumeric characters, hyphens, and underscores
    if not re.match(r'^[A-Za-z0-9_-]+$', api_key):
        return False
    
    return True

def format_number(num: float) -> str:
    """Format large numbers with appropriate suffixes"""
    if num is None:
        return "0"
    
    try:
        num = float(num)
    except (ValueError, TypeError):
        return "0"
    
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return str(int(num))

def format_duration(duration_str: str) -> str:
    """Convert YouTube duration format (PT4M13S) to readable format"""
    if not duration_str or duration_str == 'Unknown':
        return 'Unknown'
    
    # Parse ISO 8601 duration format
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return duration_str
    
    hours, minutes, seconds = match.groups()
    
    # Convert to integers, default to 0 if None
    hours = int(hours) if hours else 0
    minutes = int(minutes) if minutes else 0
    seconds = int(seconds) if seconds else 0
    
    # Format based on duration
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"

def validate_channel_name(channel_name: str) -> bool:
    """Validate channel name format"""
    if not channel_name:
        return False
    
    # Remove whitespace
    channel_name = channel_name.strip()
    
    # Should not be empty after stripping
    if not channel_name:
        return False
    
    # Should not contain only special characters
    if re.match(r'^[^a-zA-Z0-9]+$', channel_name):
        return False
    
    return True

def get_youtube_thumbnail(video_id: str, quality: str = 'medium') -> str:
    """Get YouTube thumbnail URL for a video"""
    quality_map = {
        'default': 'default',
        'medium': 'mqdefault',
        'high': 'hqdefault',
        'standard': 'sddefault',
        'maxres': 'maxresdefault'
    }
    
    quality_key = quality_map.get(quality, 'mqdefault')
    return f"https://img.youtube.com/vi/{video_id}/{quality_key}.jpg"

def safe_int_convert(value) -> int:
    """Safely convert value to integer"""
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return 0

def calculate_engagement_rate(likes: int, views: int) -> float:
    """Calculate engagement rate (likes/views * 100)"""
    if views == 0:
        return 0.0
    
    return (likes / views) * 100

def group_videos_by_month(videos: list) -> dict:
    """Group videos by publication month"""
    from datetime import datetime
    import pandas as pd
    
    monthly_groups = {}
    
    for video in videos:
        try:
            # Parse publication date
            pub_date = pd.to_datetime(video['published_at'])
            month_key = pub_date.strftime('%Y-%m')
            
            if month_key not in monthly_groups:
                monthly_groups[month_key] = []
            
            monthly_groups[month_key].append(video)
            
        except Exception:
            # Skip videos with invalid dates
            continue
    
    return monthly_groups

def export_to_csv(data: list, filename: str) -> bool:
    """Export data to CSV file"""
    try:
        import pandas as pd
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        return True
        
    except Exception as e:
        st.error(f"Error exporting to CSV: {str(e)}")
        return False

def clean_text(text: str) -> str:
    """Clean text by removing extra whitespace and newlines"""
    if not text:
        return ""
    
    # Remove extra whitespace and newlines
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text

def is_valid_url(url: str) -> bool:
    """Check if string is a valid URL"""
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None

def get_file_size_mb(filepath: str) -> float:
    """Get file size in MB"""
    try:
        size_bytes = os.path.getsize(filepath)
        return round(size_bytes / (1024 * 1024), 2)
    except OSError:
        return 0.0
