import streamlit as st
import google.generativeai as genai
import requests
import json
import os
from datetime import datetime
import pymongo
from pymongo import MongoClient
import pandas as pd
import re
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="YouTube Data Fetcher with Pagination",
    page_icon="🎥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Backend Configuration - Store your API keys here
class APIConfig:
    """Backend configuration for API keys - modify these values"""
    
    # Automatically fetch keys from .env file using environment variables
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', 'your_gemini_api_key_here')
    YOUTUBE_API_KEY = os.environ.get('YOUTUBE_API_KEY', 'your_youtube_api_key_here')
    MONGODB_CONNECTION = os.environ.get('MONGODB_CONNECTION', 'mongodb://localhost:27017/local/yt_videodata')
    
    @classmethod
    def validate_keys(cls):
        """Validate if API keys are configured"""
        missing_keys = []
        
        if not cls.GEMINI_API_KEY or cls.GEMINI_API_KEY == 'your_gemini_api_key_here':
            missing_keys.append('Gemini API Key')
        
        if not cls.YOUTUBE_API_KEY or cls.YOUTUBE_API_KEY == 'your_youtube_api_key_here':
            missing_keys.append('YouTube API Key')
            
        return missing_keys

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        padding: 2rem 0;
        text-align: center;
        background: linear-gradient(90deg, #ff6b6b, #4ecdc4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: bold;
    }
    .stButton > button {
        width: 100%;
    }
    .error-box {
        background-color: #ffebee;
        border-left: 5px solid #f44336;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0.25rem;
    }
    .success-box {
        background-color: #e8f5e8;
        border-left: 5px solid #4caf50;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0.25rem;
    }
    .warning-box {
        background-color: #fff3e0;
        border-left: 5px solid #ff9800;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0.25rem;
    }
    .pagination-info {
        background-color: #e3f2fd;
        border-left: 5px solid #2196f3;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'videos_data' not in st.session_state:
    st.session_state.videos_data = []
if 'smart_query' not in st.session_state:
    st.session_state.smart_query = ""
if 'pagination_info' not in st.session_state:
    st.session_state.pagination_info = {}
if 'fetch_mode' not in st.session_state:
    st.session_state.fetch_mode = "search"  # "search" or "channel"
if 'channel_info' not in st.session_state:
    st.session_state.channel_info = {}
if 'fetching_in_progress' not in st.session_state:
    st.session_state.fetching_in_progress = False

def check_api_configuration():
    """Check if API keys are properly configured"""
    missing_keys = APIConfig.validate_keys()
    
    if missing_keys:
        st.error("🚨 **Configuration Required!**")
        st.markdown("""
        **Missing API Keys:** {}
        
        **To fix this:**
        1. Create a `.env` file in your project directory
        2. Add your API keys:
        ```
        GEMINI_API_KEY=your_actual_gemini_key
        YOUTUBE_API_KEY=your_actual_youtube_key
        MONGODB_CONNECTION=mongodb://localhost:27017/
        ```
        3. Or modify the `APIConfig` class in the code directly
        """.format(", ".join(missing_keys)))
        return False
    
    return True

def configure_gemini():
    """Configure Gemini AI with API key"""
    try:
        genai.configure(api_key=APIConfig.GEMINI_API_KEY)
        return True
    except Exception as e:
        st.error(f"❌ Error configuring Gemini: {str(e)}")
        return False

def generate_smart_query(prompt):
    """Convert user prompt to optimized YouTube search query using Gemini"""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        system_prompt = """
        You are an expert at creating optimized YouTube search queries. 
        Convert the user's message into a concise, effective YouTube search query that will find relevant videos.
        
        Guidelines:
        - Use keywords that are commonly used in YouTube video titles
        - Keep it under 50 characters when possible
        - Remove unnecessary words like "I want to", "show me", etc.
        - Focus on the main topic/subject
        - Use quotes for exact phrases when needed
        - Make it specific but not too narrow
        
        Return ONLY the search query, nothing else.
        """
        
        full_prompt = f"{system_prompt}\n\nUser message: {prompt}\n\nOptimized YouTube search query:"
        
        response = model.generate_content(full_prompt)
        return response.text.strip().replace('"', '')
    
    except Exception as e:
        st.error(f"❌ Error generating smart query: {str(e)}")
        return prompt

def get_channel_info(channel_identifier):
    """Get channel information by channel ID or username"""
    try:
        # Try to get channel by ID first
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            'part': 'snippet,statistics',
            'key': APIConfig.YOUTUBE_API_KEY
        }
        
        # Check if it's a channel ID or username
        if channel_identifier.startswith('UC') and len(channel_identifier) == 24:
            params['id'] = channel_identifier
        else:
            params['forUsername'] = channel_identifier
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get('items'):
            channel = data['items'][0]
            return {
                'id': channel['id'],
                'title': channel['snippet']['title'],
                'description': channel['snippet']['description'][:200] + '...' if len(channel['snippet']['description']) > 200 else channel['snippet']['description'],
                'subscriber_count': int(channel['statistics'].get('subscriberCount', 0)),
                'video_count': int(channel['statistics'].get('videoCount', 0)),
                'view_count': int(channel['statistics'].get('viewCount', 0)),
                'thumbnail_url': channel['snippet']['thumbnails']['medium']['url'],
                'published_at': channel['snippet']['publishedAt']
            }
        else:
            return None
            
    except Exception as e:
        st.error(f"❌ Error fetching channel info: {str(e)}")
        return None

def search_channels(query):
    """Search for channels based on query"""
    try:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            'part': 'snippet',
            'q': query,
            'type': 'channel',
            'maxResults': 10,
            'key': APIConfig.YOUTUBE_API_KEY
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        channels = []
        for item in data.get('items', []):
            channel_info = get_channel_info(item['snippet']['channelId'])
            if channel_info:
                channels.append(channel_info)
        
        return channels
        
    except Exception as e:
        st.error(f"❌ Error searching channels: {str(e)}")
        return []

def get_uploads_playlist_id(channel_id):
    """Get the uploads playlist ID for a channel"""
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        'part': 'contentDetails',
        'id': channel_id,
        'key': APIConfig.YOUTUBE_API_KEY
    }
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get('items'):
        return data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return None

def fetch_channel_videos_paginated(channel_id, max_results_per_page=50, delay_between_requests=1):
    """Fetch all videos from a channel with pagination"""
    all_videos = []
    next_page_token = None
    page_count = 0
    
    # Create placeholders for progress tracking
    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    
    try:
        while True:
            page_count += 1
            
            # Update progress
            status_placeholder.info(f"📥 Fetching page {page_count}... ({len(all_videos)} videos so far)")
            
            # Search for videos in the channel
            search_url = "https://www.googleapis.com/youtube/v3/search"
            search_params = {
                'part': 'snippet',
                'channelId': channel_id,
                'type': 'video',
                'maxResults': max_results_per_page,
                'order': 'date',  # Get latest videos first
                'key': APIConfig.YOUTUBE_API_KEY
            }
            
            if next_page_token:
                search_params['pageToken'] = next_page_token
            
            search_response = requests.get(search_url, params=search_params, timeout=30)
            search_response.raise_for_status()
            search_data = search_response.json()
            
            if 'error' in search_data:
                st.error(f"❌ YouTube API Error: {search_data['error']['message']}")
                break
            
            if not search_data.get('items'):
                status_placeholder.success(f"✅ Completed! No more videos found.")
                break
            
            # Get video IDs for this batch
            video_ids = [item['id']['videoId'] for item in search_data['items']]
            
            # Get detailed statistics for this batch
            stats_url = "https://www.googleapis.com/youtube/v3/videos"
            stats_params = {
                'part': 'statistics,snippet,contentDetails',
                'id': ','.join(video_ids),
                'key': APIConfig.YOUTUBE_API_KEY
            }
            
            stats_response = requests.get(stats_url, params=stats_params, timeout=30)
            stats_response.raise_for_status()
            stats_data = stats_response.json()
            
            # Process videos from this batch
            batch_videos = []
            for video in stats_data.get('items', []):
                video_info = {
                    'video_id': video['id'],
                    'title': video['snippet']['title'],
                    'channel_title': video['snippet']['channelTitle'],
                    'channel_id': video['snippet']['channelId'],
                    'description': video['snippet']['description'][:500] + '...' if len(video['snippet']['description']) > 500 else video['snippet']['description'],
                    'published_at': video['snippet']['publishedAt'],
                    'view_count': int(video['statistics'].get('viewCount', 0)),
                    'like_count': int(video['statistics'].get('likeCount', 0)),
                    'comment_count': int(video['statistics'].get('commentCount', 0)),
                    'duration': video['contentDetails']['duration'],
                    'thumbnail_url': video['snippet']['thumbnails']['medium']['url'],
                    'video_url': f"https://www.youtube.com/watch?v={video['id']}",
                    'channel_url': f"https://www.youtube.com/channel/{video['snippet']['channelId']}",
                    'fetched_at': datetime.now().isoformat(),
                    'batch_number': page_count,
                    'fetch_mode': 'channel_pagination'
                }
                batch_videos.append(video_info)
            
            all_videos.extend(batch_videos)
            
            # Update progress bar
            if 'totalResults' in search_data.get('pageInfo', {}):
                total_results = search_data['pageInfo']['totalResults']
                progress = min(len(all_videos) / total_results, 1.0)
                progress_placeholder.progress(progress)
            
            # Check if there are more pages
            next_page_token = search_data.get('nextPageToken')
            if not next_page_token:
                status_placeholder.success(f"✅ Completed! Fetched all {len(all_videos)} videos from the channel.")
                break
            
            # Add delay to avoid hitting API rate limits
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)
            
            # Safety check to avoid infinite loops
            if page_count > 200:  # Max 200 pages (10,000 videos)
                status_placeholder.warning(f"⚠️ Reached maximum page limit. Fetched {len(all_videos)} videos.")
                break
        
        progress_placeholder.progress(1.0)
        return all_videos
        
    except Exception as e:
        status_placeholder.error(f"❌ Error during pagination: {str(e)}")
        return all_videos  # Return what we have so far

def fetch_youtube_data(query, max_results=10):
    """Fetch video data from YouTube API (original function for search)"""
    try:
        # Search for videos
        search_url = "https://www.googleapis.com/youtube/v3/search"
        search_params = {
            'part': 'snippet',
            'q': query,
            'type': 'video',
            'maxResults': max_results,
            'order': 'relevance',
            'key': APIConfig.YOUTUBE_API_KEY
        }
        
        search_response = requests.get(search_url, params=search_params, timeout=30)
        search_response.raise_for_status()
        search_data = search_response.json()
        
        if 'error' in search_data:
            st.error(f"❌ YouTube API Error: {search_data['error']['message']}")
            return []
        
        if not search_data.get('items'):
            st.warning("⚠️ No videos found for this query.")
            return []
        
        video_ids = [item['id']['videoId'] for item in search_data['items']]
        
        # Get detailed statistics for videos
        stats_url = "https://www.googleapis.com/youtube/v3/videos"
        stats_params = {
            'part': 'statistics,snippet,contentDetails',
            'id': ','.join(video_ids),
            'key': APIConfig.YOUTUBE_API_KEY
        }
        
        stats_response = requests.get(stats_url, params=stats_params, timeout=30)
        stats_response.raise_for_status()
        stats_data = stats_response.json()
        
        videos_info = []
        
        for video in stats_data.get('items', []):
            video_info = {
                'video_id': video['id'],
                'title': video['snippet']['title'],
                'channel_title': video['snippet']['channelTitle'],
                'channel_id': video['snippet']['channelId'],
                'description': video['snippet']['description'][:500] + '...' if len(video['snippet']['description']) > 500 else video['snippet']['description'],
                'published_at': video['snippet']['publishedAt'],
                'view_count': int(video['statistics'].get('viewCount', 0)),
                'like_count': int(video['statistics'].get('likeCount', 0)),
                'comment_count': int(video['statistics'].get('commentCount', 0)),
                'duration': video['contentDetails']['duration'],
                'thumbnail_url': video['snippet']['thumbnails']['medium']['url'],
                'video_url': f"https://www.youtube.com/watch?v={video['id']}",
                'channel_url': f"https://www.youtube.com/channel/{video['snippet']['channelId']}",
                'fetched_at': datetime.now().isoformat(),
                'search_query': query,
                'fetch_mode': 'search'
            }
            videos_info.append(video_info)
        
        return videos_info
    
    except Exception as e:
        st.error(f"❌ Error fetching YouTube data: {str(e)}")
        return []

def connect_mongodb():
    """Connect to MongoDB with better error handling"""
    try:
        client = MongoClient(APIConfig.MONGODB_CONNECTION, serverSelectionTimeoutMS=5000)
        # client.admin.command('ping')
        return client, None
    except pymongo.errors.ServerSelectionTimeoutError:
        error_msg = """
        **MongoDB Connection Failed!**
        
        **Possible solutions:**
        1. **Install MongoDB locally:**
           - Download from: https://www.mongodb.com/try/download/community
           - Start MongoDB service
        
        2. **Use MongoDB Atlas (Cloud):**
           - Go to: https://www.mongodb.com/cloud/atlas
           - Create free cluster
           - Get connection string
        
        3. **Check if MongoDB is running:**
           - Windows: `net start MongoDB`
           - Mac: `brew services start mongodb-community`
           - Linux: `sudo systemctl start mongod`
        """
        return None, error_msg
    except Exception as e:
        return None, f"MongoDB Error: {str(e)}"

def store_data_mongodb(videos_data, database_name="youtube_data", collection_name="videos"):
    """Store video data in MongoDB"""
    try:
        mongo_client, error = connect_mongodb()
        if not mongo_client:
            st.error(f"❌ {error}")
            return 0
        
        db = mongo_client[database_name]
        collection = db[collection_name]
        
        if videos_data:
            # Add timestamp to each document and check for duplicates
            new_videos = []
            duplicate_count = 0
            
            for video in videos_data:
                # Check if video already exists
                existing = collection.find_one({'video_id': video['video_id']})
                if not existing:
                    video['_inserted_at'] = datetime.now()
                    new_videos.append(video)
                else:
                    duplicate_count += 1
            
            if new_videos:
                result = collection.insert_many(new_videos)
                mongo_client.close()
                if duplicate_count > 0:
                    st.info(f"📝 Skipped {duplicate_count} duplicate videos, saved {len(result.inserted_ids)} new videos")
                return len(result.inserted_ids)
            else:
                mongo_client.close()
                st.info("📝 All videos already exist in database, no new videos saved")
                return 0
        
        mongo_client.close()
        return 0
    
    except Exception as e:
        st.error(f"❌ Error storing data in MongoDB: {str(e)}")
        return 0

def parse_duration(duration):
    """Convert YouTube duration format to readable format"""
    if not duration or duration == 'N/A':
        return "N/A"
    
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration)
    if match:
        hours, minutes, seconds = match.groups()
        hours = int(hours) if hours else 0
        minutes = int(minutes) if minutes else 0
        seconds = int(seconds) if seconds else 0
        
        if hours:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    return "N/A"

def format_number(num):
    """Format large numbers with K, M suffixes"""
    if num >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num/1_000:.1f}K"
    else:
        return str(num)

def show_mongodb_status():
    """Show MongoDB connection status in sidebar"""
    st.subheader("🗄️ Database Status")
    
    mongo_client, error = connect_mongodb()
    if mongo_client:
        st.success("✅ MongoDB Connected")
        mongo_client.close()
        
        try:
            client = MongoClient(APIConfig.MONGODB_CONNECTION, serverSelectionTimeoutMS=2000)
            db_names = client.list_database_names()
            st.info(f"📊 Available databases: {len(db_names)}")
            client.close()
        except:
            pass
    else:
        st.error("❌ MongoDB Disconnected")

def main():
    # Header
    st.markdown("<h1 class='main-header'>🎥 YouTube Data Fetcher with Pagination</h1>", unsafe_allow_html=True)
    st.markdown("Transform your ideas into smart YouTube searches, powered by AI with full channel pagination! 🚀")
    
    # Check API configuration first
    if not check_api_configuration():
        st.stop()
    
    # Fetch Mode Selection
    st.header("🎯 Choose Your Fetch Mode")
    fetch_mode = st.radio(
        "How would you like to fetch data?",
        ["🔍 Search Videos (Regular)", "📺 Fetch All Channel Videos (Paginated)"],
        help="Choose between regular search or complete channel data fetching"
    )
    
    # Sidebar for status and configuration
    with st.sidebar:
        st.header("⚙️ System Status")
        
        # API Status
        st.subheader("🔑 API Status")
        missing_keys = APIConfig.validate_keys()
        if not missing_keys:
            st.success("✅ All API keys configured")
        else:
            st.error(f"❌ Missing: {', '.join(missing_keys)}")
        
        # MongoDB Status
        show_mongodb_status()
        
        # Settings based on mode
        st.subheader("📊 Fetch Settings")
        
        if "Search Videos" in fetch_mode:
            max_results = st.slider("Max Videos to Fetch", min_value=5, max_value=50, value=15)
        else:
            st.info("📝 Channel mode will fetch ALL videos")
            batch_size = st.slider("Videos per API call", min_value=10, max_value=50, value=50)
            delay_between_calls = st.slider("Delay between calls (seconds)", min_value=0.5, max_value=5.0, value=1.0, step=0.5)
        
        # Auto-save option
        auto_save = st.checkbox("💾 Auto-save to MongoDB", value=True)
        
        if auto_save:
            database_name = st.text_input("Database Name", value="youtube_data")
            collection_name = st.text_input("Collection Name", value="videos")
    
    # Main interface based on mode
    if "Search Videos" in fetch_mode:
        # Regular search mode
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.header("💬 What do you want to explore?")
            user_prompt = st.text_area(
                "Describe what you're looking for:", 
                placeholder="Examples:\n• Learn Python programming for data science\n• Best cooking recipes for beginners\n• Latest tech reviews and comparisons\n• Fitness workouts at home",
                height=120
            )
            
            if st.session_state.smart_query:
                st.info(f"🎯 **Current Smart Query:** {st.session_state.smart_query}")
            
            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                search_button = st.button("🔍 Fetch Videos", type="primary", use_container_width=True)
            with col_btn2:
                clear_button = st.button("🗑️ Clear Results", use_container_width=True)
        
        with col2:
            st.header("📊 Quick Stats")
            if st.session_state.videos_data:
                total_videos = len(st.session_state.videos_data)
                total_views = sum(video['view_count'] for video in st.session_state.videos_data)
                total_likes = sum(video['like_count'] for video in st.session_state.videos_data)
                avg_views = total_views // total_videos if total_videos > 0 else 0
                
                st.metric("📹 Total Videos", total_videos)
                st.metric("👀 Total Views", format_number(total_views))
                st.metric("👍 Total Likes", format_number(total_likes))
                st.metric("📈 Avg Views", format_number(avg_views))
            else:
                st.info("No data yet. Start by entering a prompt above! 👆")
        
        # Process regular search
        if search_button and user_prompt.strip():
            if not configure_gemini():
                return
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                status_text.text("🤖 AI is crafting the perfect search query...")
                smart_query = generate_smart_query(user_prompt)
                st.session_state.smart_query = smart_query
                st.success(f"✨ **AI Generated Query:** `{smart_query}`")
                progress_bar.progress(50)
                
                status_text.text("🔍 Searching YouTube...")
                videos_data = fetch_youtube_data(smart_query, max_results)
                
                if videos_data:
                    st.session_state.videos_data = videos_data
                    progress_bar.progress(90)
                    
                    if auto_save:
                        stored_count = store_data_mongodb(videos_data, database_name, collection_name)
                        if stored_count > 0:
                            st.success(f"💾 Saved {stored_count} videos to MongoDB!")
                    
                    progress_bar.progress(100)
                    status_text.text("🎉 Success!")
                    st.balloons()
                
            except Exception as e:
                st.error(f"💥 Error: {str(e)}")
    
    else:
        # Channel pagination mode
        st.header("📺 Channel Data Fetcher")
        st.markdown("Enter a channel name or search term to find channels, then fetch ALL their videos!")
        
        # Channel search
        channel_search = st.text_input(
            "🔍 Search for Channel:",
            placeholder="Examples: T-Series, PewDiePie, MrBeast, Cocomelon",
            help="Enter channel name, username, or search term"
        )
        
        search_channels_btn = st.button("🔍 Find Channels", type="secondary")
        
        if search_channels_btn and channel_search:
            with st.spinner("🔍 Searching for channels..."):
                channels = search_channels(channel_search)
                
                if channels:
                    st.success(f"✅ Found {len(channels)} channels!")
                    
                    # Display channels for selection
                    st.subheader("📺 Select a Channel:")
                    
                    for i, channel in enumerate(channels):
                        with st.expander(f"📺 {channel['title']} - {format_number(channel['video_count'])} videos"):
                            col1, col2 = st.columns([1, 2])
                            
                            with col1:
                                st.image(channel['thumbnail_url'])
                            
                            with col2:
                                st.markdown(f"**📊 Statistics:**")
                                st.markdown(f"• 🎥 Videos: {format_number(channel['video_count'])}")
                                st.markdown(f"• 👥 Subscribers: {format_number(channel['subscriber_count'])}")
                                st.markdown(f"• 👀 Total Views: {format_number(channel['view_count'])}")
                                st.markdown(f"• 📅 Created: {channel['published_at'][:10]}")
                                
                                if st.button(f"📥 Fetch All Videos from {channel['title']}", key=f"fetch_{i}"):
                                    st.session_state.selected_channel = channel
                                    st.rerun()
        
        # Process selected channel
        if hasattr(st.session_state, 'selected_channel'):
            channel = st.session_state.selected_channel
            
            st.markdown("---")
            st.header(f"📥 Fetching Videos from: {channel['title']}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("🎥 Total Videos", format_number(channel['video_count']))
            with col2:
                st.metric("👥 Subscribers", format_number(channel['subscriber_count']))
            with col3:
                st.metric("👀 Total Views", format_number(channel['view_count']))
            
            st.markdown(f"**📝 Description:** {channel['description']}")
            
            # Fetch button
            fetch_all_btn = st.button(
                f"🚀 Start Fetching All {format_number(channel['video_count'])} Videos", 
                type="primary",
                disabled=st.session_state.fetching_in_progress
            )
            
            if fetch_all_btn:
                st.session_state.fetching_in_progress = True
                
                st.warning(f"⚠️ **Important:** This will fetch ALL {format_number(channel['video_count'])} videos from {channel['title']}. This may take several minutes and will consume API quota.")
                
                # Confirm before proceeding
                if st.button("✅ Yes, Fetch All Videos!", type="primary"):
                    
                    st.markdown("---")
                    st.header("📊 Fetching Progress")
                    
                    start_time = datetime.now()
                    uploads_playlist_id = get_uploads_playlist_id(channel['id'])
                    if uploads_playlist_id:
                        all_videos = fetch_all_videos_from_playlist(
                            uploads_playlist_id,
                            batch_size,
                            delay_between_calls
                        )
                    else:
                        st.error("❌ Could not find uploads playlist for this channel.")
                        all_videos = []
                    end_time = datetime.now()
                    
                    if all_videos:
                        st.session_state.videos_data = all_videos
                        st.session_state.channel_info = channel
                        
                        # Show completion stats
                        duration = (end_time - start_time).total_seconds()
                        
                        st.success(f"🎉 **Fetch Complete!**")
                        st.markdown(f"""
                        <div class="success-box">
                        <h3>📊 Fetch Summary</h3>
                        <ul>
                        <li>✅ <strong>Total Videos Fetched:</strong> {len(all_videos):,}</li>
                        <li>⏱️ <strong>Time Taken:</strong> {duration/60:.1f} minutes</li>
                        <li>📺 <strong>Channel:</strong> {channel['title']}</li>
                        <li>🎯 <strong>Success Rate:</strong> {(len(all_videos)/channel['video_count']*100):.1f}%</li>
                        </ul>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        # Auto-save if enabled
                        if auto_save:
                            with st.spinner("💾 Saving to MongoDB..."):
                                stored_count = store_data_mongodb(all_videos, database_name, collection_name)
                                if stored_count > 0:
                                    st.success(f"💾 Successfully saved {stored_count} videos to MongoDB!")
                        
                        st.balloons()
                        
                        # Update pagination info
                        st.session_state.pagination_info = {
                            'total_fetched': len(all_videos),
                            'channel_name': channel['title'],
                            'fetch_time': duration,
                            'success_rate': (len(all_videos)/channel['video_count']*100) if channel['video_count'] > 0 else 0
                        }
                    
                    st.session_state.fetching_in_progress = False
                    st.rerun()
    
    # Clear results
    if st.button("🗑️ Clear All Results"):
        st.session_state.videos_data = []
        st.session_state.smart_query = ""
        st.session_state.pagination_info = {}
        st.session_state.channel_info = {}
        if hasattr(st.session_state, 'selected_channel'):
            del st.session_state.selected_channel
        st.rerun()
    
    # Display results
    if st.session_state.videos_data:
        st.markdown("---")
        
        # Show pagination info if available
        if st.session_state.pagination_info:
            info = st.session_state.pagination_info
            st.markdown(f"""
            <div class="pagination-info">
            <h3>📊 Pagination Summary</h3>
            <p><strong>📺 Channel:</strong> {info['channel_name']}</p>
            <p><strong>🎥 Videos Fetched:</strong> {info['total_fetched']:,}</p>
            <p><strong>⏱️ Fetch Time:</strong> {info['fetch_time']/60:.1f} minutes</p>
            <p><strong>🎯 Success Rate:</strong> {info['success_rate']:.1f}%</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.header("🎬 Your Video Collection")
        
        # Display options
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            view_mode = st.selectbox("👁️ View Mode", ["🎴 Cards", "📊 Table", "📝 Detailed"])
        with col2:
            sort_by = st.selectbox("📈 Sort By", ["👀 Views", "👍 Likes", "💬 Comments", "📅 Date"])
        with col3:
            sort_order = st.selectbox("📋 Order", ["⬇️ High to Low", "⬆️ Low to High"])
        with col4:
            if len(st.session_state.videos_data) > 50:
                page_size = st.selectbox("📄 Videos per page", [25, 50, 100, 200], index=1)
            else:
                page_size = len(st.session_state.videos_data)
        
        # Sort data
        reverse_order = sort_order == "⬇️ High to Low"
        
        if sort_by == "👀 Views":
            sorted_data = sorted(st.session_state.videos_data, key=lambda x: x['view_count'], reverse=reverse_order)
        elif sort_by == "👍 Likes":
            sorted_data = sorted(st.session_state.videos_data, key=lambda x: x['like_count'], reverse=reverse_order)
        elif sort_by == "💬 Comments":
            sorted_data = sorted(st.session_state.videos_data, key=lambda x: x['comment_count'], reverse=reverse_order)
        else:
            sorted_data = sorted(st.session_state.videos_data, key=lambda x: x['published_at'], reverse=reverse_order)
        
        # Pagination for large datasets
        total_videos = len(sorted_data)
        total_pages = (total_videos + page_size - 1) // page_size
        
        if total_pages > 1:
            st.markdown(f"**📊 Showing {total_videos:,} videos across {total_pages} pages**")
            
            # Page navigation
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                current_page = st.slider(
                    f"Select Page (1-{total_pages})", 
                    min_value=1, 
                    max_value=total_pages, 
                    value=1,
                    help=f"Navigate through {total_pages} pages of results"
                )
            
            # Calculate start and end indices
            start_idx = (current_page - 1) * page_size
            end_idx = min(start_idx + page_size, total_videos)
            page_data = sorted_data[start_idx:end_idx]
            
            st.info(f"📄 Showing videos {start_idx + 1}-{end_idx} of {total_videos:,}")
        else:
            page_data = sorted_data
            current_page = 1
        
        # Display based on view mode
        if view_mode == "🎴 Cards":
            cols = st.columns(2)
            for i, video in enumerate(page_data):
                with cols[i % 2]:
                    with st.container():
                        st.image(video['thumbnail_url'], use_column_width=True)
                        st.markdown(f"**[{video['title']}]({video['video_url']})**")
                        st.markdown(f"📺 [{video['channel_title']}]({video['channel_url']})")
                        st.write(f"⏱️ {parse_duration(video['duration'])} • 📅 {video['published_at'][:10]}")
                        
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("👀", format_number(video['view_count']))
                        with col2:
                            st.metric("👍", format_number(video['like_count']))
                        with col3:
                            st.metric("💬", format_number(video['comment_count']))
                        
                        # Show batch info for paginated results
                        if 'batch_number' in video:
                            st.caption(f"📦 Batch {video['batch_number']}")
                        
                        st.markdown("---")
        
        elif view_mode == "📊 Table":
            df = pd.DataFrame(page_data)
            display_columns = ['title', 'channel_title', 'view_count', 'like_count', 'comment_count', 'published_at']
            if 'batch_number' in df.columns:
                display_columns.append('batch_number')
            
            display_df = df[display_columns].copy()
            column_names = ['📺 Title', '🎭 Channel', '👀 Views', '👍 Likes', '💬 Comments', '📅 Published']
            if 'batch_number' in display_df.columns:
                column_names.append('📦 Batch')
            
            display_df.columns = column_names
            display_df['👀 Views'] = display_df['👀 Views'].apply(format_number)
            display_df['👍 Likes'] = display_df['👍 Likes'].apply(format_number)
            display_df['💬 Comments'] = display_df['💬 Comments'].apply(format_number)
            display_df['📅 Published'] = pd.to_datetime(display_df['📅 Published']).dt.strftime('%Y-%m-%d')
            
            st.dataframe(display_df, use_container_width=True)
        
        else:  # Detailed List
            for i, video in enumerate(page_data, start=start_idx + 1):
                with st.expander(f"#{i} {video['title']} - {format_number(video['view_count'])} views"):
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.image(video['thumbnail_url'])
                        st.markdown(f"**[🎥 Watch Video]({video['video_url']})**")
                    
                    with col2:
                        st.markdown(f"**📺 Channel:** [{video['channel_title']}]({video['channel_url']})")
                        st.markdown(f"**⏱️ Duration:** {parse_duration(video['duration'])}")
                        st.markdown(f"**📅 Published:** {video['published_at'][:10]}")
                        
                        if 'batch_number' in video:
                            st.markdown(f"**📦 Batch:** {video['batch_number']}")
                        
                        col_a, col_b, col_c = st.columns(3)
                        with col_a:
                            st.metric("👀 Views", format_number(video['view_count']))
                        with col_b:
                            st.metric("👍 Likes", format_number(video['like_count']))
                        with col_c:
                            st.metric("💬 Comments", format_number(video['comment_count']))
                    
                    st.markdown("**📝 Description:**")
                    st.write(video['description'])
        
        # Analytics Section
        if len(st.session_state.videos_data) > 10:
            st.markdown("---")
            st.header("📈 Analytics Dashboard")
            
            # Calculate analytics
            total_videos = len(st.session_state.videos_data)
            total_views = sum(video['view_count'] for video in st.session_state.videos_data)
            total_likes = sum(video['like_count'] for video in st.session_state.videos_data)
            total_comments = sum(video['comment_count'] for video in st.session_state.videos_data)
            avg_views = total_views // total_videos if total_videos > 0 else 0
            avg_likes = total_likes // total_videos if total_videos > 0 else 0
            
            # Show analytics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📹 Total Videos", f"{total_videos:,}")
            with col2:
                st.metric("👀 Total Views", format_number(total_views))
            with col3:
                st.metric("👍 Total Likes", format_number(total_likes))
            with col4:
                st.metric("💬 Total Comments", format_number(total_comments))
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Avg Views/Video", format_number(avg_views))
            with col2:
                st.metric("📊 Avg Likes/Video", format_number(avg_likes))
            with col3:
                engagement_rate = (total_likes / total_views * 100) if total_views > 0 else 0
                st.metric("📊 Engagement Rate", f"{engagement_rate:.2f}%")
            
            # Top performers
            st.subheader("🏆 Top Performing Videos")
            top_videos = sorted(st.session_state.videos_data, key=lambda x: x['view_count'], reverse=True)[:5]
            
            for i, video in enumerate(top_videos, 1):
                st.markdown(f"**#{i}** [{video['title']}]({video['video_url']}) - {format_number(video['view_count'])} views")
        
        # Export section
        st.markdown("---")
        st.header("📥 Export Your Data")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📊 Download CSV", use_container_width=True):
                df = pd.DataFrame(st.session_state.videos_data)
                csv = df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download CSV File",
                    data=csv,
                    file_name=f"youtube_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            if st.button("📋 Download JSON", use_container_width=True):
                json_data = json.dumps(st.session_state.videos_data, indent=2)
                st.download_button(
                    label="⬇️ Download JSON File",
                    data=json_data,
                    file_name=f"youtube_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        with col3:
            if st.button("💾 Manual Save to DB", use_container_width=True):
                stored_count = store_data_mongodb(st.session_state.videos_data, database_name, collection_name)
                if stored_count > 0:
                    st.success(f"✅ Saved {stored_count} videos!")

def fetch_all_videos_from_playlist(
    playlist_id, 
    max_results_per_page=50, 
    delay_between_requests=1, 
    database_name="youtube_data", 
    collection_name="videos"
):
    """Fetch all videos from a playlist (uploads playlist for a channel) and store each batch in MongoDB"""
    all_videos = []
    next_page_token = None
    page_count = 0

    progress_placeholder = st.empty()
    status_placeholder = st.empty()

    try:
        while True:
            page_count += 1
            status_placeholder.info(f"📥 Fetching page {page_count}... ({len(all_videos)} videos so far)")

            url = "https://www.googleapis.com/youtube/v3/playlistItems"
            params = {
                'part': 'snippet,contentDetails',
                'playlistId': playlist_id,
                'maxResults': max_results_per_page,
                'key': APIConfig.YOUTUBE_API_KEY
            }
            if next_page_token:
                params['pageToken'] = next_page_token

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get('items'):
                status_placeholder.success(f"✅ Completed! No more videos found.")
                break

            video_ids = [item['contentDetails']['videoId'] for item in data['items'] if 'contentDetails' in item and 'videoId' in item['contentDetails']]

            # Get detailed statistics for this batch
            batch_videos = []
            if video_ids:
                stats_url = "https://www.googleapis.com/youtube/v3/videos"
                stats_params = {
                    'part': 'statistics,snippet,contentDetails',
                    'id': ','.join(video_ids),
                    'key': APIConfig.YOUTUBE_API_KEY
                }
                stats_response = requests.get(stats_url, params=stats_params, timeout=30)
                stats_response.raise_for_status()
                stats_data = stats_response.json()

                for video in stats_data.get('items', []):
                    video_info = {
                        'video_id': video['id'],
                        'title': video['snippet']['title'],
                        'channel_title': video['snippet']['channelTitle'],
                        'channel_id': video['snippet']['channelId'],
                        'description': video['snippet']['description'][:500] + '...' if len(video['snippet']['description']) > 500 else video['snippet']['description'],
                        'published_at': video['snippet']['publishedAt'],
                        'view_count': int(video['statistics'].get('viewCount', 0)),
                        'like_count': int(video['statistics'].get('likeCount', 0)),
                        'comment_count': int(video['statistics'].get('commentCount', 0)),
                        'duration': video['contentDetails']['duration'],
                        'thumbnail_url': video['snippet']['thumbnails']['medium']['url'],
                        'video_url': f"https://www.youtube.com/watch?v={video['id']}",
                        'channel_url': f"https://www.youtube.com/channel/{video['snippet']['channelId']}",
                        'fetched_at': datetime.now().isoformat(),
                        'batch_number': page_count,
                        'fetch_mode': 'channel_playlist'
                    }
                    batch_videos.append(video_info)
                all_videos.extend(batch_videos)

                # Store this batch in MongoDB immediately
                stored_count = store_data_mongodb(batch_videos, database_name, collection_name)
                status_placeholder.info(f"💾 Stored {stored_count} new videos from batch {page_count} in MongoDB.")

            # Progress bar
            total_results = data.get('pageInfo', {}).get('totalResults', 0)
            if total_results:
                progress = min(len(all_videos) / total_results, 1.0)
                progress_placeholder.progress(progress)

            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                status_placeholder.success(f"✅ Completed! Fetched all {len(all_videos)} videos from the channel.")
                break

            if delay_between_requests > 0:
                time.sleep(delay_between_requests)

            if page_count > 500:  # Safety
                status_placeholder.warning(f"⚠️ Reached maximum page limit. Fetched {len(all_videos)} videos.")
                break

        progress_placeholder.progress(1.0)
        return all_videos

    except Exception as e:
        status_placeholder.error(f"❌ Error during playlist pagination: {str(e)}")
        return all_videos

if __name__ == "__main__":
    main()