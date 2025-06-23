from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd
import time
import streamlit as st

class YouTubeScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.quota_used = 0
        self.max_quota = 10000  # Daily quota limit
    
    def get_channel_id_from_name(self, channel_name):
        """Get channel ID from channel name"""
        try:
            # Remove @ symbol if present
            if channel_name.startswith('@'):
                channel_name = channel_name[1:]
            
            # Search for channel
            search_response = self.youtube.search().list(
                q=channel_name,
                type='channel',
                part='id,snippet',
                maxResults=5
            ).execute()
            
            self.quota_used += 100  # Search costs 100 units
            
            if search_response['items']:
                # Return the first match
                return search_response['items'][0]['id']['channelId']
            
            return None
            
        except Exception as e:
            st.error(f"Error searching for channel '{channel_name}': {str(e)}")
            return None
    
    def get_channel_info(self, channel_id):
        """Get basic channel information"""
        try:
            channel_response = self.youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            ).execute()
            
            self.quota_used += 1  # Channels.list costs 1 unit
            
            if channel_response['items']:
                channel = channel_response['items'][0]
                return {
                    'channel_id': channel_id,
                    'channel_name': channel['snippet']['title'],
                    'description': channel['snippet']['description'],
                    'subscriber_count': channel['statistics'].get('subscriberCount', 0),
                    'video_count': channel['statistics'].get('videoCount', 0),
                    'view_count': channel['statistics'].get('viewCount', 0)
                }
            
            return None
            
        except Exception as e:
            st.error(f"Error getting channel info: {str(e)}")
            return None
    
    def get_channel_videos(self, channel_id, batch_size=50, days_back=365, max_videos=None):
        """Get videos from a channel with pagination - supports unlimited videos"""
        try:
            # Calculate date threshold if specified
            published_after = None
            if days_back and days_back > 0:
                published_after = (datetime.utcnow() - timedelta(days=days_back)).isoformat("T") + "Z"
            
            videos = []
            next_page_token = None
            page_count = 0
            
            # Use channel uploads playlist for more comprehensive results
            uploads_playlist_id = self.get_uploads_playlist_id(channel_id)
            
            if uploads_playlist_id:
                # Use playlist method for better coverage
                videos = self.get_videos_from_playlist(uploads_playlist_id, batch_size, published_after, max_videos)
            else:
                # Fallback to search method
                while True:
                    # Break if we've reached the specified max_videos
                    if max_videos and len(videos) >= max_videos:
                        break
                    
                    current_batch_size = min(batch_size, 50)  # API limit is 50
                    if max_videos:
                        remaining = max_videos - len(videos)
                        current_batch_size = min(current_batch_size, remaining)
                    
                    search_params = {
                        'channelId': channel_id,
                        'part': 'id,snippet',
                        'maxResults': current_batch_size,
                        'order': 'date',
                        'type': 'video',
                        'pageToken': next_page_token
                    }
                    
                    if published_after:
                        search_params['publishedAfter'] = published_after
                    
                    search_response = self.youtube.search().list(**search_params).execute()
                    self.quota_used += 100  # Search costs 100 units
                    
                    video_items = search_response.get('items', [])
                    if not video_items:
                        st.info(f"No more videos found after {len(videos)} videos")
                        break
                    
                    # Get video IDs
                    video_ids = [item['id']['videoId'] for item in video_items]
                    
                    # Get detailed video statistics
                    video_stats = self.get_video_statistics(video_ids)
                    
                    # Combine video info with statistics
                    batch_videos = []
                    for item in video_items:
                        video_id = item['id']['videoId']
                        video_info = {
                            'video_id': video_id,
                            'title': item['snippet']['title'],
                            'description': item['snippet']['description'][:500],  # Truncate description
                            'published_at': item['snippet']['publishedAt'],
                            'channel_id': channel_id,
                            'thumbnail_url': item['snippet']['thumbnails'].get('medium', {}).get('url', ''),
                        }
                        
                        # Add statistics if available
                        if video_id in video_stats:
                            video_info.update(video_stats[video_id])
                        else:
                            # Default values if stats not available
                            video_info.update({
                                'view_count': 0,
                                'like_count': 0,
                                'comment_count': 0,
                                'duration': 'Unknown'
                            })
                        
                        batch_videos.append(video_info)
                    
                    videos.extend(batch_videos)
                    page_count += 1
                    
                    # Progress update
                    if page_count % 10 == 0:
                        st.info(f"📊 Scraped {len(videos)} videos so far... (Page {page_count})")
                    
                    # Check for next page
                    next_page_token = search_response.get('nextPageToken')
                    if not next_page_token:
                        st.success(f"✅ Reached end of channel videos. Total: {len(videos)} videos")
                        break
                    
                    # Check quota usage
                    if self.quota_used > self.max_quota * 0.8:  # Stop at 80% quota usage
                        st.warning(f"⚠️ Approaching quota limit. Used {self.quota_used} units. Scraped {len(videos)} videos so far.")
                        break
                    
                    # Small delay to avoid rate limiting
                    time.sleep(0.2)
            
            return videos
            
        except Exception as e:
            st.error(f"Error getting channel videos: {str(e)}")
            return []
    
    def get_uploads_playlist_id(self, channel_id):
        """Get the uploads playlist ID for a channel"""
        try:
            channel_response = self.youtube.channels().list(
                part='contentDetails',
                id=channel_id
            ).execute()
            
            self.quota_used += 1
            
            if channel_response['items']:
                return channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            
            return None
            
        except Exception as e:
            st.warning(f"Could not get uploads playlist: {str(e)}")
            return None
    
    def get_videos_from_playlist(self, playlist_id, batch_size=50, published_after=None, max_videos=None):
        """Get all videos from a playlist (more comprehensive than search)"""
        try:
            videos = []
            next_page_token = None
            page_count = 0
            
            while True:
                if max_videos and len(videos) >= max_videos:
                    break
                
                current_batch_size = min(batch_size, 50)
                if max_videos:
                    remaining = max_videos - len(videos)
                    current_batch_size = min(current_batch_size, remaining)
                
                playlist_response = self.youtube.playlistItems().list(
                    part='snippet',
                    playlistId=playlist_id,
                    maxResults=current_batch_size,
                    pageToken=next_page_token
                ).execute()
                
                self.quota_used += 1  # PlaylistItems.list costs 1 unit
                
                playlist_items = playlist_response.get('items', [])
                if not playlist_items:
                    break
                
                # Filter by date if specified
                filtered_items = []
                for item in playlist_items:
                    if published_after:
                        video_date = item['snippet']['publishedAt']
                        if video_date >= published_after:
                            filtered_items.append(item)
                    else:
                        filtered_items.append(item)
                
                if not filtered_items:
                    if published_after:
                        # If we're filtering by date and no items match, we might have gone too far back
                        break
                    continue
                
                # Get video IDs
                video_ids = [item['snippet']['resourceId']['videoId'] for item in filtered_items]
                
                # Get detailed video statistics
                video_stats = self.get_video_statistics(video_ids)
                
                # Combine playlist info with statistics
                batch_videos = []
                for item in filtered_items:
                    video_id = item['snippet']['resourceId']['videoId']
                    video_info = {
                        'video_id': video_id,
                        'title': item['snippet']['title'],
                        'description': item['snippet']['description'][:500],
                        'published_at': item['snippet']['publishedAt'],
                        'channel_id': item['snippet']['channelId'],
                        'thumbnail_url': item['snippet']['thumbnails'].get('medium', {}).get('url', ''),
                    }
                    
                    # Add statistics if available
                    if video_id in video_stats:
                        video_info.update(video_stats[video_id])
                    else:
                        video_info.update({
                            'view_count': 0,
                            'like_count': 0,
                            'comment_count': 0,
                            'duration': 'Unknown'
                        })
                    
                    batch_videos.append(video_info)
                
                videos.extend(batch_videos)
                page_count += 1
                
                # Progress update
                if page_count % 20 == 0:
                    st.info(f"📊 Scraped {len(videos)} videos from playlist... (Page {page_count})")
                
                # Check for next page
                next_page_token = playlist_response.get('nextPageToken')
                if not next_page_token:
                    st.success(f"✅ Completed playlist scraping. Total: {len(videos)} videos")
                    break
                
                # Check quota usage
                if self.quota_used > self.max_quota * 0.8:
                    st.warning(f"⚠️ Approaching quota limit. Used {self.quota_used} units. Scraped {len(videos)} videos so far.")
                    break
                
                # Small delay
                time.sleep(0.1)
            
            return videos
            
        except Exception as e:
            st.error(f"Error getting videos from playlist: {str(e)}")
            return []
    
    def get_video_statistics(self, video_ids):
        """Get detailed statistics for a list of video IDs"""
        try:
            # Split video IDs into chunks of 50 (API limit)
            video_stats = {}
            
            for i in range(0, len(video_ids), 50):
                chunk = video_ids[i:i+50]
                
                videos_response = self.youtube.videos().list(
                    part='statistics,contentDetails',
                    id=','.join(chunk)
                ).execute()
                
                self.quota_used += 1  # Videos.list costs 1 unit
                
                for item in videos_response.get('items', []):
                    video_id = item['id']
                    stats = item.get('statistics', {})
                    content_details = item.get('contentDetails', {})
                    
                    video_stats[video_id] = {
                        'view_count': int(stats.get('viewCount', 0)),
                        'like_count': int(stats.get('likeCount', 0)),
                        'comment_count': int(stats.get('commentCount', 0)),
                        'duration': content_details.get('duration', 'Unknown')
                    }
            
            return video_stats
            
        except Exception as e:
            st.error(f"Error getting video statistics: {str(e)}")
            return {}
    
    def scrape_channel(self, channel_id, batch_size=50, days_back=365, max_videos=200):
        """Main method to scrape a complete channel"""
        try:
            # Get channel info
            channel_info = self.get_channel_info(channel_id)
            if not channel_info:
                return []
            
            # Get channel videos
            videos = self.get_channel_videos(
                channel_id, 
                batch_size=batch_size, 
                days_back=days_back, 
                max_videos=max_videos
            )
            
            # Add channel name to each video
            for video in videos:
                video['channel_name'] = channel_info['channel_name']
                video['channel_subscriber_count'] = channel_info['subscriber_count']
            
            return videos
            
        except Exception as e:
            st.error(f"Error scraping channel: {str(e)}")
            return []
    
    def get_quota_usage(self):
        """Get current quota usage"""
        return {
            'used': self.quota_used,
            'limit': self.max_quota,
            'percentage': (self.quota_used / self.max_quota) * 100
        }
