import pymongo
from pymongo import MongoClient
from datetime import datetime
from typing import Dict, List, Any, Optional
import streamlit as st
import os

class MongoDBStorage:
    def __init__(self, connection_string: str = None, database_name: str = "youtube_scraper"):
        """Initialize MongoDB connection"""
        self.connection_string = connection_string or os.getenv("MONGODB_URI")
        self.database_name = database_name
        self.client = None
        self.db = None
        self.collection = None
        
        if self.connection_string:
            self.connect()
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            self.collection = self.db.videos
            
            # Test connection
            self.client.admin.command('ping')
            st.success("✅ Connected to MongoDB")
            return True
            
        except Exception as e:
            st.error(f"❌ MongoDB connection failed: {str(e)}")
            return False
    
    def store_videos_batch(self, channel_name: str, videos: List[Dict[str, Any]], batch_info: Dict[str, Any] = None):
        """Store a batch of videos in MongoDB"""
        if self.collection is None:
            st.warning("⚠️ MongoDB not connected. Skipping MongoDB storage.")
            return False
        
        try:
            # Prepare documents for insertion
            documents = []
            for video in videos:
                doc = video.copy()
                doc.update({
                    'channel_name': channel_name,
                    'scraped_at': datetime.utcnow(),
                    'batch_info': batch_info or {}
                })
                documents.append(doc)
            
            # Insert batch
            if documents:
                result = self.collection.insert_many(documents, ordered=False)
                st.info(f"💾 Stored {len(result.inserted_ids)} videos in MongoDB")
                return True
            
        except pymongo.errors.BulkWriteError as e:
            # Handle duplicate key errors gracefully
            inserted_count = e.details.get('nInserted', 0)
            if inserted_count > 0:
                st.info(f"💾 Stored {inserted_count} new videos in MongoDB (some duplicates skipped)")
            return True
            
        except Exception as e:
            st.error(f"❌ Error storing to MongoDB: {str(e)}")
            return False
    
    def get_channel_videos(self, channel_name: str) -> List[Dict[str, Any]]:
        """Get all videos for a specific channel from MongoDB"""
        if self.collection is None:
            return []
        
        try:
            cursor = self.collection.find({'channel_name': channel_name})
            return list(cursor)
        except Exception as e:
            st.error(f"❌ Error retrieving from MongoDB: {str(e)}")
            return []
    
    def get_video_count(self, channel_name: str = None) -> int:
        """Get total video count or count for specific channel"""
        if self.collection is None:
            return 0
        
        try:
            filter_query = {'channel_name': channel_name} if channel_name else {}
            return self.collection.count_documents(filter_query)
        except Exception as e:
            st.error(f"❌ Error counting documents: {str(e)}")
            return 0
    
    def get_channels_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all channels in database"""
        if self.collection is None:
            return []
        
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': '$channel_name',
                        'video_count': {'$sum': 1},
                        'total_views': {'$sum': {'$toDouble': '$view_count'}},
                        'latest_video': {'$max': '$published_at'},
                        'last_scraped': {'$max': '$scraped_at'}
                    }
                },
                {
                    '$sort': {'video_count': -1}
                }
            ]
            
            result = list(self.collection.aggregate(pipeline))
            return result
            
        except Exception as e:
            st.error(f"❌ Error getting channel summary: {str(e)}")
            return []
    
    def delete_channel_data(self, channel_name: str) -> bool:
        """Delete all data for a specific channel"""
        if self.collection is None:
            return False
        
        try:
            result = self.collection.delete_many({'channel_name': channel_name})
            st.success(f"🗑️ Deleted {result.deleted_count} videos for {channel_name}")
            return True
        except Exception as e:
            st.error(f"❌ Error deleting channel data: {str(e)}")
            return False
    
    def create_indexes(self):
        """Create useful indexes for better performance"""
        if self.collection is None:
            return False
        
        try:
            # Create indexes
            self.collection.create_index([('channel_name', 1)])
            self.collection.create_index([('video_id', 1)], unique=True)
            self.collection.create_index([('published_at', -1)])
            self.collection.create_index([('view_count', -1)])
            st.info("📊 MongoDB indexes created successfully")
            return True
        except Exception as e:
            st.warning(f"⚠️ Index creation warning: {str(e)}")
            return False
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()