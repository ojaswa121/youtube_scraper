import json
import os
from datetime import datetime
from typing import Dict, List, Any
import streamlit as st
from mongodb_storage import MongoDBStorage

class DataStorage:
    def __init__(self, json_directory="data", mongodb_uri=None):
        self.json_directory = json_directory
        self.memory_storage = {}  # In-memory storage for session
        
        # Create data directory if it doesn't exist
        if not os.path.exists(self.json_directory):
            os.makedirs(self.json_directory)
        
        # Initialize MongoDB storage
        self.mongodb = MongoDBStorage(mongodb_uri) if mongodb_uri else None
    
    def store_channel_data(self, channel_name: str, video_data: List[Dict[str, Any]], batch_info: Dict[str, Any] = None):
        """Store channel data in JSON, memory, and MongoDB simultaneously"""
        # Clean channel name for filename
        safe_channel_name = self._sanitize_filename(channel_name)
        
        # Prepare data structure
        channel_data = {
            'channel_name': channel_name,
            'scrape_timestamp': datetime.now().isoformat(),
            'video_count': len(video_data),
            'videos': video_data,
            'batch_info': batch_info or {}
        }
        
        # Store in JSON file
        json_filename = f"{safe_channel_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        json_path = os.path.join(self.json_directory, json_filename)
        
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(channel_data, f, indent=2, ensure_ascii=False)
            
            st.success(f"💾 Saved {len(video_data)} videos to {json_filename}")
            
        except Exception as e:
            st.error(f"❌ Error saving to JSON: {str(e)}")
        
        # Store in MongoDB if available
        if self.mongodb:
            self.mongodb.store_videos_batch(channel_name, video_data, batch_info)
        
        # Store in memory
        self.memory_storage[channel_name] = {
            'data': video_data,
            'timestamp': datetime.now().isoformat(),
            'json_file': json_filename
        }
    
    def get_channel_data(self, channel_name: str) -> Dict[str, Any]:
        """Get channel data from memory storage"""
        return self.memory_storage.get(channel_name, {})
    
    def get_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all stored data from memory"""
        return {name: info['data'] for name, info in self.memory_storage.items()}
    
    def load_from_json(self, filename: str) -> Dict[str, Any]:
        """Load data from a specific JSON file"""
        json_path = os.path.join(self.json_directory, filename)
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"❌ Error loading from JSON: {str(e)}")
            return {}
    
    def get_json_files(self) -> List[str]:
        """Get list of all JSON files in storage directory"""
        try:
            files = [f for f in os.listdir(self.json_directory) if f.endswith('.json')]
            return sorted(files, reverse=True)  # Most recent first
        except Exception as e:
            st.error(f"❌ Error listing JSON files: {str(e)}")
            return []
    
    def save_to_json(self, data: Dict[str, Any], filename: str):
        """Save arbitrary data to JSON file"""
        json_path = os.path.join(self.json_directory, filename)
        
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            st.error(f"❌ Error saving to JSON: {str(e)}")
    
    def delete_json_file(self, filename: str) -> bool:
        """Delete a specific JSON file"""
        json_path = os.path.join(self.json_directory, filename)
        
        try:
            if os.path.exists(json_path):
                os.remove(json_path)
                return True
            return False
        except Exception as e:
            st.error(f"❌ Error deleting JSON file: {str(e)}")
            return False
    
    def clear_all_data(self):
        """Clear all in-memory data"""
        self.memory_storage.clear()
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        json_files = self.get_json_files()
        memory_channels = len(self.memory_storage)
        total_videos_memory = sum(len(info['data']) for info in self.memory_storage.values())
        
        # Calculate total size of JSON files
        total_json_size = 0
        for filename in json_files:
            try:
                json_path = os.path.join(self.json_directory, filename)
                total_json_size += os.path.getsize(json_path)
            except:
                pass
        
        return {
            'json_files_count': len(json_files),
            'json_total_size_mb': round(total_json_size / (1024 * 1024), 2),
            'memory_channels': memory_channels,
            'memory_total_videos': total_videos_memory,
            'latest_json_file': json_files[0] if json_files else None
        }
    
    def merge_json_files(self, output_filename: str = None) -> str:
        """Merge all JSON files into a single file"""
        if not output_filename:
            output_filename = f"merged_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        json_files = self.get_json_files()
        merged_data = {
            'merge_timestamp': datetime.now().isoformat(),
            'source_files': json_files,
            'channels': {}
        }
        
        total_videos = 0
        
        for filename in json_files:
            file_data = self.load_from_json(filename)
            if file_data and 'videos' in file_data:
                channel_name = file_data.get('channel_name', filename)
                
                if channel_name not in merged_data['channels']:
                    merged_data['channels'][channel_name] = []
                
                merged_data['channels'][channel_name].extend(file_data['videos'])
                total_videos += len(file_data['videos'])
        
        merged_data['total_videos'] = total_videos
        merged_data['total_channels'] = len(merged_data['channels'])
        
        # Save merged data
        self.save_to_json(merged_data, output_filename)
        
        return output_filename
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to be safe for filesystem"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove extra spaces and limit length
        filename = filename.strip().replace(' ', '_')[:50]
        
        return filename
