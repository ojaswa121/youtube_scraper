import requests
import time

def get_trending_videos(api_key, category="music", country="us", language="en"):
    """
    Fetch trending YouTube videos using the YouTube Trends API.

    Parameters:
        api_key (str): Your SearchAPI.io API key.
        category (str): Trending category (e.g., now, music, gaming, films).
        country (str): Country code (e.g., us, in, gb).
        language (str): Language code (e.g., en, es, fr).

    Returns:
        list: A list of trending video dictionaries.
    """
    url = "https://www.searchapi.io/api/v1/search"
    params = {
        "engine": "youtube_trends",
        "bp": category,
        "gl": country,
        "hl": language,
        "api_key": api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("trending", [])
    except Exception as e:
        print(f"Error fetching trending videos: {e}")
        return []


def extract_unique_channels(trending_videos):
    """
    Extract unique channel names from trending videos.

    Parameters:
        trending_videos (list): List of video dictionaries.

    Returns:
        list[str]: Sorted list of unique channel names.
    """
    channels = set()
    for video in trending_videos:
        channel_title = video.get("channel", {}).get("title")
        if channel_title:
            channels.add(channel_title)
    return sorted(list(channels))


def get_unique_trending_channels(api_key, category="music", country="us", language="en"):
    """
    Fetch trending videos and return unique channel names.

    Returns:
        list[str]: Sorted list of unique trending channel names.
    """
    trending_videos = get_trending_videos(api_key, category, country, language)
    if not trending_videos:
        return []
    return extract_unique_channels(trending_videos)


# Optional display/debugging helpers for CLI usage

def display_trending_video_details(trending_videos):
    """
    Display detailed information for each trending video.
    """
    print(f"\n🔥 Trending YouTube Videos ({len(trending_videos)} found):\n")
    for video in trending_videos:
        print(f"{video['position']}. {video['title']}")
        print(f"   🔗 Link: {video['link']}")
        print(f"   📺 Channel: {video['channel']['title']}")
        print(f"   ✅ Verified: {video['channel'].get('is_verified', False)}")
        print(f"   ⏱ Length: {video.get('length', 'N/A')}")
        print(f"   📅 Published: {video.get('published_time', 'N/A')}")
        print(f"   👀 Views: {video.get('views', 0):,}")
        print(f"   🖼 Thumbnail: {video.get('thumbnail', 'N/A')}")
        print("-" * 60)


def get_channel_videos(api_key, channel_name):
    """
    Search for recent videos uploaded by a specific channel.

    Returns:
        list: A list of video dictionaries.
    """
    url = "https://www.searchapi.io/api/v1/search"
    params = {
        "engine": "youtube",
        "q": channel_name,
        "api_key": api_key
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        results = response.json()
        return results.get("videos", [])
    except Exception as e:
        print(f"Error searching for videos by channel '{channel_name}': {e}")
        return []


def display_channel_videos(api_key, unique_channels, limit=3):
    """
    Display recent videos for each unique channel.
    """
    print("\n📽 Fetching recent videos from each trending channel...\n")
    for channel in unique_channels:
        print(f"🔍 Channel: {channel}")
        videos = get_channel_videos(api_key, channel)
        if not videos:
            print("   ⚠️ No videos found or error occurred.")
        else:
            for video in videos[:limit]:
                title = video.get("title", "N/A")
                link = video.get("link", "N/A")
                views = video.get("views", 0)
                published = video.get("published_time", "N/A")
                duration = video.get("length", "N/A")
                print(f"   🎬 {title}")
                print(f"      ⏱ {duration} | 👀 {views:,} views | 📅 {published}")
                print(f"      🔗 {link}")
        print("-" * 60)
        time.sleep(1)  # prevent rate limiting


# CLI test runner (optional)

if __name__ == "__main__":
    API_KEY = "hidden---change---for---testing"  # Replace with your own API key
    CATEGORY = "music"
    COUNTRY = "in"
    LANGUAGE = "en"

    trending_videos = get_trending_videos(API_KEY, CATEGORY, COUNTRY, LANGUAGE)
    if trending_videos:
        display_trending_video_details(trending_videos)
        unique_channels = extract_unique_channels(trending_videos)
        print(f"\n🎤 Total Unique Channels: {len(unique_channels)}")
        for i, ch in enumerate(unique_channels, 1):
            print(f"{i}. {ch}")
        display_channel_videos(API_KEY, unique_channels)
    else:
        print("❌ No trending videos found.")
