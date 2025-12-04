import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Optional
import praw
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 5
BATCH_SIZE = 1

# Stats
stats = {
    "posts_processed": 0,
    "posts_sent": 0,
    "posts_failed": 0,
    "start_time": datetime.utcnow()
}

# -----------------------------
# Reddit API
# -----------------------------
try:
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),            # Trong video thay bằng giá trị thật
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),    # Trong video thay bằng giá trị thật
        user_agent=os.getenv('REDDIT_USER_AGENT')           # Trong video thay bằng giá trị thật
    )
    logger.info("✅ Kết nối Reddit thành công!")
except Exception as e:
    logger.error(f"❌ Lỗi kết nối Reddit: {str(e)}")
    exit(1)

# -----------------------------
# Event Hub Producer
# -----------------------------
try:
    EVENTHUB_NAME = os.getenv('EVENTHUB_NAME', 'reddit-posts')          # Mặc định là 'reddit-posts' nếu không có biến môi trường
    EVENTHUB_CONNECTION_STRING = os.getenv('EVENTHUB_CONNECTION_STRING')    # Trong video thay bằng giá trị thật

    if not EVENTHUB_CONNECTION_STRING:
        raise ValueError("EVENTHUB_CONNECTION_STRING not configured")
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    logger.info("✅ Kết nối Event Hub thành công!")
except Exception as e:
    logger.error(f"❌ Lỗi kết nối Event Hub: {str(e)}")
    exit(1)

# -----------------------------
# Subreddits
# -----------------------------
SUBREDDITS = [
    "popular", "news", "worldnews", "funny", "pics", "memes", "AskReddit", 
    "technology", "interestingasfuck", "aww", "WTF", "gaming", "movies", 
    "space"
]

multi_subreddit = "+".join(SUBREDDITS)

logger.info(f"🚀 Bắt đầu stream từ: {multi_subreddit}")

# Helper Functions
# -----------------------------
def extract_submission_data(submission) -> Optional[Dict]:
    """Extract và validate dữ liệu từ submission"""
    try:
        return {
            # Identity
            "id": submission.id,
            "title": submission.title[:500],  # Limit length
            "selftext": (submission.selftext or "")[:2000],
            "subreddit": submission.subreddit.display_name,
            "author": str(submission.author) if submission.author else "[deleted]",

            # Time & URL
            "created_utc": submission.created_utc,
            "url": submission.url,
            "permalink": submission.permalink,

            # Metadata
            "over_18": submission.over_18,
            "spoiler": submission.spoiler,
            "link_flair_text": submission.link_flair_text or "unknown",

            # Engagement
            "score": submission.score,
            "ups": submission.ups,
            "downs": submission.downs,
            "ratio": submission.upvote_ratio,
            "num_comments": submission.num_comments,
            "num_awards": submission.total_awards_received,

            # Additional features
            "is_video": submission.is_video,
            "has_media": bool(submission.media),

            # System
            "ingestion_timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"❌ Lỗi extract submission {submission.id}: {str(e)}")
        return None


def send_to_event_hub(producer, data: Dict, max_retries: int = 3, retry_delay: int = 5) -> bool:
    """Gửi data tới Event Hub với retry logic"""
    for attempt in range(max_retries):
        try:
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(data)))
            producer.send_batch(batch)
            
            logger.info(f"✅ Sent: {data['subreddit']} — {data['title'][:50]}")
            stats["posts_sent"] += 1
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            stats["posts_failed"] += 1
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Failed to send after {max_retries} attempts")
                return False


def print_stats():
    """In thống kê"""
    elapsed = (datetime.utcnow() - stats["start_time"]).total_seconds()
    rate = stats["posts_sent"] / elapsed if elapsed > 0 else 0
    logger.info(f"📊 Stats - Processed: {stats['posts_processed']} | Sent: {stats['posts_sent']} | Failed: {stats['posts_failed']} | Rate: {rate:.2f} posts/sec")

# -----------------------------
# MAIN STREAM LOOP
# -----------------------------
try:
    for submission in reddit.subreddit(multi_subreddit).stream.submissions(skip_existing=True):
        stats["posts_processed"] += 1
        
        # Extract submission data
        data = extract_submission_data(submission)
        if data is None:
            continue
        
        # Send to Event Hub with retry
        send_to_event_hub(producer, data, MAX_RETRIES, RETRY_DELAY)
        
        # Print stats every 100 posts
        if stats["posts_processed"] % 100 == 0:
            print_stats()

except KeyboardInterrupt:
    logger.info("🛑 Dừng chương trình...")

except Exception as e:
    logger.error(f"❌ Lỗi stream Reddit: {str(e)}")

finally:
    try:
        producer.close()
        logger.info("✅ Đã đóng kết nối Event Hub")
    except Exception as e:
        logger.error(f"❌ Lỗi đóng Event Hub: {str(e)}")
    
    # Print final stats
    print_stats()
