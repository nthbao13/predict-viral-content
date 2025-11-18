import os
import json
import praw
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Reddit API
# -----------------------------
reddit = praw.Reddit(
    client_id=os.getenv("CLIENT_ID"),
    client_secret=os.getenv("CLIENT_SECRET"),
    user_agent=os.getenv("USER_AGENT")
)

print("✅ Đã kết nối Reddit API!")
print("🚀 Bắt đầu stream bài mới từ r/technology ...")

# -----------------------------
# Event Hub Producer
# -----------------------------
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")

producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STRING,
    eventhub_name=EVENTHUB_NAME
)

# -----------------------------
# Stream Reddit -> Event Hub
# -----------------------------
try:
    for submission in reddit.subreddit("all").stream.submissions(skip_existing=True):
        data = {
            "id": submission.id,
            "title": submission.title,
            "score": submission.score,
            "url": submission.url,
            "num_comments": submission.num_comments,
            "created_utc": submission.created_utc,
            "subreddit": submission.subreddit.display_name
        }

        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))
        producer.send_batch(event_data_batch)
        print("📤 Sent:", data["title"])

except KeyboardInterrupt:
    print("🛑 Dừng stream...")

finally:
    producer.close()
    print("✅ Producer đã đóng kết nối")
