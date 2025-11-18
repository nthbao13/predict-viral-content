import os
import json
from azure.eventhub import EventHubConsumerClient
from dotenv import load_dotenv

load_dotenv()

EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")

def on_event(partition_context, event):
    data = json.loads(event.body_as_str())
    print("\n📥 Received message:")
    print(json.dumps(data, indent=4, ensure_ascii=False))
    partition_context.update_checkpoint(event)

client = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STRING,
    consumer_group="$Default",
    eventhub_name=EVENTHUB_NAME
)

print("👀 Listening messages...")

try:
    with client:
        client.receive(
            on_event=on_event,
            starting_position="@latest"  # chỉ nhận bài mới
        )
except KeyboardInterrupt:
    print("🛑 Dừng consumer...")
finally:
    client.close()
    print("✅ Consumer đã đóng kết nối")
