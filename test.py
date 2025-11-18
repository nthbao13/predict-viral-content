import os
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

print(f"🔗 Connection String: {EVENTHUB_CONNECTION_STRING[:50]}...")
print(f"🔗 Event Hub Name: {EVENTHUB_NAME}")

try:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )
    print("✅ Kết nối Producer thành công!")
    
    # Test gửi 1 event
    batch = producer.create_batch()
    batch.add(EventData('{"test": "hello"}'))
    producer.send_batch(batch)
    print("✅ Gửi test event thành công!")
    
    producer.close()
    
except Exception as e:
    print(f"❌ Lỗi: {type(e).__name__}")
    print(f"❌ Chi tiết: {e}")