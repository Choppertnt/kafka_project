import json
from confluent_kafka import Consumer, KafkaException, KafkaError
import sqlite3
import requests

# ตั้งค่าการเชื่อมต่อ Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # เปลี่ยนเป็นเซิร์ฟเวอร์ Kafka ของคุณ
    'group.id': 'event-consumer-group',
    'auto.offset.reset': 'earliest'
}

# เชื่อมต่อกับ Kafka
consumer = Consumer(kafka_config)
consumer.subscribe(['events'])  # ชื่อ topic ที่คุณต้องการรับข้อมูล

# ฟังก์ชันเพื่อส่งข้อมูลไปยัง Turso หรือฐานข้อมูลที่คุณใช้งาน
def save_event_to_db(event):
    url = "https://cvbwpkwiiorjirhavbmd.supabase.co/rest/v1/events"
    headers = { 
        'apikey': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN2Yndwa3dpaW9yamlyaGF2Ym1kIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzM3NjI1NzAsImV4cCI6MjA0OTMzODU3MH0.kxHYHsB2Zfs6KlmHiQdEQitxg_QhblG9xJgyLkoO0D8',  
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, json=event, headers=headers, timeout=10)
        if response.status_code in [200, 201]:
            print("Event saved successfully!")
        else:
            print(f"Failed to save event: {response.status_code} - {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error saving event to database: {e}")

# เริ่มฟังข้อมูลจาก Kafka
print("Listening for events...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # รับข้อมูลจาก Kafka
        if msg is None:
            # ไม่มีข้อความใหม่
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # ไปที่ตอนจบของ partition
                print(f"End of partition reached: {msg.topic()} [{msg.partition}] @ {msg.offset()}")
            else:
                # เกิดข้อผิดพลาด
                raise KafkaException(msg.error())
        else:
            # รับข้อมูลจาก Kafka
            event = json.loads(msg.value().decode('utf-8'))
            print(f"Received event: {event}")

            # ส่งข้อมูลไปยังฐานข้อมูล (เช่น Turso)
            save_event_to_db(event)

except KeyboardInterrupt:
    # เมื่อหยุดการทำงาน
    print("Consumer stopped.")
finally:
    # ปิดการเชื่อมต่อ Kafka
    consumer.close()
