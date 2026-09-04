from kafka import KafkaProducer
import json
import time


TOPIC = "sensor-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


events = [
    {
        "sensor_id": "test_sensor",
        "temperature": 10.0,
        "timestamp": "2026-09-04T07:00:10+00:00"
    },
    {
        "sensor_id": "test_sensor",
        "temperature": 20.0,
        "timestamp": "2026-09-04T07:01:10+00:00"
    },
    {
        "sensor_id": "test_sensor",
        "temperature": 30.0,
        "timestamp": "2026-09-04T07:05:10+00:00"
    },
    {
        "sensor_id": "test_sensor",
        "temperature": 40.0,
        "timestamp": "2026-09-04T07:02:00+00:00"
    }
]


print("========================================")
print("   StreamForge Late Event Test")
print("========================================")
print()


for event in events:

    producer.send(
        TOPIC,
        value=event,
        partition=0
    )

    producer.flush()

    print("Sent test event:", event)

    time.sleep(1)


producer.close()

print()
print("Late event test completed.")