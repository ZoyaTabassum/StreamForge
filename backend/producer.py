from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


TOPIC = "sensor-events"


print("StreamForge Kafka Producer started.")
print("Sending sensor events to:", TOPIC)
print("Press Ctrl+C to stop.\n")


try:
    while True:

        event = {
            "sensor_id": f"sensor_{random.randint(1, 5):02d}",
            "temperature": round(random.uniform(-10, 40), 2),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.send(TOPIC, value=event)

        print("Sent:", event)

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()
    producer.close()