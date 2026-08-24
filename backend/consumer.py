from kafka import KafkaConsumer
import json

TOPIC = "sensor-events"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="streamforge-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("StreamForge Kafka Consumer started.")
print(f"Listening to topic: {TOPIC}")
print("Waiting for events...\n")

try:
    for message in consumer:
        event = message.value

        print(
            f"Received | "
            f"partition={message.partition} | "
            f"offset={message.offset} | "
            f"sensor={event['sensor_id']} | "
            f"temperature={event['temperature']}°C | "
            f"timestamp={event['timestamp']}"
        )

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()