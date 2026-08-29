from kafka import KafkaConsumer
import json

TOPIC = "sensor-events"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="streamforge-processor",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("========================================")
print("   StreamForge Stream Processor")
print("========================================")
print("Pipeline: Consume -> Filter")
print("Condition: temperature > 0")
print("Listening to:", TOPIC)
print()

try:
    for message in consumer:

        event = message.value
        temperature = event["temperature"]

        print(
            f"Consumed | "
            f"partition={message.partition} | "
            f"sensor={event['sensor_id']} | "
            f"temperature={temperature}°C"
        )

        if temperature > 0:
            print(
                f"  ✓ PASSED FILTER | "
                f"temperature={temperature}°C"
            )
        else:
            print(
                f"  ✗ FILTERED OUT | "
                f"temperature={temperature}°C"
            )

except KeyboardInterrupt:
    print("\nProcessor stopped.")

finally:
    consumer.close()