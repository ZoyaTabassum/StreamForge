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
print("Pipeline: Consume -> Filter -> Map")
print("Condition: temperature > 0")
print("Map: Celsius -> Fahrenheit")
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

        # Filter stage
        if temperature > 0:

            print(
                f"  ✓ PASSED FILTER | "
                f"temperature={temperature}°C"
            )

            # Map stage
            mapped_event = {
                "sensor_id": event["sensor_id"],
                "temperature": temperature,
                "temperature_fahrenheit": round(
                    (temperature * 9 / 5) + 32, 2
                ),
                "timestamp": event["timestamp"]
            }

            print(
                f"  → MAPPED | "
                f"{temperature}°C = "
                f"{mapped_event['temperature_fahrenheit']}°F"
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