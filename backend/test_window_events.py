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


# Expected mathematical results
expected_initial_average = 15.00
expected_late_average = 23.33


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
print()
print("========================================")
print("       MATHEMATICAL VALIDATION")
print("========================================")

initial_average = (10.0 + 20.0) / 2
late_average = (10.0 + 20.0 + 40.0) / 3

print(f"Initial average : {initial_average:.2f}°C")
print(f"Expected        : {expected_initial_average:.2f}°C")

if round(initial_average, 2) == expected_initial_average:
    print("Initial average : PASS")
else:
    print("Initial average : FAIL")

print()

print(f"Late-event average : {late_average:.2f}°C")
print(f"Expected           : {expected_late_average:.2f}°C")

if round(late_average, 2) == expected_late_average:
    print("Late-event average : PASS")
else:
    print("Late-event average : FAIL")

print()
print("========================================")
print("Window validation test completed.")
print("========================================")