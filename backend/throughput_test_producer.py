from kafka import KafkaProducer
import json
import time

TOPIC = "throughput-test"
TOTAL_EVENTS = 100_000

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    linger_ms=5,
    batch_size=64 * 1024
)

event = {
    "sensor_id": "benchmark_sensor",
    "temperature": 25.0,
    "timestamp": "2026-09-04T07:00:00+00:00"
}

print("========================================")
print("      Throughput Test Producer")
print("========================================")
print(f"Total events : {TOTAL_EVENTS}")
print("Partitions   : 0, 1, 2")
print()

start = time.perf_counter()

for i in range(TOTAL_EVENTS):
    producer.send(
        TOPIC,
        value=event,
        partition=i % 3
    )

producer.flush()

elapsed = time.perf_counter() - start

print("========================================")
print("        PRODUCER RESULTS")
print("========================================")
print(f"Events sent : {TOTAL_EVENTS}")
print(f"Time taken  : {elapsed:.4f} seconds")
print(f"Send rate   : {TOTAL_EVENTS / elapsed:,.2f} events/sec")
print("Partitions  : 0, 1, 2")
print("========================================")

producer.close()