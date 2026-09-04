from kafka import KafkaConsumer, KafkaProducer
from multiprocessing import Process, Event, Manager
import json
import time
import os

TOPIC = "throughput-test"
GROUP_ID = "streamforge-final-benchmark"
TOTAL_EVENTS = 100_000
NUM_WORKERS = 3

# 100,000 events distributed as evenly as possible:
TARGETS = [33334, 33333, 33333]


def worker(worker_id, ready_event, start_event, results):

    consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print(f"Worker {worker_id} | PID={os.getpid()} | Starting...")

    # Subscribe to the benchmark topic.
    consumer.subscribe([TOPIC])

    # Wait for Kafka to assign a partition.
    while not consumer.assignment():
        consumer.poll(timeout_ms=500)

    partitions = list(consumer.assignment())

    print(
        f"Worker {worker_id} | PID={os.getpid()} | "
        f"Assigned partitions={partitions}"
    )

    ready_event.set()

    # Wait until the controller tells all workers to start.
    start_event.wait()

    target = TARGETS[worker_id]

    processed = 0
    start_time = time.perf_counter()

    while processed < target:

        records = consumer.poll(
            timeout_ms=1000,
            max_records=1000
        )

        for partition, messages in records.items():

            for message in messages:

                if processed >= target:
                    break

                event = message.value
                temperature = event["temperature"]

                # Filter
                if temperature > 0:

                    # Map Celsius → Fahrenheit
                    fahrenheit = (temperature * 9 / 5) + 32
                    _ = round(fahrenheit, 2)

                processed += 1

    elapsed = time.perf_counter() - start_time

    results[worker_id] = {
        "pid": os.getpid(),
        "partitions": [p.partition for p in partitions],
        "processed": processed,
        "elapsed": elapsed
    }

    consumer.close()


def produce_events():

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

    print()
    print("Producing 100,000 benchmark events...")

    for i in range(TOTAL_EVENTS):

        producer.send(
            TOPIC,
            value=event,
            partition=i % NUM_WORKERS
        )

    producer.flush()
    producer.close()

    print("100,000 events produced.")
    print()


if __name__ == "__main__":

    print("========================================")
    print(" StreamForge FINAL Throughput Benchmark")
    print("========================================")
    print(f"Topic        : {TOPIC}")
    print(f"Total events : {TOTAL_EVENTS}")
    print(f"Workers      : {NUM_WORKERS}")
    print("Partitions   : 3")
    print()

    manager = Manager()
    results = manager.dict()

    start_event = Event()

    ready_events = [
        Event() for _ in range(NUM_WORKERS)
    ]

    workers = []

    # Start all three Python workers.
    for worker_id in range(NUM_WORKERS):

        process = Process(
            target=worker,
            args=(
                worker_id,
                ready_events[worker_id],
                start_event,
                results
            )
        )

        process.start()
        workers.append(process)

    print("Waiting for all workers to receive partitions...")

    for event in ready_events:
        event.wait()

    print()
    print("All 3 workers are ready.")
    print()

    # Now produce the benchmark data.
    produce_events()

    # Start timing and release workers.
    overall_start = time.perf_counter()
    start_event.set()

    # Wait for all workers to finish.
    for process in workers:
        process.join()

    overall_elapsed = time.perf_counter() - overall_start

    print()
    print("========================================")
    print("        WORKER RESULTS")
    print("========================================")

    total_processed = 0

    for worker_id in range(NUM_WORKERS):

        result = results.get(worker_id)

        if result:

            print(f"Worker {worker_id + 1}")
            print(f"  PID        : {result['pid']}")
            print(f"  Partitions : {result['partitions']}")
            print(f"  Processed  : {result['processed']}")
            print(f"  Time       : {result['elapsed']:.4f} sec")
            print(
                f"  Rate       : "
                f"{result['processed'] / result['elapsed']:,.2f} events/sec"
            )
            print()

            total_processed += result["processed"]

    print("========================================")
    print("       FINAL AGGREGATE RESULTS")
    print("========================================")
    print(f"Total processed : {total_processed}")
    print(f"Overall time    : {overall_elapsed:.4f} sec")
    print(
        f"Aggregate rate  : "
        f"{total_processed / overall_elapsed:,.2f} events/sec"
    )
    print("========================================")