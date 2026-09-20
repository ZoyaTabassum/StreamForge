from confluent_kafka import Consumer, KafkaException


BROKERS = "localhost:9092"
TOPIC = "truck-telemetry"
GROUP_ID = "streamforge-consumer"


def main():
    print("=" * 50)
    print("StreamForge Kafka Consumer")
    print("=" * 50)
    print(f"Kafka: {BROKERS}")
    print(f"Topic: {TOPIC}")
    print("Waiting for messages...")
    print("Press CTRL+C to stop")
    print()

    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })

    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            value = msg.value().decode("utf-8")

            print(
                f"Received: {value} "
                f"| partition={msg.partition()} "
                f"| offset={msg.offset()}"
            )

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("Consumer stopped.")


if __name__ == "__main__":
    main()