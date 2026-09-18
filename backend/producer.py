import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


# ============================================
# StreamForge Kafka Configuration
# ============================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "truck-telemetry"


# ============================================
# Kafka Producer
# ============================================

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


# ============================================
# Delivery Callback
# ============================================

def delivery_report(err, message):

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to "
            f"{message.topic()} "
            f"[partition {message.partition()}] "
            f"offset {message.offset()}"
        )


# ============================================
# Generate Truck Telemetry
# ============================================

def generate_telemetry():

    return {
        "truck_id": f"TRUCK-{random.randint(1, 20):03d}",
        "temperature": round(
            random.uniform(20.0, 45.0),
            2
        ),
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }


# ============================================
# Main Producer
# ============================================

def main():

    print("======================================")
    print(" StreamForge Kafka Producer")
    print("======================================")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Press CTRL+C to stop\n")

    try:

        while True:

            telemetry = generate_telemetry()

            producer.produce(
                topic=KAFKA_TOPIC,
                key=telemetry["truck_id"],
                value=json.dumps(telemetry),
                callback=delivery_report
            )

            # Process delivery callbacks
            producer.poll(0)

            print(
                f"Produced: "
                f"{telemetry['truck_id']} | "
                f"Temperature: "
                f"{telemetry['temperature']}°C"
            )

            time.sleep(1)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.flush()
        print("Producer stopped.")


if __name__ == "__main__":
    main()