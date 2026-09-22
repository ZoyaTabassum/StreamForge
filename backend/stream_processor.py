import json
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win

from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.dataflow import Dataflow


BROKERS = ["localhost:9092"]

INPUT_TOPIC = "truck-telemetry"
OUTPUT_TOPIC = "processed-telemetry"


# --------------------------------------------------
# 1. Create Bytewax dataflow
# --------------------------------------------------

flow = Dataflow("streamforge-processor")


# --------------------------------------------------
# 2. Read from Kafka
# --------------------------------------------------

kafka_input = kop.input(
    "kafka-input",
    flow,
    brokers=BROKERS,
    topics=[INPUT_TOPIC],
)


# --------------------------------------------------
# 3. Parse Kafka JSON messages
# --------------------------------------------------

def parse_message(msg):
    try:
        data = json.loads(msg.value.decode("utf-8"))

        return data

    except Exception as e:
        print(f"Invalid message: {e}")
        return None


parsed = op.map(
    "parse-json",
    kafka_input.oks,
    parse_message,
)


# --------------------------------------------------
# 4. Remove invalid messages
# --------------------------------------------------

valid = op.filter(
    "valid-messages",
    parsed,
    lambda x: (
        x is not None
        and "truck_id" in x
        and "temperature" in x
        and "timestamp" in x
    ),
)


# --------------------------------------------------
# 5. Convert timestamp
# --------------------------------------------------

def add_datetime(data):
    data["event_time"] = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    return data


with_time = op.map(
    "add-event-time",
    valid,
    add_datetime,
)


# --------------------------------------------------
# 6. Group by truck_id
# --------------------------------------------------

keyed = op.key_on(
    "truck-id",
    with_time,
    lambda x: x["truck_id"],
)


# --------------------------------------------------
# 7. Five-minute event-time window
# --------------------------------------------------

clock = win.EventClock(
    lambda x: x["event_time"],
    wait_for_system_duration=timedelta(seconds=10),
)


windower = win.TumblingWindower(
    length=timedelta(minutes=5),
    align_to=datetime(2026, 1, 1, tzinfo=timezone.utc),
)


windowed = win.collect_window(
    "five-minute-window",
    keyed,
    clock,
    windower,
)


# --------------------------------------------------
# 8. Calculate average temperature
# --------------------------------------------------

def calculate_average(item):

    truck_id, (window_id, readings) = item

    temperatures = [
        reading["temperature"]
        for reading in readings
    ]

    if not temperatures:
        return None

    average_temperature = sum(temperatures) / len(temperatures)

    return {
        "truck_id": truck_id,
        "average_temperature": round(
            average_temperature,
            2,
        ),
        "message_count": len(temperatures),
        "window_id": window_id,
        "processed": True,
    }


averages = op.map(
    "calculate-average",
    windowed.down,
    calculate_average,
)


# --------------------------------------------------
# 9. Remove empty results
# --------------------------------------------------

results = op.filter(
    "valid-results",
    averages,
    lambda x: x is not None,
)


# --------------------------------------------------
# 10. Convert to Kafka messages
# --------------------------------------------------

def create_kafka_message(data):

    return KafkaSinkMessage(
        key=data["truck_id"],
        value=json.dumps(data),
    )


output_messages = op.map(
    "create-output-message",
    results,
    create_kafka_message,
)


# --------------------------------------------------
# 11. Write to Kafka
# --------------------------------------------------

kop.output(
    "kafka-output",
    output_messages,
    brokers=BROKERS,
    topic=OUTPUT_TOPIC,
)