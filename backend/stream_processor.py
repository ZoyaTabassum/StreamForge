"""
StreamForge stream processor.

Pipeline:

Kafka truck-telemetry
        |
        v
    Bytewax
        |
        +--> JSON parsing
        |
        +--> Validation
        |
        +--> Event-time processing
        |
        +--> 5-minute tumbling window
        |
        +--> Average temperature
        |
        +--> RocksDB persistent state
        |
        +--> Kafka state changelog
        |
        +--> processed-telemetry

Late events are routed to late-telemetry.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win

from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow

from confluent_kafka import Producer

from state_store import StateStore


# ==================================================
# 0. ENVIRONMENT / CONFIGURATION
# ==================================================

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


BROKERS = [os.getenv("KAFKA_BROKERS", "localhost:9092")]

INPUT_TOPIC = os.getenv(
    "INPUT_TOPIC",
    "truck-telemetry",
)

OUTPUT_TOPIC = os.getenv(
    "OUTPUT_TOPIC",
    "processed-telemetry",
)

LATE_TOPIC = os.getenv(
    "LATE_TOPIC",
    "late-telemetry",
)

CHANGELOG_TOPIC = os.getenv(
    "CHANGELOG_TOPIC",
    "streamforge-state-changelog",
)

WINDOW_LENGTH_MINUTES = float(
    os.getenv(
        "WINDOW_LENGTH_MINUTES",
        "5",
    )
)

LATE_TOLERANCE_SECONDS = float(
    os.getenv(
        "LATE_TOLERANCE_SECONDS",
        "30",
    )
)

MIN_PLAUSIBLE_TEMP_C = float(
    os.getenv(
        "MIN_PLAUSIBLE_TEMP_C",
        "-40",
    )
)

MAX_PLAUSIBLE_TEMP_C = float(
    os.getenv(
        "MAX_PLAUSIBLE_TEMP_C",
        "80",
    )
)


# ==================================================
# 1. PERSISTENT STATE
# ==================================================

state_store = StateStore()


# ==================================================
# 2. KAFKA CHANGELOG PRODUCER
# ==================================================

changelog_producer = Producer(
    {
        "bootstrap.servers": ",".join(BROKERS),
    }
)


def publish_state_changelog(state):
    """
    Publish the latest completed aggregation state
    to the Kafka changelog topic.

    RocksDB:
        Fast local state storage.

    Kafka changelog:
        Durable recovery backup.
    """

    key = str(state["truck_id"])

    value = json.dumps(
        {
            "truck_id": state["truck_id"],
            "total_temperature": state["total_temperature"],
            "message_count": state["message_count"],
            "average_temperature": state["average_temperature"],
            "window_id": state["window_id"],
        }
    )

    try:
        changelog_producer.produce(
            topic=CHANGELOG_TOPIC,
            key=key,
            value=value,
        )

        changelog_producer.poll(0)

        print(
            f"[CHANGELOG] Published {key} "
            f"window={state['window_id']}"
        )

    except Exception as exc:
        print(
            f"[CHANGELOG ERROR] {key}: {exc}"
        )


# ==================================================
# 3. BYTEWAX DATAFLOW
# ==================================================

flow = Dataflow(
    "streamforge-processor"
)


# ==================================================
# 4. KAFKA INPUT
# ==================================================

kafka_input = kop.input(
    "kafka-input",
    flow,
    brokers=BROKERS,
    topics=[INPUT_TOPIC],
)


# ==================================================
# 5. PARSE JSON
# ==================================================

def parse_message(msg):
    try:
        return json.loads(
            msg.value.decode("utf-8")
        )

    except Exception as exc:
        print(
            f"[INVALID JSON] {exc}"
        )

        return None


parsed = op.map(
    "parse-json",
    kafka_input.oks,
    parse_message,
)


# ==================================================
# 6. VALIDATION
# ==================================================

def has_required_fields(data):
    return (
        data is not None
        and "truck_id" in data
        and "temperature" in data
        and "timestamp" in data
    )


def is_plausible_temperature(data):
    try:
        temperature = float(
            data.get("temperature")
        )

    except (TypeError, ValueError):
        return False

    return (
        MIN_PLAUSIBLE_TEMP_C
        <= temperature
        <= MAX_PLAUSIBLE_TEMP_C
    )


valid = op.filter(
    "valid-messages",
    parsed,
    lambda data:
        has_required_fields(data)
        and is_plausible_temperature(data),
)


# ==================================================
# 7. EVENT TIME
# ==================================================

def add_datetime(data):
    data["event_time"] = datetime.fromisoformat(
        data["timestamp"].replace(
            "Z",
            "+00:00",
        )
    )

    return data


with_time = op.map(
    "add-event-time",
    valid,
    add_datetime,
)


# ==================================================
# 8. KEY BY TRUCK
# ==================================================

keyed = op.key_on(
    "truck-id",
    with_time,
    lambda data: data["truck_id"],
)


# ==================================================
# 9. EVENT-TIME WINDOWING
# ==================================================

clock = win.EventClock(
    lambda data: data["event_time"],
    wait_for_system_duration=timedelta(
        seconds=LATE_TOLERANCE_SECONDS
    ),
)


windower = win.TumblingWindower(
    length=timedelta(
        minutes=WINDOW_LENGTH_MINUTES
    ),
    align_to=datetime(
        2026,
        1,
        1,
        tzinfo=timezone.utc,
    ),
)


windowed = win.collect_window(
    "five-minute-window",
    keyed,
    clock,
    windower,
)


# ==================================================
# 10. CALCULATE AVERAGE
# ==================================================

def calculate_average(item):
    truck_id, (window_id, readings) = item

    temperatures = [
        float(reading["temperature"])
        for reading in readings
    ]

    if not temperatures:
        return None

    total_temperature = sum(
        temperatures
    )

    message_count = len(
        temperatures
    )

    average_temperature = (
        total_temperature
        / message_count
    )

    return {
        "truck_id": truck_id,
        "total_temperature": round(
            total_temperature,
            2,
        ),
        "average_temperature": round(
            average_temperature,
            2,
        ),
        "message_count": message_count,
        "window_id": window_id,
        "processed": True,
    }


averages = op.map(
    "calculate-average",
    windowed.down,
    calculate_average,
)


results = op.filter(
    "valid-results",
    averages,
    lambda data:
        data is not None,
)


# ==================================================
# 11. PRINT RESULTS
# ==================================================

def print_result(item):
    print(
        f"[RESULT] "
        f"{item['truck_id']} "
        f"avg={item['average_temperature']}°C "
        f"count={item['message_count']} "
        f"window={item['window_id']}"
    )

    return item


results = op.map(
    "print-result",
    results,
    print_result,
)


# ==================================================
# 12. ROCKSDB + KAFKA CHANGELOG
# ==================================================

def persist_state(item):
    """
    Save the latest completed aggregation
    to both RocksDB and Kafka changelog.
    """

    truck_id = item["truck_id"]

    state = {
        "truck_id": truck_id,
        "total_temperature": item[
            "total_temperature"
        ],
        "message_count": item[
            "message_count"
        ],
        "average_temperature": item[
            "average_temperature"
        ],
        "window_id": item[
            "window_id"
        ],
    }

    # ----------------------------------------------
    # RocksDB
    # ----------------------------------------------

    state_store.put(
        truck_id,
        state,
    )

    state_store.flush()

    print(
        f"[ROCKSDB] Saved "
        f"{truck_id}: "
        f"avg={state['average_temperature']}°C "
        f"count={state['message_count']} "
        f"window={state['window_id']}"
    )

    # ----------------------------------------------
    # Kafka changelog
    # ----------------------------------------------

    publish_state_changelog(
        state
    )

    return item


results = op.map(
    "persist-state",
    results,
    persist_state,
)


# ==================================================
# 13. LATE EVENTS
# ==================================================

def format_late_event(item):
    """
    Format late events for the
    late-telemetry Kafka topic.
    """

    try:
        key, value = item

    except (TypeError, ValueError):
        key = None
        value = item

    truck_id = None
    event_time = None

    if isinstance(value, dict):

        truck_id = value.get(
            "truck_id",
            key,
        )

        event_time_obj = value.get(
            "event_time"
        )

        if hasattr(
            event_time_obj,
            "isoformat",
        ):
            event_time = (
                event_time_obj.isoformat()
            )

        else:
            event_time = str(
                event_time_obj
            )

    else:
        truck_id = key

    return {
        "truck_id": truck_id,
        "event_time": event_time,
        "reason": (
            "arrived_after_window_closed"
        ),
        "raw": str(value),
    }


late_formatted = op.map(
    "format-late-event",
    windowed.late,
    format_late_event,
)


def log_late_event(item):
    print(
        f"[LATE] "
        f"Truck {item['truck_id']} "
        f"event at "
        f"{item['event_time']} "
        f"missed its window"
    )

    return item


late_logged = op.map(
    "log-late-event",
    late_formatted,
    log_late_event,
)


def create_late_kafka_message(data):
    return KafkaSinkMessage(
        key=str(
            data["truck_id"]
        ),
        value=json.dumps(data),
    )


late_messages = op.map(
    "create-late-message",
    late_logged,
    create_late_kafka_message,
)


kop.output(
    "kafka-late-output",
    late_messages,
    brokers=BROKERS,
    topic=LATE_TOPIC,
)


# ==================================================
# 14. PROCESSED TELEMETRY OUTPUT
# ==================================================

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


kop.output(
    "kafka-output",
    output_messages,
    brokers=BROKERS,
    topic=OUTPUT_TOPIC,
)


# ==================================================
# 15. CLEANUP
# ==================================================

def cleanup():
    """
    Flush pending Kafka and RocksDB writes
    when the processor shuts down.
    """

    try:
        changelog_producer.flush(
            timeout=10
        )

    except Exception as exc:
        print(
            f"[CLEANUP] Kafka error: {exc}"
        )

    try:
        state_store.flush()

        state_store.close()

    except Exception as exc:
        print(
            f"[CLEANUP] RocksDB error: {exc}"
        )


import atexit

atexit.register(
    cleanup
)