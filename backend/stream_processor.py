"""
StreamForge stream processor - Day 2/3 topology + Day 4 correctness work.

Day 4 additions over the original dataflow:
  1. Late-arriving events are no longer silently dropped — bytewax's
     windowing operators emit a `.late` stream alongside `.down` for
     exactly this reason. We route late events to a `late-telemetry`
     Kafka topic (a lightweight dead-letter) instead of losing them,
     and log a count so lateness is actually visible during a chaos test.
  2. Lateness tolerance and window length are now configurable via .env
     instead of hardcoded — 10s was very tight for a 5-minute window.
  3. A defensive sanity-range filter (Filter (Temp > 0), per the project
     plan) rejects physically implausible readings before they can skew
     an average, even though the current producer doesn't emit any.

NOTE ON BYTEWAX VERSIONS: this assumes `windowing.collect_window` returns
an object exposing both `.down` (on-time window results) and `.late`
(events that arrived after their window's watermark closed), i.e.
`WindowOut(down, late)`. If your installed bytewax version names or
shapes this differently, run `python -c "import bytewax.operators.windowing as w; help(w.collect_window)"`
and adjust the two lines marked below.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win

from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.dataflow import Dataflow

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# --------------------------------------------------
# 0. Config (Day 4 - was hardcoded)
# --------------------------------------------------

BROKERS = [os.getenv("KAFKA_BROKERS", "localhost:9092")]

INPUT_TOPIC = os.getenv("INPUT_TOPIC", "truck-telemetry")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "processed-telemetry")
LATE_TOPIC = os.getenv("LATE_TOPIC", "late-telemetry")

WINDOW_LENGTH_MINUTES = float(os.getenv("WINDOW_LENGTH_MINUTES", "5"))
# How long the clock waits for stragglers before closing a window. 10s was
# the original value — too tight for a 5-min window under any real network
# jitter. Widen this if you see a lot of traffic on late-telemetry.
LATE_TOLERANCE_SECONDS = float(os.getenv("LATE_TOLERANCE_SECONDS", "30"))

# Physically implausible readings get rejected regardless of what the
# producer currently emits — defense in depth, matches the project plan's
# "Filter (Temp > 0)" step.
MIN_PLAUSIBLE_TEMP_C = float(os.getenv("MIN_PLAUSIBLE_TEMP_C", "-40"))
MAX_PLAUSIBLE_TEMP_C = float(os.getenv("MAX_PLAUSIBLE_TEMP_C", "80"))


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
        return json.loads(msg.value.decode("utf-8"))
    except Exception as e:
        print(f"Invalid message: {e}")
        return None


parsed = op.map(
    "parse-json",
    kafka_input.oks,
    parse_message,
)


# --------------------------------------------------
# 4. Remove invalid / implausible messages
# --------------------------------------------------

def has_required_fields(x):
    return (
        x is not None
        and "truck_id" in x
        and "temperature" in x
        and "timestamp" in x
    )


def is_plausible_temperature(x):
    # Day 4 - defensive range check (Filter (Temp > 0) from the plan,
    # widened to a real sanity range rather than a literal > 0, since
    # sub-zero readings are legitimate for a refrigerated truck).
    try:
        temp = float(x.get("temperature"))
    except (TypeError, ValueError):
        return False
    return MIN_PLAUSIBLE_TEMP_C <= temp <= MAX_PLAUSIBLE_TEMP_C


valid = op.filter(
    "valid-messages",
    parsed,
    lambda x: has_required_fields(x) and is_plausible_temperature(x),
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
# 7. Windowed aggregation (Day 4 - configurable + late output captured)
# --------------------------------------------------

clock = win.EventClock(
    lambda x: x["event_time"],
    wait_for_system_duration=timedelta(seconds=LATE_TOLERANCE_SECONDS),
)

windower = win.TumblingWindower(
    length=timedelta(minutes=WINDOW_LENGTH_MINUTES),
    align_to=datetime(2026, 1, 1, tzinfo=timezone.utc),
)

windowed = win.collect_window(
    "five-minute-window",
    keyed,
    clock,
    windower,
)


# --------------------------------------------------
# 8. Calculate average temperature (on-time results)
# --------------------------------------------------

def calculate_average(item):
    truck_id, (window_id, readings) = item

    temperatures = [reading["temperature"] for reading in readings]
    if not temperatures:
        return None

    average_temperature = sum(temperatures) / len(temperatures)

    return {
        "truck_id": truck_id,
        "average_temperature": round(average_temperature, 2),
        "message_count": len(temperatures),
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
    lambda x: x is not None,
)


def print_result(item):
    print(f"[RESULT] {item['truck_id']} avg={item['average_temperature']}°C "
          f"count={item['message_count']} window={item['window_id']}")
    return item


results = op.map(
    "print-result",
    results,
    print_result,
)


# --------------------------------------------------
# 9. Day 4 - handle late-arriving events instead of dropping them
# --------------------------------------------------

def format_late_event(item):
    """
    Shape of `windowed.late` items can vary slightly by bytewax version —
    this handles the common (key, value) shape defensively and always
    produces something loggable/routable rather than raising.
    """
    try:
        key, value = item
    except (TypeError, ValueError):
        key, value = None, item

    truck_id = None
    event_time = None
    if isinstance(value, dict):
        truck_id = value.get("truck_id", key)
        event_time_obj = value.get("event_time")
        event_time = event_time_obj.isoformat() if hasattr(event_time_obj, "isoformat") else str(event_time_obj)
    else:
        truck_id = key

    return {
        "truck_id": truck_id,
        "event_time": event_time,
        "reason": "arrived_after_window_closed",
        "raw": str(value),
    }


late_formatted = op.map(
    "format-late-event",
    windowed.late,  # <-- adjust this line if your bytewax version names it differently
    format_late_event,
)


def log_late_event(item):
    print(f"[LATE] Truck {item['truck_id']} event at {item['event_time']} missed its window")
    return item


late_logged = op.map(
    "log-late-event",
    late_formatted,
    log_late_event,
)


def create_late_kafka_message(data):
    return KafkaSinkMessage(
        key=str(data["truck_id"]),
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


# --------------------------------------------------
# 10. Write on-time results to Kafka
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

kop.output(
    "kafka-output",
    output_messages,
    brokers=BROKERS,
    topic=OUTPUT_TOPIC,
)