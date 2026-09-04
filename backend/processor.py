from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta, timezone

TOPIC = "sensor-events"
WINDOW_MINUTES = 5

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="streamforge-processor-window-v2",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

# Dictionary of 5-minute windows.
# Each window keeps its own events so older windows can
# still accept late-arriving events.
windows = {}

latest_window_start = None


def parse_event_time(timestamp):
    """Convert ISO timestamp into a UTC datetime."""
    timestamp = timestamp.replace("Z", "+00:00")
    event_time = datetime.fromisoformat(timestamp)

    if event_time.tzinfo is not None:
        event_time = (
            event_time
            .astimezone(timezone.utc)
            .replace(tzinfo=None)
        )

    return event_time


def get_window_start(timestamp):
    """Find the 5-minute window using event time."""
    event_time = parse_event_time(timestamp)

    window_minute = (
        event_time.minute // WINDOW_MINUTES
    ) * WINDOW_MINUTES

    return event_time.replace(
        minute=window_minute,
        second=0,
        microsecond=0
    )


def calculate_average(events):
    """Calculate average Celsius temperature."""
    if not events:
        return 0

    total = sum(
        event["temperature"]
        for event in events
    )

    return total / len(events)


def display_window(window_start, events, label="WINDOW SUMMARY"):
    """Display window statistics."""

    window_end = window_start + timedelta(
        minutes=WINDOW_MINUTES
    )

    average = calculate_average(events)

    print()
    print("========================================")
    print(f"       {label}")
    print("========================================")
    print(f"Window start : {window_start}")
    print(f"Window end   : {window_end}")
    print(f"Events       : {len(events)}")
    print(f"Temperature average: {average:.2f}°C")
    print("========================================")
    print()


print("========================================")
print("   StreamForge Stream Processor")
print("========================================")
print(
    "Pipeline: Consume -> Filter -> Map "
    "-> Window -> Average"
)
print("Filter: temperature > 0")
print("Window: 5 minutes based on event timestamp")
print("Late-arriving event handling: ENABLED")
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
            f"temperature={temperature}°C | "
            f"timestamp={event['timestamp']}"
        )

        # -------------------------
        # FILTER
        # -------------------------

        if temperature <= 0:

            print(
                f"  ✗ FILTERED OUT | "
                f"temperature={temperature}°C"
            )

            continue

        print(
            f"  ✓ PASSED FILTER | "
            f"temperature={temperature}°C"
        )

        # -------------------------
        # MAP
        # -------------------------

        mapped_event = {
            "sensor_id": event["sensor_id"],
            "temperature": temperature,
            "temperature_fahrenheit": round(
                (temperature * 9 / 5) + 32,
                2
            ),
            "timestamp": event["timestamp"]
        }

        print(
            f"  → MAPPED | "
            f"{temperature}°C = "
            f"{mapped_event['temperature_fahrenheit']}°F"
        )

        # -------------------------
        # EVENT-TIME WINDOW
        # -------------------------

        event_window_start = get_window_start(
            mapped_event["timestamp"]
        )

        # Create window if this is the first event
        # we've seen for that 5-minute interval.
        if event_window_start not in windows:
            windows[event_window_start] = []

        # -------------------------
        # LATE EVENT DETECTION
        # -------------------------

        is_late = (
            latest_window_start is not None
            and event_window_start < latest_window_start
        )

        windows[event_window_start].append(
            mapped_event
        )

        if is_late:

            print(
                f"  ⚠ LATE EVENT | "
                f"assigned to original window "
                f"{event_window_start}"
            )

            updated_average = calculate_average(
                windows[event_window_start]
            )

            print(
                f"  → UPDATED WINDOW | "
                f"events="
                f"{len(windows[event_window_start])} | "
                f"average="
                f"{updated_average:.2f}°C"
            )

        else:

            if (
                latest_window_start is None
                or event_window_start > latest_window_start
            ):

                # Show previous window when stream
                # moves into a newer 5-minute window.
                if latest_window_start is not None:

                    display_window(
                        latest_window_start,
                        windows[latest_window_start],
                        "COMPLETED 5-MINUTE WINDOW"
                    )

                latest_window_start = (
                    event_window_start
                )

            current_average = calculate_average(
                windows[event_window_start]
            )

            print(
                f"  → WINDOW | "
                f"{event_window_start} | "
                f"events="
                f"{len(windows[event_window_start])} | "
                f"average="
                f"{current_average:.2f}°C"
            )

except KeyboardInterrupt:

    print("\nProcessor stopped.")

    if latest_window_start is not None:

        display_window(
            latest_window_start,
            windows[latest_window_start],
            "CURRENT 5-MINUTE WINDOW"
        )

finally:

    consumer.close()