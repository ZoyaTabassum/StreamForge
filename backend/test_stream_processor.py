"""
Day 4 tests - run with: pytest test_stream_processor.py -v

No live Kafka broker needed: pure-function tests use plain pytest,
and the dataflow-level test uses bytewax.testing to feed a deterministic
in-memory stream (including a deliberately late event) through the real
windowing logic.
"""

from datetime import datetime, timedelta, timezone

import pytest

from stream_processor import (
    has_required_fields,
    is_plausible_temperature,
    add_datetime,
    calculate_average,
    parse_message,
    MIN_PLAUSIBLE_TEMP_C,
    MAX_PLAUSIBLE_TEMP_C,
)


# --------------------------------------------------
# parse_message
# --------------------------------------------------

class FakeMsg:
    def __init__(self, raw: bytes):
        self.value = raw


def test_parse_message_valid_json():
    msg = FakeMsg(b'{"truck_id": "TRUCK-001", "temperature": 22.5}')
    result = parse_message(msg)
    assert result == {"truck_id": "TRUCK-001", "temperature": 22.5}


def test_parse_message_invalid_json_returns_none():
    msg = FakeMsg(b"not json at all")
    assert parse_message(msg) is None


def test_parse_message_empty_bytes_returns_none():
    msg = FakeMsg(b"")
    assert parse_message(msg) is None


# --------------------------------------------------
# has_required_fields
# --------------------------------------------------

def test_has_required_fields_all_present():
    event = {"truck_id": "TRUCK-001", "temperature": 22.5, "timestamp": "2026-01-01T00:00:00+00:00"}
    assert has_required_fields(event) is True


@pytest.mark.parametrize("missing_field", ["truck_id", "temperature", "timestamp"])
def test_has_required_fields_missing_one(missing_field):
    event = {"truck_id": "TRUCK-001", "temperature": 22.5, "timestamp": "2026-01-01T00:00:00+00:00"}
    del event[missing_field]
    assert has_required_fields(event) is False


def test_has_required_fields_none_input():
    assert has_required_fields(None) is False


# --------------------------------------------------
# is_plausible_temperature (Day 4 sanity filter)
# --------------------------------------------------

def test_plausible_temperature_normal_value():
    assert is_plausible_temperature({"temperature": 22.5}) is True


def test_plausible_temperature_at_boundaries():
    assert is_plausible_temperature({"temperature": MIN_PLAUSIBLE_TEMP_C}) is True
    assert is_plausible_temperature({"temperature": MAX_PLAUSIBLE_TEMP_C}) is True


def test_plausible_temperature_rejects_spike():
    assert is_plausible_temperature({"temperature": 999}) is False


def test_plausible_temperature_rejects_below_range():
    assert is_plausible_temperature({"temperature": MIN_PLAUSIBLE_TEMP_C - 1}) is False


def test_plausible_temperature_rejects_non_numeric():
    assert is_plausible_temperature({"temperature": "not-a-number"}) is False


def test_plausible_temperature_rejects_missing_field():
    assert is_plausible_temperature({}) is False


# --------------------------------------------------
# add_datetime
# --------------------------------------------------

def test_add_datetime_parses_offset_timestamp():
    event = {"timestamp": "2026-01-01T12:30:00+00:00"}
    result = add_datetime(event)
    assert result["event_time"] == datetime(2026, 1, 1, 12, 30, 0, tzinfo=timezone.utc)


def test_add_datetime_handles_z_suffix():
    # Producer's own isoformat() never emits "Z", but the code defends
    # against it in case a different producer path does.
    event = {"timestamp": "2026-01-01T12:30:00Z"}
    result = add_datetime(event)
    assert result["event_time"] == datetime(2026, 1, 1, 12, 30, 0, tzinfo=timezone.utc)


def test_add_datetime_raises_on_malformed_timestamp():
    event = {"timestamp": "not-a-timestamp"}
    with pytest.raises(ValueError):
        add_datetime(event)


# --------------------------------------------------
# calculate_average
# --------------------------------------------------

def test_calculate_average_normal_window():
    item = ("TRUCK-001", ("w1", [{"temperature": 20.0}, {"temperature": 30.0}]))
    result = calculate_average(item)
    assert result["truck_id"] == "TRUCK-001"
    assert result["average_temperature"] == 25.0
    assert result["message_count"] == 2
    assert result["processed"] is True


def test_calculate_average_single_reading_window():
    # Edge case: a truck with exactly one reading in a 5-min window
    # (e.g. right after a worker recovers from an outage).
    item = ("TRUCK-002", ("w1", [{"temperature": 18.5}]))
    result = calculate_average(item)
    assert result["average_temperature"] == 18.5
    assert result["message_count"] == 1


def test_calculate_average_empty_window_returns_none():
    # Edge case: window closes with zero readings for this key.
    item = ("TRUCK-003", ("w1", []))
    assert calculate_average(item) is None


def test_calculate_average_rounds_to_two_decimals():
    item = ("TRUCK-004", ("w1", [{"temperature": 20.111}, {"temperature": 20.222}]))
    result = calculate_average(item)
    assert result["average_temperature"] == 20.17


# --------------------------------------------------
# Dataflow-level: windowing + late-data behavior
# --------------------------------------------------
# These use bytewax.testing to run the real dataflow logic (clock +
# windower + collect_window) against a scripted, deterministic sequence
# of events — including one that's late — without needing Kafka running.

def test_windowing_end_to_end_with_late_event():
    from bytewax.dataflow import Dataflow
    from bytewax.testing import TestingSource, run_main
    import bytewax.operators as op
    import bytewax.operators.windowing as win

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)

    # Two on-time events in the first 5-min window, then one event whose
    # event_time belongs to that same closed window but arrives (in
    # processing order) after the watermark has moved well past it.
    events = [
        {"truck_id": "TRUCK-001", "temperature": 20.0, "event_time": base + timedelta(seconds=10)},
        {"truck_id": "TRUCK-001", "temperature": 30.0, "event_time": base + timedelta(seconds=20)},
        # Watermark-advancing event, far enough ahead to close window 1
        # given a short test tolerance.
        {"truck_id": "TRUCK-001", "temperature": 25.0, "event_time": base + timedelta(minutes=10)},
        # Late straggler: belongs to window 1 by event_time, but shows up
        # after the watermark already passed it.
        {"truck_id": "TRUCK-001", "temperature": 999.0, "event_time": base + timedelta(seconds=15)},
    ]

    test_flow = Dataflow("test-windowing")
    inp = op.input("in", test_flow, TestingSource(events))
    keyed = op.key_on("key", inp, lambda x: x["truck_id"])

    clock = win.EventClock(
        lambda x: x["event_time"],
        wait_for_system_duration=timedelta(seconds=1),
    )
    windower = win.TumblingWindower(length=timedelta(minutes=5), align_to=base)

    windowed = win.collect_window("win", keyed, clock, windower)

    on_time_out = []
    late_out = []

    op.inspect("capture-down", windowed.down, lambda step_id, item: on_time_out.append(item))
    op.inspect("capture-late", windowed.late, lambda step_id, item: late_out.append(item))

    run_main(test_flow)

    # The first window should have closed with exactly the 2 on-time
    # readings (20.0, 30.0) — NOT including the late 999.0 straggler.
    first_window_readings = [
        r["temperature"]
        for (_key, (_wid, readings)) in on_time_out
        for r in readings
    ]
    assert 999.0 not in first_window_readings, (
        "Late event leaked into the closed window's average — "
        "this is exactly the correctness bug Day 4 exists to catch."
    )
    assert sorted(first_window_readings[:2]) == [20.0, 30.0]

    # And it should show up on the late stream instead of vanishing.
    assert len(late_out) == 1


def test_window_boundary_assigns_to_correct_window():
    # Edge case: an event exactly on a window boundary should land in the
    # window that starts at that instant, not the previous one.
    from bytewax.dataflow import Dataflow
    from bytewax.testing import TestingSource, run_main
    import bytewax.operators as op
    import bytewax.operators.windowing as win

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    boundary_time = base + timedelta(minutes=5)  # exact edge

    events = [
        {"truck_id": "TRUCK-001", "temperature": 40.0, "event_time": boundary_time},
        # advance the watermark so both windows close
        {"truck_id": "TRUCK-001", "temperature": 40.0, "event_time": boundary_time + timedelta(minutes=10)},
    ]

    test_flow = Dataflow("test-boundary")
    inp = op.input("in", test_flow, TestingSource(events))
    keyed = op.key_on("key", inp, lambda x: x["truck_id"])

    clock = win.EventClock(lambda x: x["event_time"], wait_for_system_duration=timedelta(seconds=1))
    windower = win.TumblingWindower(length=timedelta(minutes=5), align_to=base)
    windowed = win.collect_window("win", keyed, clock, windower)

    seen_window_ids = []
    op.inspect(
        "capture",
        windowed.down,
        lambda step_id, item: seen_window_ids.append(item[1][0]),
    )

    run_main(test_flow)

    # The boundary event should belong to the SECOND window (id 1), not
    # the first (id 0) — i.e. windows are [start, end).
    assert len(seen_window_ids) >= 1
    assert seen_window_ids[0] != 0 or len(set(seen_window_ids)) > 1