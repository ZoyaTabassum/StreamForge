import json

from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.dataflow import Dataflow


# ============================================
# StreamForge Day 2
# Kafka -> Bytewax -> Kafka
# ============================================

BROKERS = ["localhost:9092"]

INPUT_TOPIC = "truck-telemetry"

OUTPUT_TOPIC = "processed-telemetry"


# ============================================
# Create Bytewax Dataflow
# ============================================

flow = Dataflow("streamforge-processing")


# ============================================
# 1. Read messages from Kafka
# ============================================

kafka_input = kop.input(
    "kafka-input",
    flow,
    brokers=BROKERS,
    topics=[INPUT_TOPIC],
)


# ============================================
# 2. Show Kafka errors
# ============================================

op.inspect(
    "kafka-errors",
    kafka_input.errs,
)


# ============================================
# 3. Extract message value
# ============================================

def extract_value(message):

    return message.value


messages = op.map(
    "extract-value",
    kafka_input.oks,
    extract_value,
)


# ============================================
# 4. Convert JSON bytes -> Python dictionary
# ============================================

def parse_json(value):

    if value is None:
        return None

    try:

        if isinstance(value, bytes):
            value = value.decode("utf-8")

        return json.loads(value)

    except (json.JSONDecodeError, UnicodeDecodeError):

        return None


parsed_messages = op.map(
    "parse-json",
    messages,
    parse_json,
)


# ============================================
# 5. Remove invalid messages
# ============================================

valid_messages = op.filter(
    "valid-messages",
    parsed_messages,
    lambda message: message is not None,
)


# ============================================
# 6. Filter temperature > 0
# ============================================

temperature_messages = op.filter(
    "temperature-greater-than-zero",
    valid_messages,
    lambda message: (
        message.get("temperature", 0) > 0
    ),
)


# ============================================
# 7. Transform the event
# ============================================

def transform_message(message):

    return {
        "truck_id": message.get("truck_id"),
        "temperature": message.get("temperature"),
        "timestamp": message.get("timestamp"),
        "processed": True,
    }


processed_messages = op.map(
    "transform-message",
    temperature_messages,
    transform_message,
)


# ============================================
# 8. Print processed messages
# ============================================

op.inspect(
    "processed-events",
    processed_messages,
)


# ============================================
# 9. Convert Python dictionary -> Kafka message
# ============================================

def create_kafka_message(message):

    key = message["truck_id"].encode("utf-8")

    value = json.dumps(message).encode("utf-8")

    return KafkaSinkMessage(
        key=key,
        value=value,
    )


kafka_messages = op.map(
    "create-kafka-message",
    processed_messages,
    create_kafka_message,
)


# ============================================
# 10. Send to Kafka
# ============================================

kop.output(
    "kafka-output",
    kafka_messages,
    brokers=BROKERS,
    topic=OUTPUT_TOPIC,
)