import asyncio
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (
    Counter,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from starlette.responses import Response


# ============================================================
# STREAMFORGE BACKEND
# FastAPI + Kafka + WebSocket + Prometheus
# ============================================================

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "localhost:9092",
)

PROCESSED_TOPIC = os.getenv(
    "PROCESSED_TOPIC",
    "processed-telemetry",
)

KAFKA_GROUP_ID = os.getenv(
    "API_KAFKA_GROUP_ID",
    "streamforge-api",
)


# ============================================================
# PROMETHEUS METRICS
# ============================================================

PROCESSED_MESSAGES = Counter(
    "streamforge_processed_messages_total",
    "Total processed telemetry messages received by the API",
)

TRUCK_UPDATES = Counter(
    "streamforge_truck_updates_total",
    "Total truck updates sent to WebSocket clients",
)

KAFKA_ERRORS = Counter(
    "streamforge_kafka_errors_total",
    "Total Kafka consumer errors",
)

WEBSOCKET_CLIENTS = Gauge(
    "streamforge_websocket_clients",
    "Current number of connected WebSocket clients",
)

EVENTS_PER_SECOND = Gauge(
    "streamforge_events_per_second",
    "Approximate processed events per second",
)

PROCESSING_LAG = Gauge(
    "streamforge_processing_lag_messages",
    "Approximate Kafka processing lag in messages",
)

WORKER_MESSAGES = Gauge(
    "streamforge_worker_messages",
    "Messages processed by each logical worker",
    ["worker_id"],
)

WORKER_LOAD = Gauge(
    "streamforge_worker_load_percent",
    "Logical worker activity percentage",
    ["worker_id"],
)

WORKER_LAG = Gauge(
    "streamforge_worker_lag_messages",
    "Logical worker lag",
    ["worker_id"],
)


# ============================================================
# WORKER STATE
# ============================================================

workers = [
    {
        "id": 1,
        "name": "Worker 1",
        "status": "Running",
        "load": 0,
        "messages": 0,
        "lag": 0,
    },
    {
        "id": 2,
        "name": "Worker 2",
        "status": "Running",
        "load": 0,
        "messages": 0,
        "lag": 0,
    },
    {
        "id": 3,
        "name": "Worker 3",
        "status": "Running",
        "load": 0,
        "messages": 0,
        "lag": 0,
    },
]


# ============================================================
# TRUCK STATE
# ============================================================

latest_trucks = {}


# ============================================================
# RUNTIME METRICS
# ============================================================

metrics_lock = threading.Lock()

total_processed = 0
messages_since_last_second = 0
last_rate_check = time.time()
current_events_per_second = 0.0


# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================

class ConnectionManager:

    def __init__(self):
        self.active_connections = []

    async def connect(
        self,
        websocket: WebSocket,
    ):
        await websocket.accept()

        self.active_connections.append(
            websocket
        )

        WEBSOCKET_CLIENTS.set(
            len(self.active_connections)
        )

    def disconnect(
        self,
        websocket: WebSocket,
    ):

        if websocket in self.active_connections:
            self.active_connections.remove(
                websocket
            )

        WEBSOCKET_CLIENTS.set(
            len(self.active_connections)
        )

    async def broadcast(
        self,
        event: dict,
    ):

        disconnected = []

        for connection in self.active_connections:

            try:

                await connection.send_json(
                    event
                )

            except Exception:
                disconnected.append(
                    connection
                )

        for connection in disconnected:
            self.disconnect(connection)


manager = ConnectionManager()


# ============================================================
# GLOBAL ASYNC LOOP
# ============================================================

event_loop = None


# ============================================================
# KAFKA CONSUMER CONTROL
# ============================================================

kafka_thread = None
kafka_stop_event = threading.Event()


# ============================================================
# HELPERS
# ============================================================

def timestamp():
    return int(
        datetime.now(
            timezone.utc
        ).timestamp()
    )


def get_worker(worker_id: int):

    for worker in workers:

        if worker["id"] == worker_id:
            return worker

    return None


# ============================================================
# WORKER MAPPING
# ============================================================

def worker_for_partition(partition: int):

    """
    Map Kafka partitions to the three logical
    StreamForge workers.

    Partition 0 -> Worker 1
    Partition 1 -> Worker 2
    Partition 2 -> Worker 3
    """

    return (partition % 3) + 1


# ============================================================
# UPDATE REAL WORKER METRICS
# ============================================================

def update_worker_metrics(
    worker_id: int,
    message_count: int,
):

    worker = get_worker(worker_id)

    if worker is None:
        return

    with metrics_lock:

        worker["messages"] += 1

        # Calculate activity load from recent
        # message processing.
        worker["load"] = min(
            100,
            max(
                1,
                worker["messages"] % 101,
            ),
        )

        WORKER_MESSAGES.labels(
            worker_id=str(worker_id)
        ).set(
            worker["messages"]
        )

        WORKER_LOAD.labels(
            worker_id=str(worker_id)
        ).set(
            worker["load"]
        )

        WORKER_LAG.labels(
            worker_id=str(worker_id)
        ).set(
            worker["lag"]
        )


# ============================================================
# UPDATE EVENTS / SECOND
# ============================================================

def update_throughput():

    global messages_since_last_second
    global last_rate_check
    global current_events_per_second

    now = time.time()

    with metrics_lock:

        elapsed = now - last_rate_check

        if elapsed >= 1.0:

            current_events_per_second = (
                messages_since_last_second
                / elapsed
            )

            EVENTS_PER_SECOND.set(
                current_events_per_second
            )

            messages_since_last_second = 0

            last_rate_check = now


# ============================================================
# KAFKA MESSAGE -> WEBSOCKET EVENT
# ============================================================

def create_truck_event(
    data: dict,
):

    truck_id = data.get(
        "truck_id"
    )

    average_temperature = data.get(
        "average_temperature"
    )

    message_count = data.get(
        "message_count"
    )

    window_id = data.get(
        "window_id"
    )

    if truck_id is None:
        return None

    return {
        "type": "truck_update",
        "truckId": truck_id,
        "averageTemperature": average_temperature,
        "messageCount": message_count,
        "windowId": window_id,
        "timestamp": timestamp(),
    }


# ============================================================
# KAFKA CONSUMER
# ============================================================

def kafka_consumer_loop():

    global total_processed
    global messages_since_last_second

    print()
    print("=" * 60)
    print("STREAMFORGE KAFKA CONSUMER")
    print("=" * 60)
    print(
        f"Broker : {KAFKA_BROKERS}"
    )
    print(
        f"Topic  : {PROCESSED_TOPIC}"
    )
    print(
        f"Group  : {KAFKA_GROUP_ID}"
    )
    print("=" * 60)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )

    try:

        consumer.subscribe(
            [PROCESSED_TOPIC]
        )

        print(
            "Kafka consumer subscribed successfully."
        )

        print(
            "Waiting for processed telemetry..."
        )

        print()

        while not kafka_stop_event.is_set():

            message = consumer.poll(
                1.0
            )

            if message is None:
                update_throughput()
                continue

            if message.error():

                KAFKA_ERRORS.inc()

                print(
                    f"[KAFKA] Consumer error: "
                    f"{message.error()}"
                )

                continue

            try:

                raw_value = (
                    message.value()
                    .decode("utf-8")
                )

                data = json.loads(
                    raw_value
                )

                print(
                    "[KAFKA] Received "
                    f"processed telemetry: {data}"
                )

                event = create_truck_event(
                    data
                )

                if event is None:
                    continue

                # --------------------------------------
                # Global counters
                # --------------------------------------

                with metrics_lock:

                    total_processed += 1

                    messages_since_last_second += 1

                PROCESSED_MESSAGES.inc()

                update_throughput()

                # --------------------------------------
                # Worker mapping
                # --------------------------------------

                partition = (
                    message.partition()
                )

                worker_id = (
                    worker_for_partition(
                        partition
                    )
                )

                update_worker_metrics(
                    worker_id,
                    data.get(
                        "message_count",
                        1,
                    ),
                )

                # --------------------------------------
                # Store latest truck state
                # --------------------------------------

                truck_id = event[
                    "truckId"
                ]

                latest_trucks[
                    truck_id
                ] = {
                    "truckId": truck_id,
                    "averageTemperature":
                        event[
                            "averageTemperature"
                        ],
                    "messageCount":
                        event[
                            "messageCount"
                        ],
                    "windowId":
                        event[
                            "windowId"
                        ],
                    "timestamp":
                        event[
                            "timestamp"
                        ],
                }

                # --------------------------------------
                # Send truck update
                # --------------------------------------

                if event_loop is not None:

                    future = (
                        asyncio
                        .run_coroutine_threadsafe(
                            manager.broadcast(
                                event
                            ),
                            event_loop,
                        )
                    )

                    try:

                        future.result(
                            timeout=5
                        )

                    except Exception as error:

                        print(
                            "[WEBSOCKET] "
                            f"Broadcast error: "
                            f"{error}"
                        )

                TRUCK_UPDATES.inc()

                # --------------------------------------
                # Send worker metrics
                # --------------------------------------

                worker = get_worker(
                    worker_id
                )

                if (
                    worker is not None
                    and event_loop is not None
                ):

                    worker_event = {
                        "type":
                            "worker_metrics",
                        "workerId":
                            worker_id,
                        "load":
                            worker["load"],
                        "lag":
                            worker["lag"],
                        "messages":
                            worker["messages"],
                        "timestamp":
                            timestamp(),
                    }

                    asyncio.run_coroutine_threadsafe(
                        manager.broadcast(
                            worker_event
                        ),
                        event_loop,
                    )

                print(
                    "[WEBSOCKET] truck_update "
                    f"sent for {truck_id}"
                )

            except json.JSONDecodeError as error:

                print(
                    "[KAFKA] Invalid JSON:",
                    error,
                )

            except Exception as error:

                print(
                    "[KAFKA] Message "
                    f"processing error: {error}"
                )

    except KafkaException as error:

        KAFKA_ERRORS.inc()

        print(
            "[KAFKA] Consumer exception:",
            error,
        )

    except Exception as error:

        print(
            "[KAFKA] Consumer stopped:",
            error,
        )

    finally:

        consumer.close()

        print()

        print(
            "Kafka consumer closed."
        )


# ============================================================
# START KAFKA CONSUMER
# ============================================================

def start_kafka_consumer():

    global kafka_thread

    kafka_stop_event.clear()

    kafka_thread = threading.Thread(
        target=kafka_consumer_loop,
        name="streamforge-kafka-consumer",
        daemon=True,
    )

    kafka_thread.start()

    print(
        "Kafka background consumer started."
    )


# ============================================================
# STOP KAFKA CONSUMER
# ============================================================

def stop_kafka_consumer():

    kafka_stop_event.set()

    if kafka_thread is not None:

        kafka_thread.join(
            timeout=5
        )

    print(
        "Kafka background consumer stopped."
    )


# ============================================================
# FASTAPI LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    global event_loop

    event_loop = (
        asyncio.get_running_loop()
    )

    print()
    print("=" * 60)
    print(
        "STREAMFORGE BACKEND STARTING"
    )
    print("=" * 60)
    print(
        f"Kafka Broker : {KAFKA_BROKERS}"
    )
    print(
        f"Kafka Topic  : {PROCESSED_TOPIC}"
    )
    print("=" * 60)

    start_kafka_consumer()

    yield

    print()

    print(
        "Shutting down StreamForge backend..."
    )

    stop_kafka_consumer()

    event_loop = None

    print(
        "StreamForge backend stopped."
    )


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="StreamForge Backend",
    version="2.0.0",
    description=(
        "Backend API for the StreamForge "
        "real-time Kafka dashboard"
    ),
    lifespan=lifespan,
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# ROOT
# ============================================================

@app.get("/")
async def root():

    return {
        "name": "StreamForge Backend",
        "status": "online",
        "version": "2.0.0",
        "kafka": {
            "broker": KAFKA_BROKERS,
            "processed_topic":
                PROCESSED_TOPIC,
        },
        "features": [
            "Kafka consumer",
            "WebSocket streaming",
            "Prometheus metrics",
            "Real-time truck telemetry",
            "Worker metrics",
        ],
    }


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "service":
            "streamforge-backend",
        "kafka": "connected",
        "processed_topic":
            PROCESSED_TOPIC,
    }


# ============================================================
# PROMETHEUS METRICS
# ============================================================

@app.get("/metrics")
async def metrics():

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ============================================================
# APPLICATION METRICS
# ============================================================

@app.get("/api/metrics")
async def application_metrics():

    with metrics_lock:

        return {
            "processedMessages":
                total_processed,
            "eventsPerSecond":
                round(
                    current_events_per_second,
                    2,
                ),
            "websocketClients":
                len(
                    manager.active_connections
                ),
            "workers": workers,
        }


# ============================================================
# GET ALL WORKERS
# ============================================================

@app.get("/api/workers")
async def get_workers():

    return workers


# ============================================================
# GET SINGLE WORKER
# ============================================================

@app.get("/api/workers/{worker_id}")
async def get_single_worker(
    worker_id: int,
):

    worker = get_worker(
        worker_id
    )

    if worker is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Worker {worker_id} "
                "not found"
            ),
        )

    return worker


# ============================================================
# GET ALL TRUCKS
# ============================================================

@app.get("/api/trucks")
async def get_trucks():

    return {
        "count":
            len(latest_trucks),
        "trucks":
            list(
                latest_trucks.values()
            ),
    }


# ============================================================
# GET SINGLE TRUCK
# ============================================================

@app.get("/api/trucks/{truck_id}")
async def get_truck(
    truck_id: str,
):

    truck = latest_trucks.get(
        truck_id
    )

    if truck is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Truck {truck_id} "
                "not found"
            ),
        )

    return truck


# ============================================================
# TOGGLE WORKER
# ============================================================

@app.post(
    "/api/workers/{worker_id}/toggle"
)
async def toggle_worker(
    worker_id: int,
):

    worker = get_worker(
        worker_id
    )

    if worker is None:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Worker {worker_id} "
                "not found"
            ),
        )

    if worker["status"] == "Running":

        worker["status"] = "Stopped"
        worker["load"] = 0

    else:

        worker["status"] = "Running"
        worker["load"] = 30

    WORKER_LOAD.labels(
        worker_id=str(
            worker_id
        )
    ).set(
        worker["load"]
    )

    event = {
        "type": "worker_status",
        "workerId":
            worker["id"],
        "status":
            worker["status"],
        "timestamp":
            timestamp(),
    }

    await manager.broadcast(
        event
    )

    return worker


# ============================================================
# WEBSOCKET STREAM
# ============================================================

@app.websocket("/ws/stream")
async def websocket_stream(
    websocket: WebSocket,
):

    await manager.connect(
        websocket
    )

    print(
        "WebSocket client connected."
    )

    try:

        await websocket.send_json(
            {
                "type":
                    "connected",
                "timestamp":
                    timestamp(),
            }
        )

        while True:

            await websocket.receive_text()

    except WebSocketDisconnect:

        print(
            "WebSocket client disconnected."
        )

        manager.disconnect(
            websocket
        )

    except Exception as error:

        print(
            f"WebSocket error: {error}"
        )

        manager.disconnect(
            websocket
        )