from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone


# ============================================================
# STREAMFORGE BACKEND - DAY 1
# ============================================================

app = FastAPI(
    title="StreamForge Backend",
    version="1.0.0",
    description="Backend API for the StreamForge real-time Kafka dashboard",
)


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# WORKER STATE
# ============================================================

workers = [
    {
        "id": 1,
        "name": "Worker 1",
        "status": "Running",
        "load": 42,
        "messages": 12,
    },
    {
        "id": 2,
        "name": "Worker 2",
        "status": "Running",
        "load": 67,
        "messages": 18,
    },
    {
        "id": 3,
        "name": "Worker 3",
        "status": "Running",
        "load": 35,
        "messages": 9,
    },
]


# ============================================================
# WEBSOCKET CONNECTION MANAGER
# ============================================================

class ConnectionManager:

    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, event: dict):

        disconnected = []

        for connection in self.active_connections:
            try:
                await connection.send_json(event)
            except Exception:
                disconnected.append(connection)

        for connection in disconnected:
            self.disconnect(connection)


manager = ConnectionManager()


# ============================================================
# HELPERS
# ============================================================

def timestamp():
    return int(datetime.now(timezone.utc).timestamp())


def get_worker(worker_id: int):

    for worker in workers:
        if worker["id"] == worker_id:
            return worker

    return None


# ============================================================
# ROOT
# ============================================================

@app.get("/")
async def root():

    return {
        "name": "StreamForge Backend",
        "status": "online",
        "version": "1.0.0",
    }


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "service": "streamforge-backend",
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
async def get_single_worker(worker_id: int):

    worker = get_worker(worker_id)

    if worker is None:
        raise HTTPException(
            status_code=404,
            detail=f"Worker {worker_id} not found",
        )

    return worker


# ============================================================
# TOGGLE WORKER
# ============================================================

@app.post("/api/workers/{worker_id}/toggle")
async def toggle_worker(worker_id: int):

    worker = get_worker(worker_id)

    if worker is None:
        raise HTTPException(
            status_code=404,
            detail=f"Worker {worker_id} not found",
        )

    # ------------------------------------------
    # Running -> Stopped
    # ------------------------------------------

    if worker["status"] == "Running":

        worker["status"] = "Stopped"
        worker["load"] = 0

    # ------------------------------------------
    # Stopped -> Running
    # ------------------------------------------

    else:

        worker["status"] = "Running"
        worker["load"] = 30

    # ------------------------------------------
    # WebSocket event
    # ------------------------------------------

    event = {
        "type": "worker_status",
        "workerId": worker["id"],
        "status": worker["status"],
        "timestamp": timestamp(),
    }

    await manager.broadcast(event)

    return worker


# ============================================================
# WEBSOCKET STREAM
# ============================================================

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):

    await manager.connect(websocket)

    print("WebSocket client connected.")

    try:

        # Tell frontend the connection succeeded
        await websocket.send_json(
            {
                "type": "connected",
                "timestamp": timestamp(),
            }
        )

        while True:

            # Keep connection alive.
            # Real Kafka events will be pushed here later.
            await websocket.receive_text()

    except WebSocketDisconnect:

        print("WebSocket client disconnected.")

        manager.disconnect(websocket)

    except Exception as error:

        print(f"WebSocket error: {error}")

        manager.disconnect(websocket)