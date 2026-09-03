import { useCallback, useState } from "react";

import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  addEdge,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

import "@xyflow/react/dist/style.css";
import "./index.css";


// ===============================
// KAFKA TOPOLOGY
// ===============================

const initialNodes = [
  {
    id: "kafka",
    position: { x: 250, y: 50 },
    data: { label: "Kafka" },
    style: {
      background: "#2563eb",
      color: "white",
      padding: "12px",
      borderRadius: "8px",
      width: 120,
      textAlign: "center",
      fontWeight: "bold",
    },
  },

  {
    id: "processor",
    position: { x: 250, y: 150 },
    data: { label: "Processor" },
    style: {
      background: "#7c3aed",
      color: "white",
      padding: "12px",
      borderRadius: "8px",
      width: 120,
      textAlign: "center",
      fontWeight: "bold",
    },
  },

  {
    id: "worker1",
    position: { x: 220, y: 270 },
    data: {
      label: (
        <div>
          <strong>Worker 1</strong>
          <br />
          🟢 Running
          <br />
          Load: 42%
        </div>
      ),
    },
    style: {
      background: "#111827",
      color: "white",
      border: "2px solid #facc15",
      padding: "12px",
      borderRadius: "8px",
      width: 140,
      textAlign: "center",
    },
  },

  {
    id: "worker2",
    position: { x: 220, y: 390 },
    data: {
      label: (
        <div>
          <strong>Worker 2</strong>
          <br />
          🟢 Running
          <br />
          Load: 67%
        </div>
      ),
    },
    style: {
      background: "#111827",
      color: "white",
      border: "2px solid #facc15",
      padding: "12px",
      borderRadius: "8px",
      width: 140,
      textAlign: "center",
    },
  },

  {
    id: "worker3",
    position: { x: 220, y: 510 },
    data: {
      label: (
        <div>
          <strong>Worker 3</strong>
          <br />
          🔴 Stopped
          <br />
          Load: 0%
        </div>
      ),
    },
    style: {
      background: "#111827",
      color: "white",
      border: "2px solid #facc15",
      padding: "12px",
      borderRadius: "8px",
      width: 140,
      textAlign: "center",
    },
  },
];


const initialEdges = [
  {
    id: "kafka-processor",
    source: "kafka",
    target: "processor",
    animated: true,
  },

  {
    id: "processor-worker1",
    source: "processor",
    target: "worker1",
    animated: true,
  },

  {
    id: "processor-worker2",
    source: "processor",
    target: "worker2",
    animated: true,
  },

  {
    id: "processor-worker3",
    source: "processor",
    target: "worker3",
    animated: true,
  },
];


// ===============================
// WORKER DATA
// ===============================

const initialWorkers = [
  {
    id: 1,
    name: "Worker 1",
    status: "Running",
    load: 42,
    messages: 12,
  },

  {
    id: 2,
    name: "Worker 2",
    status: "Running",
    load: 67,
    messages: 18,
  },

  {
    id: 3,
    name: "Worker 3",
    status: "Stopped",
    load: 0,
    messages: 0,
  },
];


// ===============================
// APP
// ===============================

function App() {
  const [nodes, , onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const [messageCount, setMessageCount] =
    useState(0);

  const [activity, setActivity] =
    useState([]);

  // Day 7 worker state
  const [workers, setWorkers] =
    useState(initialWorkers);


  // ===============================
  // CONNECT NODES
  // ===============================

  const onConnect = useCallback(
    (connection) =>
      setEdges((edges) =>
        addEdge(connection, edges)
      ),
    [setEdges]
  );


  // ===============================
  // SIMULATE MESSAGE
  // ===============================

  const simulateMessage = () => {
    const newMessage = {
      id: Date.now(),

      text: "Message processed successfully",

      time: new Date().toLocaleTimeString(),
    };

    setMessageCount(
      (count) => count + 1
    );

    setActivity(
      (previousActivity) => [
        newMessage,
        ...previousActivity,
      ]
    );

    // Update Worker 1 message count
    setWorkers(
      (previousWorkers) =>
        previousWorkers.map((worker) =>
          worker.id === 1
            ? {
                ...worker,
                messages:
                  worker.messages + 1,
              }
            : worker
        )
    );
  };


  // ===============================
  // TOGGLE WORKER
  // ===============================

  const toggleWorker = (workerId) => {
    setWorkers(
      (previousWorkers) =>
        previousWorkers.map((worker) => {
          if (worker.id !== workerId) {
            return worker;
          }

          const isRunning =
            worker.status === "Running";

          return {
            ...worker,

            status: isRunning
              ? "Stopped"
              : "Running",

            load: isRunning
              ? 0
              : Math.floor(
                  Math.random() * 50
                ) + 30,
          };
        })
    );
  };


  return (
    <div className="app">

      {/* =========================
          HEADER
      ========================= */}

      <h1>
        Real-Time Kafka Streaming Dashboard
      </h1>

      <div className="status">
        🟢 System Online
      </div>


      {/* =========================
          STATISTICS
      ========================= */}

      <div className="stats">

        <div className="card">
          <h3>Messages</h3>
          <p>{messageCount}</p>
        </div>

        <div className="card">
          <h3>Producers</h3>
          <p>1</p>
        </div>

        <div className="card">
          <h3>Consumers</h3>
          <p>3</p>
        </div>

      </div>


      {/* =========================
          TOPOLOGY
      ========================= */}

      <h2>
        Kafka Stream Topology
      </h2>

      <div className="flow-container">

        <ReactFlow
          nodes={nodes}
          edges={edges}

          onNodesChange={
            onNodesChange
          }

          onEdgesChange={
            onEdgesChange
          }

          onConnect={onConnect}

          fitView
        >

          <Background />

          <Controls />

          <MiniMap />

        </ReactFlow>

      </div>


      {/* =========================
          DAY 7 WORKER MONITORING
      ========================= */}

      <h2>
        Worker Monitoring
      </h2>

      <div className="worker-monitoring">

        {workers.map((worker) => (

          <div
            className="monitor-card"
            key={worker.id}
          >

            <div className="monitor-header">

              <h3>
                {worker.name}
              </h3>

              <span
                className={
                  worker.status === "Running"
                    ? "monitor-status running"
                    : "monitor-status stopped"
                }
              >
                {worker.status === "Running"
                  ? "🟢 Running"
                  : "🔴 Stopped"}
              </span>

            </div>


            <div className="monitor-info">

              <p>
                <strong>Load:</strong>{" "}
                {worker.load}%
              </p>

              <div className="load-bar">

                <div
                  className="load-fill"
                  style={{
                    width: `${worker.load}%`,
                  }}
                ></div>

              </div>


              <p>
                <strong>
                  Messages:
                </strong>{" "}
                {worker.messages}
              </p>

            </div>


            <button
              className="worker-button"
              onClick={() =>
                toggleWorker(worker.id)
              }
            >
              {worker.status === "Running"
                ? "Stop Worker"
                : "Start Worker"}
            </button>

          </div>

        ))}

      </div>


      {/* =========================
          STREAM ACTIVITY
      ========================= */}

      <h2>
        Stream Activity
      </h2>

      <button
        className="simulate-button"
        onClick={simulateMessage}
      >
        Simulate Message
      </button>

      <p>
        Messages processed:{" "}
        {messageCount}
      </p>


      {/* ACTIVITY LOG */}

      <div className="activity-list">

        {activity.length === 0 ? (

          <p>
            No messages yet.
          </p>

        ) : (

          activity.map((item) => (

            <div
              className="activity-item"
              key={item.id}
            >

              <span>🟢</span>

              <span>
                {item.text}
              </span>

              <small>
                {item.time}
              </small>

            </div>

          ))

        )}

      </div>

    </div>
  );
}


export default App;