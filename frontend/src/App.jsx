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

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const [messageCount, setMessageCount] = useState(0);

  const onConnect = useCallback(
    (connection) =>
      setEdges((edges) => addEdge(connection, edges)),
    [setEdges]
  );

  const simulateMessage = () => {
    setMessageCount((count) => count + 1);
  };

  return (
    <div className="app">
      <h1>Real-Time Kafka Streaming Dashboard</h1>

      <div className="status">
        🟢 System Online
      </div>

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

      <h2>Kafka Stream Topology</h2>

      <div className="flow-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </div>

      <h2>Stream Activity</h2>

      <button onClick={simulateMessage}>
        Simulate Message
      </button>

      <p>
        Messages processed: {messageCount}
      </p>
    </div>
  );
}

export default App;