import { useCallback } from "react";
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
    position: { x: 50, y: 150 },
    data: { label: "Kafka\n🟢 Running" },
    style: {
      background: "#2563eb",
      color: "white",
      padding: "15px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
      fontWeight: "bold",
    },
  },
  {
    id: "processor",
    position: { x: 300, y: 150 },
    data: { label: "Processor\n🟢 Running" },
    style: {
      background: "#7c3aed",
      color: "white",
      padding: "15px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
      fontWeight: "bold",
    },
  },
  {
    id: "worker",
    position: { x: 550, y: 150 },
    data: { label: "Worker\n🟢 Running" },
    style: {
      background: "#059669",
      color: "white",
      padding: "15px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
      fontWeight: "bold",
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
    id: "processor-worker",
    source: "processor",
    target: "worker",
    animated: true,
  },
];

function App() {
  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const onConnect = useCallback(
    (connection) =>
      setEdges((edges) => addEdge(connection, edges)),
    [setEdges]
  );

  return (
    <div className="app">
      <h1>Real-Time Kafka Streaming Dashboard</h1>

      <div className="status">
        🟢 System Online
      </div>

      <div className="stats">
        <div className="card">
          <h3>Messages</h3>
          <p>0</p>
        </div>

        <div className="card">
          <h3>Producers</h3>
          <p>1</p>
        </div>

        <div className="card">
          <h3>Consumers</h3>
          <p>1</p>
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

      <button>
        Simulate Message
      </button>
    </div>
  );
}

export default App;