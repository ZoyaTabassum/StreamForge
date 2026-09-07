import {
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

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
    position: { x: 250, y: 40 },
    data: { label: "Kafka" },
    style: {
      background: "#2563eb",
      color: "white",
      padding: "12px",
      borderRadius: "10px",
      width: 120,
      textAlign: "center",
      fontWeight: "bold",
      border: "1px solid #3b82f6",
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
      borderRadius: "10px",
      width: 120,
      textAlign: "center",
      fontWeight: "bold",
      border: "1px solid #8b5cf6",
    },
  },
  {
    id: "worker1",
    position: { x: 220, y: 270 },
    data: { label: "Worker 1" },
    style: {
      background: "#111827",
      color: "white",
      padding: "12px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
    },
  },
  {
    id: "worker2",
    position: { x: 220, y: 390 },
    data: { label: "Worker 2" },
    style: {
      background: "#111827",
      color: "white",
      padding: "12px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
    },
  },
  {
    id: "worker3",
    position: { x: 220, y: 510 },
    data: { label: "Worker 3" },
    style: {
      background: "#111827",
      color: "white",
      padding: "12px",
      borderRadius: "10px",
      width: 150,
      textAlign: "center",
    },
  },
];

const initialEdges = [
  { id: "kafka-processor", source: "kafka", target: "processor", animated: true },
  { id: "processor-worker1", source: "processor", target: "worker1", animated: true },
  { id: "processor-worker2", source: "processor", target: "worker2", animated: true },
  { id: "processor-worker3", source: "processor", target: "worker3", animated: true },
];

// ===============================
// WORKER DATA
// ===============================

const initialWorkers = [
  { id: 1, name: "Worker 1", status: "Running", load: 42, messages: 12 },
  { id: 2, name: "Worker 2", status: "Running", load: 67, messages: 18 },
  { id: 3, name: "Worker 3", status: "Stopped", load: 0, messages: 0 },
];

// Rolling window shown on the rebalance timeline (2 minutes).
const TIMELINE_WINDOW_MS = 120000;

function NodeLabel({ worker }) {
  const isRunning = worker.status === "Running";
  return (
    <div>
      <strong>{worker.name}</strong>
      <br />
      {isRunning ? "🟢 Running" : "🔴 Stopped"}
      <br />
      Load: {worker.load}%
    </div>
  );
}

// ===============================
// APP
// ===============================

function App() {
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  const [messageCount, setMessageCount] = useState(0);
  const [activity, setActivity] = useState([]);
  const [workers, setWorkers] = useState(initialWorkers);
  const [autoStream, setAutoStream] = useState(false);
  const [activityFilter, setActivityFilter] = useState("all");

  // DAY 12 - per-worker up/down segments for the rebalance timeline.
  const [workerHistory, setWorkerHistory] = useState(() =>
    initialWorkers.map((w) => ({
      id: w.id,
      segments: [{ status: w.status, start: Date.now(), end: null }],
    }))
  );

  // Ticks once a second so the "in progress" timeline segment keeps growing live.
  const [now, setNow] = useState(Date.now());

  const workersRef = useRef(workers);
  useEffect(() => {
    workersRef.current = workers;
  }, [workers]);

  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);

  // ===============================
  // REFLECT WORKER STATE ONTO GRAPH NODES
  // ===============================

  useEffect(() => {
    setNodes((currentNodes) =>
      currentNodes.map((node) => {
        const worker = workers.find((w) => `worker${w.id}` === node.id);
        if (!worker) return node;

        const isRunning = worker.status === "Running";
        const isHot = isRunning && worker.load >= 85;
        const borderColor = !isRunning ? "#ef4444" : isHot ? "#f59e0b" : "#22d3ee";
        const glow = !isRunning
          ? "none"
          : isHot
          ? "0 0 14px rgba(245,158,11,0.55)"
          : "0 0 10px rgba(34,211,238,0.35)";

        return {
          ...node,
          data: { label: <NodeLabel worker={worker} /> },
          style: {
            ...node.style,
            border: `2px solid ${borderColor}`,
            opacity: isRunning ? 1 : 0.5,
            boxShadow: glow,
            transition: "all 0.4s ease",
          },
        };
      })
    );
  }, [workers, setNodes]);

  // ===============================
  // REFLECT WORKER STATE ONTO EDGES
  // ===============================

  useEffect(() => {
    setEdges((currentEdges) =>
      currentEdges.map((edge) => {
        const worker = workers.find((w) => `worker${w.id}` === edge.target);
        if (!worker) return edge;

        const isRunning = worker.status === "Running";
        return {
          ...edge,
          animated: isRunning,
          style: {
            stroke: isRunning ? "#22d3ee" : "#374151",
            strokeDasharray: isRunning ? undefined : "5 5",
          },
        };
      })
    );
  }, [workers, setEdges]);

  const onConnect = useCallback(
    (connection) => setEdges((currentEdges) => addEdge(connection, currentEdges)),
    [setEdges]
  );

  // ===============================
  // SIMULATE MESSAGE
  // ===============================

  const simulateMessage = useCallback(() => {
    const currentWorkers = workersRef.current;
    const runningWorkers = currentWorkers.filter((w) => w.status === "Running");
    if (runningWorkers.length === 0) return;

    const selectedWorker = runningWorkers[Math.floor(Math.random() * runningWorkers.length)];
    const loadIncrease = Math.floor(Math.random() * 10) + 1;

    setMessageCount((count) => count + 1);

    setActivity((previousActivity) => [
      {
        id: Date.now() + Math.random(),
        type: "processed",
        text: `${selectedWorker.name} processed message successfully`,
        time: new Date().toLocaleTimeString(),
      },
      ...previousActivity,
    ].slice(0, 40));

    setWorkers((previousWorkers) =>
      previousWorkers.map((worker) =>
        worker.id !== selectedWorker.id
          ? worker
          : {
              ...worker,
              messages: worker.messages + 1,
              load: Math.min(100, worker.load + loadIncrease),
            }
      )
    );
  }, []);

  useEffect(() => {
    if (!autoStream) return;
    const intervalId = setInterval(() => simulateMessage(), 1000);
    return () => clearInterval(intervalId);
  }, [autoStream, simulateMessage]);

  // ===============================
  // WORKER LOAD RECOVERY
  // ===============================

  useEffect(() => {
    const recoveryInterval = setInterval(() => {
      setWorkers((previousWorkers) =>
        previousWorkers.map((worker) =>
          worker.status !== "Running"
            ? { ...worker, load: 0 }
            : { ...worker, load: Math.max(0, worker.load - 2) }
        )
      );
    }, 2000);
    return () => clearInterval(recoveryInterval);
  }, []);

  const toggleAutoStream = () => setAutoStream((current) => !current);

  // ===============================
  // TOGGLE WORKER (+ event log)
  // ===============================

  const toggleWorker = (workerId) => {
    const worker = workers.find((w) => w.id === workerId);
    if (!worker) return;
    const isRunning = worker.status === "Running";
    const newStatus = isRunning ? "Stopped" : "Running";
    const changeTime = Date.now();

    setWorkers((previousWorkers) =>
      previousWorkers.map((w) =>
        w.id !== workerId
          ? w
          : {
              ...w,
              status: newStatus,
              load: isRunning ? 0 : Math.floor(Math.random() * 50) + 30,
            }
      )
    );

    // DAY 12 - close the current timeline segment and open a new one.
    setWorkerHistory((previousHistory) =>
      previousHistory.map((h) => {
        if (h.id !== workerId) return h;
        const segments = [...h.segments];
        const last = segments[segments.length - 1];
        segments[segments.length - 1] = { ...last, end: changeTime };
        segments.push({ status: newStatus, start: changeTime, end: null });
        return { ...h, segments };
      })
    );

    setActivity((previousActivity) => [
      {
        id: Date.now() + Math.random(),
        type: isRunning ? "stopped" : "recovered",
        text: isRunning
          ? `${worker.name} went offline — partition rebalancing to healthy workers`
          : `${worker.name} back online — state recovered from changelog`,
        time: new Date().toLocaleTimeString(),
      },
      ...previousActivity,
    ].slice(0, 40));
  };

  // ===============================
  // ALERTS (derived, not stored state)
  // ===============================

  const alerts = [
    ...workers
      .filter((w) => w.status !== "Running")
      .map((w) => ({
        id: `down-${w.id}`,
        level: "critical",
        text: `${w.name} is down — traffic rerouted to healthy workers`,
      })),
    ...workers
      .filter((w) => w.status === "Running" && w.load >= 85)
      .map((w) => ({
        id: `hot-${w.id}`,
        level: "warning",
        text: `${w.name} approaching capacity (${w.load}% load)`,
      })),
  ];

  const loadClass = (load) => (load >= 85 ? "hot" : load >= 60 ? "warm" : "cool");

  // ===============================
  // DAY 12 - FILTERED ACTIVITY
  // ===============================

  const filteredActivity = activity.filter((item) => {
    if (activityFilter === "all") return true;
    if (activityFilter === "processed") return item.type === "processed";
    if (activityFilter === "rebalance") return item.type === "stopped" || item.type === "recovered";
    return true;
  });

  // ===============================
  // DAY 12 - TIMELINE GEOMETRY
  // ===============================

  const windowStart = now - TIMELINE_WINDOW_MS;

  const timelineRows = workerHistory.map((h) => {
    const worker = workers.find((w) => w.id === h.id);
    const bars = h.segments
      .map((seg) => {
        const segStart = Math.max(seg.start, windowStart);
        const segEnd = seg.end ?? now;
        if (segEnd < windowStart) return null;
        const leftPct = ((segStart - windowStart) / TIMELINE_WINDOW_MS) * 100;
        const widthPct = ((segEnd - segStart) / TIMELINE_WINDOW_MS) * 100;
        return {
          key: seg.start,
          left: Math.max(0, leftPct),
          width: Math.max(0.5, widthPct),
          status: seg.status,
        };
      })
      .filter(Boolean);
    return { id: h.id, name: worker ? worker.name : `Worker ${h.id}`, bars };
  });

  // ===============================
  // UI
  // ===============================

  return (
    <div className="app">
      <header className="app-header">
        <h1>Real-Time Kafka Streaming Dashboard</h1>
        <div className="status">● System Online</div>
      </header>

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
          <p>{workers.length}</p>
        </div>
      </div>

      {alerts.length > 0 && (
        <div className="alert-stack">
          {alerts.map((alert) => (
            <div key={alert.id} className={`alert-item ${alert.level}`}>
              <span className="alert-dot" />
              {alert.text}
            </div>
          ))}
        </div>
      )}

      <h2 className="section-title">Kafka Stream Topology</h2>
      <div className="flow-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          fitView
        >
          <Background gap={18} color="#1b2436" />
          <Controls />
          <MiniMap pannable zoomable />
        </ReactFlow>
      </div>

      <h2 className="section-title">Worker Monitoring</h2>
      <div className="worker-monitoring">
        {workers.map((worker) => (
          <div className={`monitor-card ${worker.status === "Running" ? "" : "is-down"}`} key={worker.id}>
            <div className="monitor-header">
              <h3>{worker.name}</h3>
              <span className={worker.status === "Running" ? "monitor-status running" : "monitor-status stopped"}>
                {worker.status === "Running" ? "🟢 Running" : "🔴 Stopped"}
              </span>
            </div>

            <div className="monitor-info">
              <p><strong>Load:</strong> {worker.load}%</p>
              <div className="load-bar">
                <div
                  className={`load-fill ${loadClass(worker.load)}`}
                  style={{ width: `${worker.load}%` }}
                />
              </div>
              <p><strong>Messages:</strong> {worker.messages}</p>
            </div>

            <button className="worker-button" onClick={() => toggleWorker(worker.id)}>
              {worker.status === "Running" ? "Stop Worker" : "Start Worker"}
            </button>
          </div>
        ))}
      </div>

      <h2 className="section-title">Partition Rebalance Timeline</h2>
      <div className="timeline-panel">
        {timelineRows.map((row) => (
          <div className="timeline-row" key={row.id}>
            <span className="timeline-label">{row.name}</span>
            <div className="timeline-track">
              {row.bars.map((bar) => (
                <div
                  key={bar.key}
                  className={`timeline-bar ${bar.status === "Running" ? "up" : "down"}`}
                  style={{ left: `${bar.left}%`, width: `${bar.width}%` }}
                />
              ))}
            </div>
          </div>
        ))}
        <div className="timeline-legend">
          <span><i className="dot up" /> Running</span>
          <span><i className="dot down" /> Down / rebalancing</span>
          <span className="timeline-window">last {TIMELINE_WINDOW_MS / 60000} min</span>
        </div>
      </div>

      <h2 className="section-title">Stream Activity</h2>

      <div className={autoStream ? "auto-stream-status active" : "auto-stream-status"}>
        {autoStream ? "🟢 Auto Stream Running" : "⚪ Auto Stream Stopped"}
      </div>

      <div className="stream-controls">
        <button className="simulate-button" onClick={simulateMessage}>
          Simulate Message
        </button>
        <button className="auto-stream-button" onClick={toggleAutoStream}>
          {autoStream ? "⏹ Stop Auto Stream" : "▶ Start Auto Stream"}
        </button>
      </div>

      <p className="messages-processed">Messages processed: {messageCount}</p>

      <div className="activity-filters">
        <button
          className={activityFilter === "all" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("all")}
        >
          All
        </button>
        <button
          className={activityFilter === "processed" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("processed")}
        >
          Messages
        </button>
        <button
          className={activityFilter === "rebalance" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("rebalance")}
        >
          Rebalance events
        </button>
      </div>

      <div className="activity-list">
        {filteredActivity.length === 0 ? (
          <p className="empty-state">No matching events yet.</p>
        ) : (
          filteredActivity.map((item) => (
            <div className={`activity-item ${item.type}`} key={item.id}>
              <span className="activity-dot" />
              <span className="activity-text">{item.text}</span>
              <small>{item.time}</small>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export default App;