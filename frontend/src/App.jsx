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
  { id: "kafka-processor", source: "kafka", target: "processor", animated: true, data: { base: true } },
  { id: "processor-worker1", source: "processor", target: "worker1", animated: true, data: { base: true } },
  { id: "processor-worker2", source: "processor", target: "worker2", animated: true, data: { base: true } },
  { id: "processor-worker3", source: "processor", target: "worker3", animated: true, data: { base: true } },
];

// ===============================
// WORKER DATA
// ===============================

const initialWorkers = [
  { id: 1, name: "Worker 1", status: "Running", load: 42, messages: 12, lag: 0, lagHistory: [0] },
  { id: 2, name: "Worker 2", status: "Running", load: 67, messages: 18, lag: 0, lagHistory: [0] },
  { id: 3, name: "Worker 3", status: "Stopped", load: 0, messages: 0, lag: 0, lagHistory: [0] },
];

// Rolling window shown on the rebalance timeline (2 minutes).
const TIMELINE_WINDOW_MS = 120000;

// DAY 16 - simulated Prometheus-style consumer lag.
const PROM_SCRAPE_INTERVAL_MS = 2000;
const LAG_RISE_LOAD_THRESHOLD = 70;
const LAG_DRAIN_LOAD_THRESHOLD = 40;
const BOTTLENECK_LAG_MS = 2500;
const LAG_HISTORY_LENGTH = 20;

// DAY 17 - cluster-wide throughput sampling.
// Each scrape records how many messages the whole cluster processed since
// the previous scrape, converted to events/sec. This is the aggregate view
// the Week 4 brief asks for ("expose metrics like processing lag, events/sec").
const THROUGHPUT_HISTORY_LENGTH = 30;

function NodeLabel({ worker, isBottleneck }) {
  const isRunning = worker.status === "Running";
  return (
    <div>
      <strong>{isBottleneck ? "🔥 " : ""}{worker.name}</strong>
      <br />
      {isRunning ? "🟢 Running" : "🔴 Stopped"}
      <br />
      Load: {worker.load}%
      <br />
      Lag: {worker.lag}ms
    </div>
  );
}

// DAY 16 - tiny inline sparkline, no chart library needed.
function LagSparkline({ history, isBottleneck }) {
  const max = Math.max(1, ...history);
  const points = history
    .map((value, index) => {
      const x = (index / Math.max(1, history.length - 1)) * 100;
      const y = 24 - (value / max) * 22;
      return `${x},${y}`;
    })
    .join(" ");

  return (
    <svg viewBox="0 0 100 24" className="lag-sparkline" preserveAspectRatio="none" aria-hidden="true">
      <polyline
        points={points}
        fill="none"
        stroke={isBottleneck ? "#fb7185" : "#67e8f9"}
        strokeWidth="2"
      />
    </svg>
  );
}

// DAY 17 - larger area chart for cluster throughput over time.
function ThroughputChart({ history }) {
  if (history.length < 2) {
    return <p className="chart-empty">Collecting samples…</p>;
  }

  const max = Math.max(1, ...history.map((h) => h.eps));
  const width = 100;
  const height = 60;

  const toPoint = (sample, index) => {
    const x = (index / (history.length - 1)) * width;
    const y = height - (sample.eps / max) * (height - 6);
    return { x, y };
  };

  const points = history.map(toPoint);
  const line = points.map((p) => `${p.x},${p.y}`).join(" ");
  const area = `0,${height} ${line} ${width},${height}`;

  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      className="throughput-chart"
      preserveAspectRatio="none"
      role="img"
      aria-label={`Cluster throughput over the last ${history.length} samples, peak ${max.toFixed(1)} events per second`}
    >
      <defs>
        <linearGradient id="tpGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#22d3ee" stopOpacity="0.45" />
          <stop offset="100%" stopColor="#22d3ee" stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon points={area} fill="url(#tpGrad)" />
      <polyline points={line} fill="none" stroke="#22d3ee" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
    </svg>
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

  // DAY 13 - which healthy worker currently covers a down worker's partition.
  const [coverage, setCoverage] = useState({});

  // DAY 12 - per-worker up/down segments for the rebalance timeline.
  const [workerHistory, setWorkerHistory] = useState(() =>
    initialWorkers.map((w) => ({
      id: w.id,
      segments: [{ status: w.status, start: Date.now(), end: null }],
    }))
  );

  const [now, setNow] = useState(Date.now());

  // DAY 15 - Chaos Monkey toggle + tick counter to reschedule itself
  const [chaosMonkey, setChaosMonkey] = useState(false);
  const [chaosTick, setChaosTick] = useState(0);

  // DAY 17 - rolling cluster throughput samples.
  const [throughputHistory, setThroughputHistory] = useState([]);

  const workersRef = useRef(workers);
  useEffect(() => {
    workersRef.current = workers;
  }, [workers]);

  // DAY 17 - coverage must be readable synchronously inside toggleWorker so we
  // can compute the activity message without depending on updater timing.
  const coverageRef = useRef(coverage);
  useEffect(() => {
    coverageRef.current = coverage;
  }, [coverage]);

  // DAY 17 - lets the throughput sampler compute a delta without re-subscribing.
  const messageCountRef = useRef(messageCount);
  useEffect(() => {
    messageCountRef.current = messageCount;
  }, [messageCount]);

  const toggleWorkerRef = useRef();

  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);

  // ===============================
  // DAY 16 - BOTTLENECK DETECTION
  // ===============================

  const runningWorkersForLag = workers.filter((w) => w.status === "Running");
  const worstLagWorker =
    runningWorkersForLag.length > 0
      ? runningWorkersForLag.reduce((worst, w) => (w.lag > worst.lag ? w : worst), runningWorkersForLag[0])
      : null;
  const bottleneckWorkerId =
    worstLagWorker && worstLagWorker.lag >= BOTTLENECK_LAG_MS ? worstLagWorker.id : null;

  const prevBottleneckRef = useRef(null);
  useEffect(() => {
    if (bottleneckWorkerId === prevBottleneckRef.current) return;

    const eventTime = new Date().toLocaleTimeString();

    if (bottleneckWorkerId) {
      const w = workersRef.current.find((worker) => worker.id === bottleneckWorkerId);
      setActivity((previousActivity) => [
        {
          id: Date.now() + Math.random(),
          type: "bottleneck",
          text: `${w ? w.name : "A worker"} flagged as the bottleneck — consumer lag ${w ? w.lag : "?"}ms`,
          time: eventTime,
        },
        ...previousActivity,
      ].slice(0, 40));
    } else if (prevBottleneckRef.current !== null) {
      setActivity((previousActivity) => [
        {
          id: Date.now() + Math.random(),
          type: "recovered",
          text: "Bottleneck cleared — consumer lag back to normal",
          time: eventTime,
        },
        ...previousActivity,
      ].slice(0, 40));
    }

    prevBottleneckRef.current = bottleneckWorkerId;
  }, [bottleneckWorkerId]);

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
        const isBottleneck = worker.id === bottleneckWorkerId;

        const borderColor = !isRunning ? "#ef4444" : isHot ? "#f59e0b" : "#22d3ee";
        const glow = !isRunning
          ? "none"
          : isHot
          ? "0 0 14px rgba(245,158,11,0.55)"
          : "0 0 10px rgba(34,211,238,0.35)";

        return {
          ...node,
          data: { label: <NodeLabel worker={worker} isBottleneck={isBottleneck} /> },
          style: {
            ...node.style,
            border: isBottleneck ? "3px solid #fb7185" : `2px solid ${borderColor}`,
            opacity: isRunning ? 1 : 0.5,
            boxShadow: isBottleneck ? "0 0 16px rgba(244,63,94,0.6)" : glow,
            animation: isBottleneck ? "bottleneckPulse 1.1s ease-in-out infinite" : "none",
            transition: "all 0.4s ease",
          },
        };
      })
    );
  }, [workers, bottleneckWorkerId, setNodes]);

  // ===============================
  // REFLECT WORKER STATE ONTO EDGES
  // ===============================

  useEffect(() => {
    setEdges((currentEdges) =>
      currentEdges.map((edge) => {
        if (!edge.data?.base) return edge;

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
  // WORKER LOAD RECOVERY + DAY 16 CONSUMER LAG + DAY 17 THROUGHPUT SAMPLE
  // ===============================

  useEffect(() => {
    let lastSampledCount = messageCountRef.current;

    const recoveryInterval = setInterval(() => {
      // DAY 17 - sample cluster throughput for this scrape window.
      const currentCount = messageCountRef.current;
      const delta = currentCount - lastSampledCount;
      lastSampledCount = currentCount;
      const eps = delta / (PROM_SCRAPE_INTERVAL_MS / 1000);

      setThroughputHistory((previous) =>
        [...previous, { t: Date.now(), eps }].slice(-THROUGHPUT_HISTORY_LENGTH)
      );

      setWorkers((previousWorkers) =>
        previousWorkers.map((worker) => {
          if (worker.status !== "Running") {
            const drainedLag = Math.max(0, worker.lag - 300);
            return {
              ...worker,
              load: 0,
              lag: drainedLag,
              lagHistory: [...worker.lagHistory.slice(-(LAG_HISTORY_LENGTH - 1)), drainedLag],
            };
          }

          const nextLoad = Math.max(0, worker.load - 2);

          let lagDelta;
          if (worker.load >= LAG_RISE_LOAD_THRESHOLD) {
            lagDelta = 60 + Math.random() * 140;
          } else if (worker.load <= LAG_DRAIN_LOAD_THRESHOLD) {
            lagDelta = -(80 + Math.random() * 120);
          } else {
            lagDelta = (Math.random() - 0.5) * 40;
          }

          const nextLag = Math.max(0, Math.round(worker.lag + lagDelta));

          return {
            ...worker,
            load: nextLoad,
            lag: nextLag,
            lagHistory: [...worker.lagHistory.slice(-(LAG_HISTORY_LENGTH - 1)), nextLag],
          };
        })
      );
    }, PROM_SCRAPE_INTERVAL_MS);

    return () => clearInterval(recoveryInterval);
  }, []);

  const toggleAutoStream = () => setAutoStream((current) => !current);

  // ===============================
  // DAY 15 - temporary migrate/reclaim edge on the graph
  // ===============================

  const spawnPartitionEdge = useCallback(
    (sourceId, targetId, label, stroke, labelColor) => {
      const kind = label === "partition moving" ? "migrate" : "reclaim";
      const edgeId = `${kind}-${sourceId}-${targetId}-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2, 7)}`;

      setEdges((currentEdges) => [
        ...currentEdges,
        {
          id: edgeId,
          source: `worker${sourceId}`,
          target: `worker${targetId}`,
          animated: true,
          label,
          labelBgPadding: [6, 3],
          labelBgBorderRadius: 6,
          labelStyle: { fill: labelColor, fontSize: 10, fontWeight: 600 },
          labelBgStyle: { fill: "#1a1526" },
          style: { stroke, strokeWidth: 2, strokeDasharray: "4 4" },
        },
      ]);

      setTimeout(() => {
        setEdges((currentEdges) => currentEdges.filter((e) => e.id !== edgeId));
      }, 3200);
    },
    [setEdges]
  );

  // ===============================
  // TOGGLE WORKER — DAY 17 rewrite
  // ===============================
  //
  // WHY THIS CHANGED: the Day 15/16 version assigned `activityText` INSIDE the
  // setCoverage updater, then read it immediately afterwards. React does not
  // guarantee an updater has run by then, and under StrictMode updaters run
  // twice — so the log line sometimes fell through to the generic fallback and
  // partition edges could be drawn twice.
  //
  // Now: everything is computed from refs FIRST (pure, synchronous), then the
  // setters are called with already-decided values. Updaters stay side-effect
  // free, which is what React actually expects.

  const toggleWorker = useCallback(
    (workerId) => {
      const currentWorkers = workersRef.current;
      const currentCoverage = coverageRef.current;
      const worker = currentWorkers.find((w) => w.id === workerId);
      if (!worker) return;

      const isRunning = worker.status === "Running";
      const newStatus = isRunning ? "Stopped" : "Running";
      const changeTime = Date.now();

      // ---- decide everything up front ----
      let nextCoverage = { ...currentCoverage };
      let activityText;
      const edgesToSpawn = [];

      if (isRunning) {
        // Its own partition, plus anything it inherited from an earlier outage.
        const inherited = Object.entries(currentCoverage)
          .filter(([, coveringId]) => coveringId === workerId)
          .map(([downId]) => Number(downId));
        const partitionsNeedingHome = [workerId, ...inherited];

        // Least-loaded healthy worker takes over.
        const target = currentWorkers
          .filter((w) => w.id !== workerId && w.status === "Running")
          .sort((a, b) => a.load - b.load)[0];

        if (target) {
          partitionsNeedingHome.forEach((pid) => {
            nextCoverage[pid] = target.id;
            edgesToSpawn.push([pid, target.id, "partition moving", "#fbbf24", "#fcd34d"]);
          });

          activityText =
            inherited.length > 0
              ? `${worker.name} went offline — its partition and ${inherited.length} inherited one(s) reassigned to ${target.name}`
              : `${worker.name} went offline — partition reassigned to ${target.name}`;
        } else {
          partitionsNeedingHome.forEach((pid) => {
            delete nextCoverage[pid];
          });
          activityText = `${worker.name} went offline — NO healthy worker available, ${partitionsNeedingHome.length} partition(s) unassigned`;
        }
      } else {
        const coveringId = currentCoverage[workerId];
        const coveringWorker = currentWorkers.find((w) => w.id === coveringId);

        if (coveringWorker) {
          edgesToSpawn.push([coveringId, workerId, "partition returning", "#22d3ee", "#67e8f9"]);
          activityText = `${worker.name} back online — reclaimed partition from ${coveringWorker.name}`;
        } else {
          activityText = `${worker.name} back online — state recovered from changelog`;
        }

        delete nextCoverage[workerId];
      }

      // ---- now commit, with no logic left inside any updater ----
      coverageRef.current = nextCoverage;
      setCoverage(nextCoverage);

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

      edgesToSpawn.forEach((args) => spawnPartitionEdge(...args));

      setActivity((previousActivity) =>
        [
          {
            id: Date.now() + Math.random(),
            type: isRunning ? "stopped" : "recovered",
            text: activityText,
            time: new Date().toLocaleTimeString(),
          },
          ...previousActivity,
        ].slice(0, 40)
      );
    },
    [spawnPartitionEdge]
  );

  useEffect(() => {
    toggleWorkerRef.current = toggleWorker;
  }, [toggleWorker]);

  // ===============================
  // DAY 15 - CHAOS MONKEY
  // ===============================

  useEffect(() => {
    if (!chaosMonkey) return;
    const delay = 4000 + Math.random() * 4000; // 4-8s
    const timeoutId = setTimeout(() => {
      const currentWorkers = workersRef.current;
      if (currentWorkers.length > 0) {
        const victim = currentWorkers[Math.floor(Math.random() * currentWorkers.length)];
        toggleWorkerRef.current?.(victim.id);
      }
      setChaosTick((t) => t + 1);
    }, delay);
    return () => clearTimeout(timeoutId);
  }, [chaosMonkey, chaosTick]);

  const toggleChaosMonkey = () => setChaosMonkey((c) => !c);

  // ===============================
  // ALERTS (derived)
  // ===============================

  const allWorkersDown = workers.every((w) => w.status !== "Running");

  const alerts = [
    ...(allWorkersDown
      ? [
          {
            id: "total-outage",
            level: "critical",
            text: "🔥 Total outage — every worker is down, no failover possible",
          },
        ]
      : []),
    ...(bottleneckWorkerId
      ? [
          {
            id: `bottleneck-${bottleneckWorkerId}`,
            level: "critical",
            text: `${worstLagWorker.name} is the throughput bottleneck (lag ${worstLagWorker.lag}ms) — investigate before it backs up the pipeline`,
          },
        ]
      : []),
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
  // DAY 17 - DERIVED CLUSTER METRICS
  // ===============================

  const currentEps = throughputHistory.length
    ? throughputHistory[throughputHistory.length - 1].eps
    : 0;

  const peakEps = throughputHistory.length
    ? Math.max(...throughputHistory.map((h) => h.eps))
    : 0;

  const avgEps = throughputHistory.length
    ? throughputHistory.reduce((sum, h) => sum + h.eps, 0) / throughputHistory.length
    : 0;

  const healthyCount = workers.filter((w) => w.status === "Running").length;

  const totalLag = workers
    .filter((w) => w.status === "Running")
    .reduce((sum, w) => sum + w.lag, 0);

  // ===============================
  // DAY 12 - FILTERED ACTIVITY
  // ===============================

  const filteredActivity = activity.filter((item) => {
    if (activityFilter === "all") return true;
    if (activityFilter === "processed") return item.type === "processed";
    if (activityFilter === "rebalance")
      return item.type === "stopped" || item.type === "recovered" || item.type === "bottleneck";
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
          <h3>Events / sec</h3>
          <p>{currentEps.toFixed(1)}</p>
        </div>
        <div className="card">
          <h3>Healthy Workers</h3>
          <p>{healthyCount}/{workers.length}</p>
        </div>
      </div>

      {alerts.length > 0 && (
        <div className="alert-stack" role="status" aria-live="polite">
          {alerts.map((alert) => (
            <div key={alert.id} className={`alert-item ${alert.level}`}>
              <span className="alert-dot" />
              {alert.text}
            </div>
          ))}
        </div>
      )}

      {/* DAY 17 - cluster throughput panel */}
      <h2 className="section-title">Cluster Throughput</h2>
      <div className="throughput-panel">
        <div className="throughput-figures">
          <div className="throughput-stat">
            <span className="throughput-label">Current</span>
            <span className="throughput-value">{currentEps.toFixed(1)} <small>ev/s</small></span>
          </div>
          <div className="throughput-stat">
            <span className="throughput-label">Average</span>
            <span className="throughput-value">{avgEps.toFixed(1)} <small>ev/s</small></span>
          </div>
          <div className="throughput-stat">
            <span className="throughput-label">Peak</span>
            <span className="throughput-value">{peakEps.toFixed(1)} <small>ev/s</small></span>
          </div>
          <div className="throughput-stat">
            <span className="throughput-label">Total lag</span>
            <span className={`throughput-value ${totalLag >= BOTTLENECK_LAG_MS ? "hot" : ""}`}>
              {totalLag} <small>ms</small>
            </span>
          </div>
        </div>
        <ThroughputChart history={throughputHistory} />
        <p className="throughput-note">
          Sampled every {PROM_SCRAPE_INTERVAL_MS / 1000}s — same cadence as a Prometheus scrape.
          Start Auto Stream to see it climb.
        </p>
      </div>

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

      <div className="chaos-controls">
        <button
          className={chaosMonkey ? "chaos-button active" : "chaos-button"}
          onClick={toggleChaosMonkey}
          aria-pressed={chaosMonkey}
        >
          {chaosMonkey ? "🐒 Stop Chaos Monkey" : "🐒 Unleash Chaos Monkey"}
        </button>
        <div className={chaosMonkey ? "chaos-status active" : "chaos-status"}>
          {chaosMonkey ? "Randomly crashing workers every 4–8s" : "Chaos Monkey idle"}
        </div>
      </div>

      <div className="worker-monitoring">
        {workers.map((worker) => {
          const isBottleneck = worker.id === bottleneckWorkerId;
          return (
            <div
              className={`monitor-card ${worker.status === "Running" ? "" : "is-down"} ${isBottleneck ? "is-bottleneck" : ""}`}
              key={worker.id}
            >
              <div className="monitor-header">
                <h3>{worker.name}</h3>
                <div className="monitor-badges">
                  <span className={worker.status === "Running" ? "monitor-status running" : "monitor-status stopped"}>
                    {worker.status === "Running" ? "🟢 Running" : "🔴 Stopped"}
                  </span>
                  {isBottleneck && <span className="bottleneck-badge">🔥 Bottleneck</span>}
                </div>
              </div>

              <div className="monitor-info">
                <p><strong>Load:</strong> {worker.load}%</p>
                <div
                  className="load-bar"
                  role="progressbar"
                  aria-valuenow={worker.load}
                  aria-valuemin={0}
                  aria-valuemax={100}
                  aria-label={`${worker.name} load`}
                >
                  <div
                    className={`load-fill ${loadClass(worker.load)}`}
                    style={{ width: `${worker.load}%` }}
                  />
                </div>

                <div className="lag-row">
                  <span className={`lag-value ${isBottleneck ? "hot" : ""}`}>
                    <strong>Lag:</strong> {worker.lag}ms
                  </span>
                  <LagSparkline history={worker.lagHistory} isBottleneck={isBottleneck} />
                </div>

                <p><strong>Messages:</strong> {worker.messages}</p>
              </div>

              <button className="worker-button" onClick={() => toggleWorker(worker.id)}>
                {worker.status === "Running" ? "Stop Worker" : "Start Worker"}
              </button>
            </div>
          );
        })}
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
        <button className="simulate-button" onClick={simulateMessage} disabled={allWorkersDown}>
          Simulate Message
        </button>
        <button className="auto-stream-button" onClick={toggleAutoStream} aria-pressed={autoStream}>
          {autoStream ? "⏹ Stop Auto Stream" : "▶ Start Auto Stream"}
        </button>
      </div>

      <p className="messages-processed">Messages processed: {messageCount}</p>

      <div className="activity-filters">
        <button
          className={activityFilter === "all" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("all")}
          aria-pressed={activityFilter === "all"}
        >
          All
        </button>
        <button
          className={activityFilter === "processed" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("processed")}
          aria-pressed={activityFilter === "processed"}
        >
          Messages
        </button>
        <button
          className={activityFilter === "rebalance" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("rebalance")}
          aria-pressed={activityFilter === "rebalance"}
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