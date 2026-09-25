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

// DAY 16 - added `lag` field: simulated consumer lag in ms (stand-in for the
// real Prometheus metric your backend teammate will eventually expose).
const initialWorkers = [
  { id: 1, name: "Worker 1", status: "Running", load: 42, messages: 12, lag: 0 },
  { id: 2, name: "Worker 2", status: "Running", load: 67, messages: 18, lag: 0 },
  { id: 3, name: "Worker 3", status: "Stopped", load: 0, messages: 0, lag: 0 },
];

// Rolling window shown on the rebalance timeline (2 minutes).
const TIMELINE_WINDOW_MS = 120000;

// DAY 16 - lag simulation tuning
const LAG_HISTORY_LENGTH = 30; // samples kept per worker for the sparkline
const BOTTLENECK_LAG_THRESHOLD = 600; // ms — above this, a worker is "the bottleneck"

// DAY 20 - default backend host, overridable from the Settings panel and
// persisted to localStorage. No protocol prefix here — http(s)/ws(s) is
// added by buildApiBase/buildWsUrl below.
const DEFAULT_BACKEND_HOST = "localhost:8000";
const BACKEND_HOST_STORAGE_KEY = "streamforge.backendHost";

function buildApiBase(host) {
  return `http://${host}`;
}

function buildWsUrl(host) {
  return `ws://${host}/ws/stream`;
}

// DAY 19 - reconnect tuning
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 16000;

// DAY 22 - small badge showing whether a field is confirmed from the
// real backend or still locally simulated. Only rendered in Live Mode.
function FieldTag({ isLive }) {
  return isLive ? (
    <span className="field-tag live" title="Confirmed by the backend">LIVE</span>
  ) : (
    <span className="field-tag sim" title="Still simulated locally — backend doesn't report this yet">SIM</span>
  );
}

function NodeLabel({ worker, isBottleneck }) {
  const isRunning = worker.status === "Running";
  return (
    <div>
      <strong>{worker.name}</strong>
      <br />
      {isRunning ? "🟢 Running" : "🔴 Stopped"}
      <br />
      Load: {worker.load}%
      {isBottleneck && (
        <>
          <br />
          <span style={{ color: "#fca5a5", fontWeight: 700 }}>⚠ BOTTLENECK</span>
        </>
      )}
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

  // DAY 13 - tracks which healthy worker is currently covering a down worker's
  // partition, so we know who to draw the "reclaim" edge back from on recovery.
  const [coverage, setCoverage] = useState({});

  // DAY 12 - per-worker up/down segments for the rebalance timeline.
  const [workerHistory, setWorkerHistory] = useState(() =>
    initialWorkers.map((w) => ({
      id: w.id,
      segments: [{ status: w.status, start: Date.now(), end: null }],
    }))
  );

  // DAY 16 - rolling lag samples per worker, feeds the sparkline.
  const [lagSamples, setLagSamples] = useState(() =>
    Object.fromEntries(initialWorkers.map((w) => [w.id, []]))
  );

  // DAY 15 - Chaos Monkey toggle + a tick to re-schedule itself.
  const [chaosMonkey, setChaosMonkey] = useState(false);
  const [chaosTick, setChaosTick] = useState(0);

  // DAY 18 - Live Mode: sync worker status from the real FastAPI backend
  // instead of the local simulation.
  const [liveMode, setLiveMode] = useState(false);

  // DAY 22 - tracks which fields per worker have actually been confirmed
  // by a real backend message, vs. still being locally simulated. Lets
  // the UI be honest about what's real instead of implying everything
  // is live just because Live Mode is on.
  const [liveFields, setLiveFields] = useState({});

  const markFieldsLive = (workerId, fields) => {
    setLiveFields((prev) => ({
      ...prev,
      [workerId]: { ...(prev[workerId] || {}), ...Object.fromEntries(fields.map((f) => [f, true])) },
    }));
  };
  const [connectionStatus, setConnectionStatus] = useState("idle"); // idle | connecting | connected | reconnecting | disconnected | error
  const wsRef = useRef(null);

  // DAY 19 - reconnect bookkeeping
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [retryNonce, setRetryNonce] = useState(0);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef(null);

  // DAY 20 - configurable backend host (persisted), so this isn't hardcoded
  // to localhost once the backend runs somewhere else.
  const [backendHost, setBackendHost] = useState(() => {
    try {
      return localStorage.getItem(BACKEND_HOST_STORAGE_KEY) || DEFAULT_BACKEND_HOST;
    } catch {
      return DEFAULT_BACKEND_HOST;
    }
  });
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [hostDraft, setHostDraft] = useState(backendHost);
  const [hostSaveNotice, setHostSaveNotice] = useState("");

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
  // DAY 16 - DERIVED BOTTLENECK (not stored state)
  // ===============================

  const runningWorkers = workers.filter((w) => w.status === "Running");
  const worstLagWorker =
    runningWorkers.length > 0
      ? runningWorkers.reduce((worst, w) => (w.lag > (worst?.lag ?? -1) ? w : worst), null)
      : null;
  const activeBottleneck =
    worstLagWorker && worstLagWorker.lag >= BOTTLENECK_LAG_THRESHOLD ? worstLagWorker : null;

  // DAY 16 - log bottleneck start/clear transitions exactly once each way.
  const prevBottleneckIdRef = useRef(null);
  useEffect(() => {
    const currentId = activeBottleneck ? activeBottleneck.id : null;
    if (currentId !== prevBottleneckIdRef.current) {
      if (currentId) {
        setActivity((previousActivity) =>
          [
            {
              id: Date.now() + Math.random(),
              type: "bottleneck",
              text: `⚠️ ${activeBottleneck.name} is now the bottleneck (${activeBottleneck.lag}ms consumer lag)`,
              time: new Date().toLocaleTimeString(),
            },
            ...previousActivity,
          ].slice(0, 40)
        );
      } else if (prevBottleneckIdRef.current) {
        setActivity((previousActivity) =>
          [
            {
              id: Date.now() + Math.random(),
              type: "bottleneck-clear",
              text: "Bottleneck cleared — lag back to normal across workers",
              time: new Date().toLocaleTimeString(),
            },
            ...previousActivity,
          ].slice(0, 40)
        );
      }
      prevBottleneckIdRef.current = currentId;
    }
  }, [activeBottleneck?.id, activeBottleneck?.name, activeBottleneck?.lag]);

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
        // DAY 16 - lag-based bottleneck outranks the plain load-based "hot" state.
        const isBottleneck = isRunning && activeBottleneck?.id === worker.id;

        const borderColor = !isRunning
          ? "#ef4444"
          : isBottleneck
          ? "#f43f5e"
          : isHot
          ? "#f59e0b"
          : "#22d3ee";
        const glow = !isRunning
          ? "none"
          : isBottleneck
          ? "0 0 20px rgba(244,63,94,0.75)"
          : isHot
          ? "0 0 14px rgba(245,158,11,0.55)"
          : "0 0 10px rgba(34,211,238,0.35)";

        return {
          ...node,
          data: { label: <NodeLabel worker={worker} isBottleneck={isBottleneck} /> },
          style: {
            ...node.style,
            border: `2px solid ${borderColor}`,
            opacity: isRunning ? 1 : 0.5,
            boxShadow: glow,
            animation: isBottleneck ? "pulseBottleneck 1.2s ease-in-out infinite" : "none",
            transition: "all 0.4s ease",
          },
        };
      })
    );
  }, [workers, setNodes, activeBottleneck]);

  // ===============================
  // REFLECT WORKER STATE ONTO EDGES
  // ===============================

  useEffect(() => {
    setEdges((currentEdges) =>
      currentEdges.map((edge) => {
        // DAY 13 - never touch temporary migration/reclaim edges here.
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
    const runningNow = currentWorkers.filter((w) => w.status === "Running");
    if (runningNow.length === 0) return;

    const selectedWorker = runningNow[Math.floor(Math.random() * runningNow.length)];
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
  // WORKER LOAD RECOVERY + DAY 16 LAG SIMULATION
  // (merged into one interval — both are per-worker "physics" ticks)
  // ===============================

  useEffect(() => {
    const recoveryInterval = setInterval(() => {
      setWorkers((previousWorkers) => {
        const updated = previousWorkers.map((worker) => {
          if (worker.status !== "Running") {
            return { ...worker, load: 0, lag: 0 };
          }

          const newLoad = Math.max(0, worker.load - 2);

          // DAY 16 - lag rises under sustained high load, drains when idle.
          let lagDelta;
          if (worker.load >= 70) {
            lagDelta = 40 + Math.random() * 80;
          } else if (worker.load >= 40) {
            lagDelta = (Math.random() - 0.5) * 20;
          } else {
            lagDelta = -(30 + Math.random() * 30);
          }
          const newLag = Math.max(0, Math.round(worker.lag + lagDelta));

          return { ...worker, load: newLoad, lag: newLag };
        });

        // DAY 16 - append this tick's lag value to each worker's sparkline history.
        setLagSamples((previousSamples) => {
          const next = { ...previousSamples };
          updated.forEach((worker) => {
            const arr = next[worker.id] ? [...next[worker.id]] : [];
            arr.push(worker.lag);
            if (arr.length > LAG_HISTORY_LENGTH) arr.shift();
            next[worker.id] = arr;
          });
          return next;
        });

        return updated;
      });
    }, 2000);
    return () => clearInterval(recoveryInterval);
  }, []);

  const toggleAutoStream = () => setAutoStream((current) => !current);

  // ===============================
  // DAY 18/19 - LIVE MODE: connect to the real backend, with auto-reconnect
  // ===============================

  useEffect(() => {
    if (!liveMode) {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      reconnectAttemptsRef.current = 0;
      setReconnectAttempt(0);
      setConnectionStatus("idle");
      setLiveFields({}); // DAY 22 - back to demo, nothing is confirmed-live anymore
      return;
    }

    let cancelled = false;
    reconnectAttemptsRef.current = 0;
    setReconnectAttempt(0);

    // DAY 20 - resolve URLs from the configurable host at connect time,
    // not from a hardcoded constant.
    const apiBase = buildApiBase(backendHost);
    const wsUrl = buildWsUrl(backendHost);

    // Pull real status from the backend on entry, but keep our simulated
    // load/lag fields so the dashboard doesn't flatten to zero — the
    // backend doesn't emit those yet (that's a later day).
    fetch(`${apiBase}/api/workers`)
      .then((res) => res.json())
      .then((backendWorkers) => {
        if (cancelled) return;
        setWorkers((previousWorkers) =>
          previousWorkers.map((w) => {
            const match = backendWorkers.find((bw) => bw.id === w.id);
            return match ? { ...w, status: match.status } : w;
          })
        );
      })
      .catch(() => {
        if (!cancelled) setConnectionStatus("error");
      });

    const logEvent = (type, text) => {
      setActivity((previousActivity) =>
        [
          { id: Date.now() + Math.random(), type, text, time: new Date().toLocaleTimeString() },
          ...previousActivity,
        ].slice(0, 40)
      );
    };

    const connect = () => {
      if (cancelled) return;
      setConnectionStatus(reconnectAttemptsRef.current === 0 ? "connecting" : "reconnecting");

      const ws = new WebSocket(wsUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        if (cancelled) return;
        if (reconnectAttemptsRef.current > 0) {
          logEvent("recovered", "[backend] Reconnected to backend WebSocket");
        }
        reconnectAttemptsRef.current = 0;
        setReconnectAttempt(0);
        setConnectionStatus("connected");
      };

      ws.onmessage = (messageEvent) => {
        let data;
        try {
          data = JSON.parse(messageEvent.data);
        } catch {
          return;
        }

        if (data.type === "worker_status") {
          setWorkers((previousWorkers) =>
            previousWorkers.map((w) =>
              w.id !== data.workerId
                ? w
                : {
                    ...w,
                    status: data.status,
                    load: data.status === "Stopped" ? 0 : w.load || Math.floor(Math.random() * 50) + 30,
                    lag: data.status === "Stopped" ? 0 : w.lag,
                  }
            )
          );
          markFieldsLive(data.workerId, ["status"]);
          logEvent(
            data.status === "Stopped" ? "stopped" : "recovered",
            `[backend] Worker ${data.workerId} is now ${data.status}`
          );
        }

        // DAY 22 - forward-compatible: the Day 8 backend plan will
        // eventually push real load/lag/message-count readings from the
        // actual Kafka/Bytewax pipeline. This handler is ready for that
        // now, so no further frontend changes are needed when it ships.
        if (data.type === "worker_metrics") {
          setWorkers((previousWorkers) =>
            previousWorkers.map((w) =>
              w.id !== data.workerId
                ? w
                : {
                    ...w,
                    load: data.load ?? w.load,
                    lag: data.lag ?? w.lag,
                    messages: data.messages ?? w.messages,
                  }
            )
          );
          markFieldsLive(
            data.workerId,
            [
              data.load !== undefined ? "load" : null,
              data.lag !== undefined ? "lag" : null,
              data.messages !== undefined ? "messages" : null,
            ].filter(Boolean)
          );
        }
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (cancelled) return;

        if (reconnectAttemptsRef.current >= MAX_RECONNECT_ATTEMPTS) {
          setConnectionStatus("error");
          logEvent(
            "stopped",
            `[backend] Gave up reconnecting after ${MAX_RECONNECT_ATTEMPTS} attempts — falling back to Demo Mode`
          );
          setLiveMode(false);
          return;
        }

        if (reconnectAttemptsRef.current === 0) {
          logEvent("stopped", "[backend] Lost connection to backend — attempting to reconnect…");
        }

        const attempt = reconnectAttemptsRef.current + 1;
        reconnectAttemptsRef.current = attempt;
        setReconnectAttempt(attempt);
        setConnectionStatus("reconnecting");

        const delay = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** (attempt - 1), RECONNECT_MAX_DELAY_MS);
        reconnectTimeoutRef.current = setTimeout(connect, delay);
      };

      ws.onerror = () => {
        // onclose fires right after this and handles retry scheduling.
      };
    };

    connect();

    return () => {
      cancelled = true;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [liveMode, retryNonce, backendHost]);

  const toggleLiveMode = () => setLiveMode((v) => !v);

  // DAY 20 - save a new backend host from the Settings panel. Applies on
  // the next connect (or immediately if already live, by forcing a retry).
  const saveBackendHost = () => {
    const trimmed = hostDraft.trim();
    if (!trimmed) {
      setHostSaveNotice("Host can't be empty.");
      return;
    }
    setBackendHost(trimmed);
    try {
      localStorage.setItem(BACKEND_HOST_STORAGE_KEY, trimmed);
    } catch {
      // localStorage unavailable (private browsing, etc.) — non-fatal,
      // the host still applies for this session.
    }
    setHostSaveNotice("Saved.");
    setTimeout(() => setHostSaveNotice(""), 2000);
    if (liveMode) {
      setRetryNonce((n) => n + 1); // force immediate reconnect to new host
    }
  };

  const resetBackendHost = () => {
    setHostDraft(DEFAULT_BACKEND_HOST);
  };
  const retryConnectionNow = () => setRetryNonce((n) => n + 1);

  // ===============================
  // TOGGLE WORKER (+ event log)
  // DAY 15 - least-loaded failover target + cascading reassignment for
  // partitions the worker had inherited from an earlier outage.
  // ===============================

  const toggleWorker = useCallback(
    (workerId) => {
      const currentWorkers = workersRef.current;
      const worker = currentWorkers.find((w) => w.id === workerId);
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
                lag: isRunning ? 0 : w.lag,
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

      // Shared helper for both migrate and reclaim edges.
      const spawnEdge = (sourceId, targetId, label, stroke, labelColor) => {
        const prefix = label === "partition moving" ? "migrate" : "reclaim";
        const edgeId = `${prefix}-${sourceId}-${targetId}-${changeTime}-${Math.random()
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
      };

      let activityText;

      setCoverage((prevCoverage) => {
        let nextCoverage = prevCoverage;

        if (isRunning) {
          // Worker going DOWN — collect its own partition plus anything it
          // had inherited from an earlier outage.
          const inherited = Object.entries(prevCoverage)
            .filter(([, coveringId]) => coveringId === workerId)
            .map(([downId]) => Number(downId));
          const partitionsNeedingHome = [workerId, ...inherited];

          // Pick the LEAST LOADED healthy worker, not just the first match.
          const target = currentWorkers
            .filter((w) => w.id !== workerId && w.status === "Running")
            .sort((a, b) => a.load - b.load)[0];

          if (target) {
            nextCoverage = { ...prevCoverage };
            partitionsNeedingHome.forEach((pid) => {
              nextCoverage[pid] = target.id;
            });

            partitionsNeedingHome.forEach((pid) =>
              spawnEdge(pid, target.id, "partition moving", "#fbbf24", "#fcd34d")
            );

            activityText =
              inherited.length > 0
                ? `${worker.name} went offline — its partition and ${inherited.length} inherited one(s) reassigned to ${target.name}`
                : `${worker.name} went offline — partition reassigned to ${target.name}`;
          } else {
            // Total outage: nobody left to take the partition(s).
            nextCoverage = { ...prevCoverage };
            partitionsNeedingHome.forEach((pid) => delete nextCoverage[pid]);
            activityText = `${worker.name} went offline — NO healthy worker available, ${partitionsNeedingHome.length} partition(s) unassigned`;
          }
        } else {
          // Worker coming back UP — reclaim only its OWN partition.
          const coveringId = prevCoverage[workerId];
          const coveringWorker = currentWorkers.find((w) => w.id === coveringId);

          if (coveringWorker) {
            spawnEdge(coveringId, workerId, "partition returning", "#22d3ee", "#67e8f9");
            nextCoverage = { ...prevCoverage };
            delete nextCoverage[workerId];
            activityText = `${worker.name} back online — reclaimed partition from ${coveringWorker.name}`;
          } else {
            activityText = `${worker.name} back online — state recovered from changelog`;
          }
        }

        return nextCoverage;
      });

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

      // DAY 18 - keep the real backend in sync when Live Mode is on. The
      // WebSocket broadcast that comes back just confirms what we already
      // set locally, so there's no double-toggle.
      if (liveMode) {
        fetch(`${buildApiBase(backendHost)}/api/workers/${workerId}/toggle`, { method: "POST" }).catch(() => {
          setActivity((previousActivity) =>
            [
              {
                id: Date.now() + Math.random(),
                type: "stopped",
                text: `[backend] Could not reach backend to toggle Worker ${workerId}`,
                time: new Date().toLocaleTimeString(),
              },
              ...previousActivity,
            ].slice(0, 40)
          );
        });
      }
    },
    [setEdges, liveMode, backendHost]
  );

  // DAY 15 - Chaos Monkey: randomly flips a worker's state every 4-8s.
  const toggleWorkerRef = useRef(toggleWorker);
  useEffect(() => {
    toggleWorkerRef.current = toggleWorker;
  }, [toggleWorker]);

  useEffect(() => {
    // DAY 18 - Chaos Monkey stays off while Live Mode owns worker state.
    if (!chaosMonkey || liveMode) return;
    const delay = 4000 + Math.random() * 4000;
    const timeoutId = setTimeout(() => {
      const currentWorkers = workersRef.current;
      if (currentWorkers.length > 0) {
        const victim = currentWorkers[Math.floor(Math.random() * currentWorkers.length)];
        toggleWorkerRef.current?.(victim.id);
      }
      setChaosTick((t) => t + 1);
    }, delay);
    return () => clearTimeout(timeoutId);
  }, [chaosMonkey, liveMode, chaosTick]);

  const toggleChaosMonkey = () => setChaosMonkey((c) => !c);

  // ===============================
  // ALERTS (derived, not stored state)
  // ===============================

  // DAY 15 - true when there's nobody left to fail over to.
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
    ...(activeBottleneck
      ? [
          {
            id: `bottleneck-${activeBottleneck.id}`,
            level: "warning",
            text: `${activeBottleneck.name} is the processing bottleneck — ${activeBottleneck.lag}ms consumer lag`,
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
  // DAY 12 - FILTERED ACTIVITY
  // ===============================

  const filteredActivity = activity.filter((item) => {
    if (activityFilter === "all") return true;
    if (activityFilter === "processed") return item.type === "processed";
    if (activityFilter === "rebalance") return item.type === "stopped" || item.type === "recovered";
    if (activityFilter === "bottleneck") return item.type === "bottleneck" || item.type === "bottleneck-clear";
    return true;
  });

  // ===============================
  // DAY 21 - EXPORTS (activity log + full snapshot)
  // ===============================

  const downloadBlob = (content, filename, mimeType) => {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const csvEscape = (value) => {
    const str = String(value ?? "");
    return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };

  const exportActivityCsv = () => {
    const rows = [
      ["time", "type", "text"],
      ...filteredActivity.map((item) => [item.time, item.type, item.text]),
    ];
    const csv = rows.map((row) => row.map(csvEscape).join(",")).join("\n");
    downloadBlob(csv, `streamforge-activity-${Date.now()}.csv`, "text/csv");
  };

  const exportActivityJson = () => {
    downloadBlob(
      JSON.stringify(filteredActivity, null, 2),
      `streamforge-activity-${Date.now()}.json`,
      "application/json"
    );
  };

  const exportSystemSnapshot = () => {
    const snapshot = {
      exportedAt: new Date().toISOString(),
      mode: liveMode ? "live" : "demo",
      connectionStatus: liveMode ? connectionStatus : null,
      messageCount,
      workers,
      lagSamples,
      activeBottleneck: activeBottleneck ? { id: activeBottleneck.id, name: activeBottleneck.name, lag: activeBottleneck.lag } : null,
      workerHistory,
      activityLog: activity,
    };
    downloadBlob(
      JSON.stringify(snapshot, null, 2),
      `streamforge-snapshot-${Date.now()}.json`,
      "application/json"
    );
  };

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

      {/* DAY 18 - Live Mode: switch between simulated data and the real backend */}
      <div className="mode-controls">
        <button
          className={liveMode ? "mode-button live" : "mode-button"}
          onClick={toggleLiveMode}
        >
          {liveMode ? "🔌 Live Mode (Backend)" : "🧪 Demo Mode (Simulated)"}
        </button>
        {liveMode && (
          <span className={`connection-badge ${connectionStatus}`}>
            {connectionStatus === "connecting" && "Connecting…"}
            {connectionStatus === "connected" && "● Connected to backend"}
            {connectionStatus === "reconnecting" &&
              `Reconnecting… (attempt ${reconnectAttempt}/${MAX_RECONNECT_ATTEMPTS})`}
            {connectionStatus === "disconnected" && "○ Disconnected"}
            {connectionStatus === "error" && "⚠ Backend unreachable — check FastAPI is running on :8000"}
          </span>
        )}
        {liveMode && (connectionStatus === "error" || connectionStatus === "disconnected") && (
          <button className="retry-button" onClick={retryConnectionNow}>
            Retry now
          </button>
        )}

        {/* DAY 20 - backend host settings */}
        <button
          className="settings-button"
          onClick={() => {
            setHostDraft(backendHost);
            setHostSaveNotice("");
            setSettingsOpen((open) => !open);
          }}
          title="Configure backend address"
        >
          ⚙ {backendHost}
        </button>
      </div>

      {/* DAY 22 - explain the LIVE/SIM tags before they show up on cards */}
      {liveMode && (
        <div className="field-legend">
          <span className="field-tag live">LIVE</span> confirmed by the backend right now ·{" "}
          <span className="field-tag sim">SIM</span> still simulated locally (backend doesn't report this yet — see Day 8 plan)
        </div>
      )}

      {settingsOpen && (
        <div className="settings-panel">
          <label htmlFor="backend-host-input">Backend host</label>
          <div className="settings-row">
            <span className="settings-prefix">http(s)://</span>
            <input
              id="backend-host-input"
              type="text"
              className="settings-input"
              value={hostDraft}
              onChange={(e) => setHostDraft(e.target.value)}
              placeholder="localhost:8000"
              spellCheck={false}
            />
          </div>
          <p className="settings-hint">
            Used for both the REST API and the WebSocket stream. Change this once your
            friend's backend is running somewhere other than your own machine.
          </p>
          <div className="settings-actions">
            <button className="settings-save" onClick={saveBackendHost}>
              Save{liveMode ? " & Reconnect" : ""}
            </button>
            <button className="settings-reset" onClick={resetBackendHost}>
              Reset to default
            </button>
            {hostSaveNotice && <span className="settings-notice">{hostSaveNotice}</span>}
          </div>
        </div>
      )}

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

      {/* DAY 15 - Chaos Monkey controls */}
      <div className="chaos-controls">
        <button
          className={chaosMonkey ? "chaos-button active" : "chaos-button"}
          onClick={toggleChaosMonkey}
          disabled={liveMode}
        >
          {chaosMonkey ? "🐒 Stop Chaos Monkey" : "🐒 Unleash Chaos Monkey"}
        </button>
        <div className={chaosMonkey ? "chaos-status active" : "chaos-status"}>
          {liveMode
            ? "Disabled in Live Mode — backend owns worker state"
            : chaosMonkey
            ? "Randomly crashing workers every 4–8s"
            : "Chaos Monkey idle"}
        </div>
      </div>

      <div className="worker-monitoring">
        {workers.map((worker) => {
          const isBottleneck = activeBottleneck?.id === worker.id;
          const samples = lagSamples[worker.id] || [];
          return (
            <div className={`monitor-card ${worker.status === "Running" ? "" : "is-down"}`} key={worker.id}>
              <div className="monitor-header">
                <h3>{worker.name}</h3>
                <span className={worker.status === "Running" ? "monitor-status running" : "monitor-status stopped"}>
                  {worker.status === "Running" ? "🟢 Running" : "🔴 Stopped"}
                </span>
                {liveMode && <FieldTag isLive={!!liveFields[worker.id]?.status} />}
              </div>

              <div className="monitor-info">
                <p>
                  <strong>Load:</strong> {worker.load}%
                  {liveMode && <FieldTag isLive={!!liveFields[worker.id]?.load} />}
                </p>
                <div className="load-bar">
                  <div
                    className={`load-fill ${loadClass(worker.load)}`}
                    style={{ width: `${worker.load}%` }}
                  />
                </div>
                <p>
                  <strong>Messages:</strong> {worker.messages}
                  {liveMode && <FieldTag isLive={!!liveFields[worker.id]?.messages} />}
                </p>

                {/* DAY 16 - consumer lag readout + sparkline */}
                <p>
                  <strong>Lag:</strong> {worker.lag}ms
                  {liveMode && <FieldTag isLive={!!liveFields[worker.id]?.lag} />}
                  {isBottleneck && <span className="bottleneck-badge">BOTTLENECK</span>}
                </p>
                <div className="lag-sparkline">
                  {samples.length === 0 ? (
                    <span className="lag-sparkline-empty">no data yet</span>
                  ) : (
                    samples.map((v, i) => (
                      <div
                        key={i}
                        className={`lag-bar ${v >= BOTTLENECK_LAG_THRESHOLD ? "critical" : v >= 300 ? "warn" : ""}`}
                        style={{ height: `${Math.max(4, Math.min(100, (v / 1000) * 100))}%` }}
                      />
                    ))
                  )}
                </div>
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
        <button
          className={activityFilter === "bottleneck" ? "filter-tab active" : "filter-tab"}
          onClick={() => setActivityFilter("bottleneck")}
        >
          Bottlenecks
        </button>

        {/* DAY 21 - export the currently filtered log */}
        <span className="export-group">
          <button
            className="export-button"
            onClick={exportActivityCsv}
            disabled={filteredActivity.length === 0}
            title="Export the filtered activity log as CSV"
          >
            ⬇ CSV
          </button>
          <button
            className="export-button"
            onClick={exportActivityJson}
            disabled={filteredActivity.length === 0}
            title="Export the filtered activity log as JSON"
          >
            ⬇ JSON
          </button>
        </span>
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

      {/* DAY 21 - full state snapshot, handy right after a chaos test run */}
      <button className="snapshot-button" onClick={exportSystemSnapshot}>
        📋 Export Full System Snapshot (JSON)
      </button>
    </div>
  );
}

export default App;