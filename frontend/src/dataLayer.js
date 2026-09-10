// ===============================================================
// DAY 14 — DATA LAYER SCAFFOLD
// ===============================================================
//
// This file is NOT imported into App.jsx yet. Your current dashboard
// still runs entirely on the mock simulateMessage/toggleWorker logic
// inside App.jsx, and that keeps working exactly as-is.
//
// This file exists so that when your friend's backend is ready, the
// swap is: import these three functions into App.jsx and call them
// instead of the mock logic — the shapes match api-contract.md exactly,
// so the rest of your component (nodes, edges, timeline, alerts) needs
// zero changes.
//
// See api-contract.md for the endpoint/event shapes these assume.

const API_BASE = "http://localhost:8000"; // adjust once backend is deployed

/**
 * Fetch the initial worker list on dashboard load.
 * Matches: GET /api/workers -> Worker[]
 */
export async function fetchWorkers() {
  const response = await fetch(`${API_BASE}/api/workers`);
  if (!response.ok) {
    throw new Error(`Failed to fetch workers: ${response.status}`);
  }
  return response.json();
}

/**
 * Ask the backend to stop/start a worker for real.
 * Matches: POST /api/workers/:id/toggle -> updated Worker
 */
export async function toggleWorkerRemote(workerId) {
  const response = await fetch(`${API_BASE}/api/workers/${workerId}/toggle`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`Failed to toggle worker ${workerId}: ${response.status}`);
  }
  return response.json();
}

/**
 * Open the live event stream. Calls onEvent(event) for every message
 * the backend pushes. Returns a cleanup function to close the socket.
 *
 * Expected event shapes (see api-contract.md):
 *   { type: "message_processed", workerId, loadDelta, timestamp }
 *   { type: "worker_status", workerId, status, timestamp }
 */
export function subscribeToStream(onEvent, onError) {
  const socket = new WebSocket(`${API_BASE.replace("http", "ws")}/ws/stream`);

  socket.onmessage = (rawEvent) => {
    try {
      const parsed = JSON.parse(rawEvent.data);
      onEvent(parsed);
    } catch (err) {
      console.error("Malformed stream event:", rawEvent.data, err);
    }
  };

  socket.onerror = (err) => {
    if (onError) onError(err);
  };

  return () => socket.close();
}

// ---------------------------------------------------------------
// HOW THIS WILL PLUG INTO App.jsx LATER (reference only, not run):
//
//   useEffect(() => {
//     fetchWorkers().then(setWorkers).catch(console.error);
//     const unsubscribe = subscribeToStream((event) => {
//       if (event.type === "message_processed") {
//         setWorkers((prev) => prev.map((w) =>
//           w.id === event.workerId
//             ? { ...w, messages: w.messages + 1, load: Math.min(100, w.load + event.loadDelta) }
//             : w
//         ));
//         setMessageCount((c) => c + 1);
//       }
//       if (event.type === "worker_status") {
//         // reuse the same setWorkers/setWorkerHistory/setActivity logic
//         // that toggleWorker already does locally today
//       }
//     });
//     return unsubscribe;
//   }, []);
//
//   // toggleWorker's button onClick would call toggleWorkerRemote(id)
//   // instead of updating local state directly — the worker_status
//   // event coming back over the socket is what actually updates the UI.
// ---------------------------------------------------------------