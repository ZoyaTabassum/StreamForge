import { useState } from "react";

function App() {
  const [messages, setMessages] = useState(0);

  const handleSimulateMessage = () => {
    setMessages(messages + 1);
  };

  return (
    <div className="app">
      <h1>StreamForge</h1>

      <p className="subtitle">
        Real-Time Kafka Streaming Dashboard
      </p>

      <div className="status">
        <h3>System Online</h3>
      </div>

      <div className="stats">
        <div className="stat-card">
          <h3>Messages</h3>
          <p>{messages}</p>
        </div>

        <div className="stat-card">
          <h3>Producers</h3>
          <p>1</p>
        </div>

        <div className="stat-card">
          <h3>Consumers</h3>
          <p>1</p>
        </div>

        <div className="stat-card">
          <h3>Status</h3>
          <p>Running</p>
        </div>
      </div>

      <div className="card">
        <h2>Kafka Stream Topology</h2>

        <div className="topology">
          <div className="node">
            <h3>Producer</h3>
            <p>Message Source</p>
          </div>

          <span>→</span>

          <div className="node">
            <h3>Kafka</h3>
            <p>Stream Processing</p>
          </div>

          <span>→</span>

          <div className="node">
            <h3>Consumer</h3>
            <p>Message Receiver</p>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Stream Activity</h2>

        <button onClick={handleSimulateMessage}>
          Simulate Message
        </button>

        <p className="activity">
          Messages received: {messages}
        </p>
      </div>
    </div>
  );
}

export default App;