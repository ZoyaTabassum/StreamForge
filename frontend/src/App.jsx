import "./index.css";

function App() {
  return (
    <div className="dashboard">

      <header className="dashboard-header">
        <h1>Real-Time Kafka Streaming Dashboard</h1>
        <p className="status">● System Online</p>
      </header>

      <section className="stats">

        <div className="stat-card">
          <h3>Messages</h3>
          <p>0</p>
        </div>

        <div className="stat-card">
          <h3>Producers</h3>
          <p>1</p>
        </div>

        <div className="stat-card">
          <h3>Consumers</h3>
          <p>1</p>
        </div>

      </section>

      <section className="topology-section">
        <h2>Kafka Stream Topology</h2>

        <div className="topology-container">
          {/* Your React Flow component goes here */}
        </div>
      </section>

      <section className="activity-section">
        <h2>Stream Activity</h2>

        <button>Simulate Message</button>

        <p>Messages received: 0</p>
      </section>

    </div>
  );
}

export default App;