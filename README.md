# Music PubSub

A distributed publish-subscribe system for real-time music event notifications. Brokers replicate events via a gossip protocol, detect failures with heartbeats, and push notifications to users over WebSocket.

## Prerequisites:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Engine and Docker Compose)
- A web browser (Chrome, Firefox, Safari, etc.)

## Setup and Run:

### Step 1: Clone the repository

### Step 2: Give Docker access to your path
1. Open Docker Desktop.
2. Go to Settings -> Resources -> File Sharing.
3. Add full path of project to the list.
4. Click Apply & Restart.

### Step 3: Start the cluster

This builds the Docker image, starts PostgreSQL, the seed broker, and two peer brokers:

```docker compose up --build --scale broker=2```

Wait until you see log lines like:

```
seed-broker  | Server running on https://0.0.0.0:5000
broker-1     | Successfully joined cluster via seed broker
broker-2     | Successfully joined cluster via seed broker
```

### Step 4: Trust the self-signed certificate

Your browser will show a certificate warning because the system uses self-signed TLS certificates. 

Open your browser and go to:

```
https://localhost:5001/health
```
1. Click on Advanced.
2. Click on Proceed to localhost.

### Step 5: Open the frontend

Open a new terminal. Change directory to the frontend folder. 
Run: ```npx serve .```
Open the browser at ```http://localhost:3000/index.html```

Alternatively, you can open the browser and open the static index.html file, by putting file://path-to-music-pubsub/Music-PubSub/frontend/index.html in the browser's address bar.

From here you can:

1. **Sign up** — create a new account
2. **Log in** — authenticate and receive a JWT token
3. **Subscribe** — subscribe to events by city/state, optionally filtered by genre or artist, within a date range
4. **Publish** — publish a music event (must match at least one subscription to trigger a notification)
5. **Receive notifications** — the subscribe page maintains a WebSocket connection and displays notifications in real time
6. **Trending** - review Trending genres, artists, or locations

### Step 6: Run the failure test cases
1. Open a new terminal. 
2. Navigate to the main folder: Music-PubSub. 
3. Run the failure test cases: ```bash test-network-partition.sh```. This tests network partition, seed broker failure, and broker failure.
4. Review the results. 

### Step 7: Run the performance tests
1. Open a new terminal. 
2. Navigate to the main folder: Music-PubSub. 
3. Run the performance test cases: ```bash test-performance.sh```.
4. Review the results. 

### Step 8: Shut down

```docker compose down -v```

The `-v` flag removes the PostgreSQL data volume so the database starts fresh next time.

### Step 9: Review our deployed product 
Our project was deployed to AWS EC2. 
1. You can access the deployed version on ```https://3.21.240.227/``` in the browser.
2. Click on Advanced.
3. Click on Proceed to localhost.
4. Now you can create a new account and use the frontend as described in Step 5. 

## Scaling

To run with more brokers, change the number after `--scale broker=`:

```docker compose up --build --scale broker=5```

## Division of Work

### Srivatsa Puranam

- Docker and infrastructure setup (`Dockerfile`, `docker-compose.yml`, Docker networking)
- Main application entry point (`app.js`) — service initialization, dependency wiring, route registration, startup sequence
- Dynamic cluster discovery (`BrokerRegistry.js`) — seed-based peer registration, peer announcement, dead-peer cleanup
- Gossip queue aging and hop-count pruning logic in `GossipService.js`
- Mobile agent chaining and route implementation (`agents.js`, `MobileAgent.js`)
- Heartbeat debugging and consecutive-miss threshold tuning
- Performance metrics system (`MetricsCollector.js`) — latency tracking, throughput windows, reliability counters
- Performance test script (`test-performance.sh`) on the `Final_Phase-Finishing-Touches` branch
- Broker IP resolution fix (`os.networkInterfaces()` instead of `os.hostname()`)

### Smruthi Danda

- Core broker services — initial implementation of `HeartbeatService.js`, `GossipService.js`, `lamportClock.js`
- Database layer (`db.js`, `CreateTables.sql`)
- Mobile agent model and initial agent logic (`MobileAgent.js`, `Message.js`)
- WebSocket server and real-time notification delivery (`websocket.js`)
- Broker authentication middleware (`brokerAuth.js`) and shared-secret verification
- TLS encryption setup — certificate generation in `Dockerfile`, HTTPS server creation
- User authentication — JWT middleware (`auth.js`), login/register routes
- Frontend — all HTML pages (`index.html`, `login.html`, `signup.html`, `subscribe.html`, `publish.html`), `api.js`
- Subscription and event routes (`subscriptions.js`, `events.js`)
- Failure scenario test scripts (`test-network-partition.sh`) on the `Final_Phase-Finishing-Touches` branch