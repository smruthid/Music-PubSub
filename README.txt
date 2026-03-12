# Music PubSub

A distributed publish-subscribe system for real-time music event notifications. Brokers replicate events via a gossip protocol, detect failures with heartbeats, and push notifications to users over WebSocket.

## Prerequisites:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Engine and Docker Compose)
- A web browser (Chrome, Firefox, Safari, etc.)

## Setup and Run:

### Step 1: Clone the repository

### Step 2: Start the cluster

This builds the Docker image, starts PostgreSQL, the seed broker, and two peer brokers:

```bash
docker compose up --build --scale broker=2
```

Wait until you see log lines like:

```
seed-broker  | Server running on https://0.0.0.0:5000
broker-1     | Successfully joined cluster via seed broker
broker-2     | Successfully joined cluster via seed broker
```

### Step 3: Open the frontend

Open your browser and go to:

```
https://localhost:5001/index.html
```
This project is deployed using AWS.
Your browser will show a certificate warning because the system uses self-signed TLS certificates. Click **Advanced** → **Proceed to localhost** (or equivalent) to continue.

From here you can:

1. **Sign up** — create a new account
2. **Log in** — authenticate and receive a JWT token
3. **Subscribe** — subscribe to events by city/state, optionally filtered by genre or artist, within a date range
4. **Publish** — publish a music event (must match at least one subscription to trigger a notification)
5. **Receive notifications** — the subscribe page maintains a WebSocket connection and displays notifications in real time

### Step 4: Shut down

```bash
docker compose down -v
```

The `-v` flag removes the PostgreSQL data volume so the database starts fresh next time.

## Scaling

To run with more brokers, change the number after `--scale broker=`:

```bash
docker compose up --build --scale broker=5
```

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