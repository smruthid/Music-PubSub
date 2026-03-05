/**
 * Unified Music PubSub Application
 * Combines broker replication endpoints with user-facing API routes.
 * Supports dynamic cluster scaling via seed-broker discovery.
 */

require('dotenv').config();

const os = require('os');
const express = require('express');
const fs = require('fs');
const https = require('https');
const pool = require('./db');
const LamportClock = require('./src/utils/lamportClock');
const BrokerRegistry = require('./src/services/BrokerRegistry');
const HeartbeatService = require('./src/services/HeartbeatService');
const GossipService = require('./src/services/GossipService');
const HeartbeatController = require('./src/controllers/HeartbeatController');
const ReplicationController = require('./src/controllers/ReplicationController');
const { setupWebSocket, sendNotification } = require('./websocket');

const authenticateBroker = require('./middleware/brokerAuth');
const authRoutes = require('./routes/auth');
const subscriptionRouteFactory = require('./routes/subscriptions');
const eventRouteFactory = require('./routes/events');
const agentRouteFactory = require('./routes/agents');

const app = express();
app.use(express.json());

// Allow the vanilla frontend (file:// or local dev server) to reach the API
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ==================== Broker Configuration ====================
const BROKER_PORT = parseInt(process.env.BROKER_PORT, 10) || 5000;
const PORT = parseInt(process.env.PORT, 10) || BROKER_PORT;

// Auto-generate a unique broker ID from the hostname when not explicitly set.
// Docker Compose gives each scaled container a unique hostname.
const BROKER_ID = process.env.BROKER_ID || `broker-${os.hostname()}`;

// Seed broker for dynamic discovery (non-seed brokers use this to join the cluster)
const SEED_BROKER = process.env.SEED_BROKER || null;
const IS_SEED = process.env.IS_SEED === 'true';

// Legacy support: if PEER_BROKERS is provided, parse it as the initial peer list
const INITIAL_PEERS = process.env.PEER_BROKERS ? parsePeerBrokers(process.env.PEER_BROKERS) : [];

// Resolve a routable host address for this broker.
// In Docker, os.hostname() returns the short container ID which is NOT
// DNS-resolvable by other containers.  We use the container's IPv4 address instead.
const BROKER_HOST = (function getContainerIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
})();

console.log(`
╔════════════════════════════════════════════════╗
║     Music PubSub Broker - Distributed Core     ║
║                                                ║
║ Broker ID: ${BROKER_ID.padEnd(37)}║
║ Port: ${PORT.toString().padEnd(43)}║
║ Host: ${BROKER_HOST.padEnd(42)}║
║ Seed Broker: ${(SEED_BROKER || 'none (I am seed)').padEnd(35)}║
║ Initial Peers: ${INITIAL_PEERS.length.toString().padEnd(33)}║
╚════════════════════════════════════════════════╝
`);

try {
  // Initialize Lamport Clock
  const lamportClock = new LamportClock(0, BROKER_ID);
  console.log(`[${BROKER_ID}] Lamport Clock initialized`);

  // Initialize Broker Registry (manages dynamic cluster membership)
  const registry = new BrokerRegistry(BROKER_ID, BROKER_HOST, PORT, lamportClock);
  console.log(`[${BROKER_ID}] BrokerRegistry initialized`);

  // Initialize Services — start with INITIAL_PEERS (may be empty for dynamic mode)
  const heartbeatService = new HeartbeatService(BROKER_ID, BROKER_HOST, PORT, [...INITIAL_PEERS], lamportClock);
  console.log(`[${BROKER_ID}] HeartbeatService initialized`);

  const gossipService = new GossipService(BROKER_ID, PORT, [...INITIAL_PEERS], lamportClock, {
    saveEvent: async (eventData) => {
      const { title, artist, genre, city, state, venue, event_date_time, priority } = eventData.payload;

      // Idempotency: all brokers share the same DB, so the event may already exist
      const existing = await pool.query(
        'SELECT * FROM events WHERE title = $1 AND artist = $2 AND venue = $3 AND event_date_time = $4',
        [title, artist, venue, event_date_time]
      );

      let event;
      if (existing.rows.length > 0) {
        event = existing.rows[0];
      } else {
        const result = await pool.query(
          'INSERT INTO events (title, artist, genre, city, state, venue, event_date_time, priority) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
          [title, artist, genre, city, state, venue, event_date_time, priority || 'normal']
        );
        event = result.rows[0];
      }

      // Find subscribers matching this event
      const matchingResult = await pool.query(
        `SELECT DISTINCT s.user_id
         FROM subscriptions s
         WHERE s.city = $1 AND s.state = $2
         AND CURRENT_DATE BETWEEN s.start_date AND s.end_date
         AND (
            (s.genre IS NULL AND s.artist IS NULL) OR
            (s.genre IS NOT NULL AND s.genre = $3) OR
            (s.artist IS NOT NULL AND s.artist = $4)
         )`,
        [city, state, genre, artist]
      );

      for (const row of matchingResult.rows) {
        const notifExists = await pool.query(
          'SELECT id FROM notifications WHERE user_id = $1 AND event_id = $2',
          [row.user_id, event.id]
        );
        if (notifExists.rows.length === 0) {
          await pool.query(
            'INSERT INTO notifications (user_id, event_id) VALUES ($1, $2)',
            [row.user_id, event.id]
          );
        }
        sendNotification(row.user_id, {
          type: priority === 'urgent' ? 'urgent_notification' : 'notification',
          event,
          source: 'replicated',
        });
      }

      console.log(`[${BROKER_ID}] Processed replicated event: "${title}", notified ${matchingResult.rows.length} local subscribers`);
    },
  });
  console.log(`[${BROKER_ID}] GossipService initialized`);

  // ==================== Wire Registry ↔ Services ====================

  // When a new peer is discovered, add it to heartbeat + gossip
  registry.onPeerAdded = (peer) => {
    heartbeatService.addPeer(peer);
    gossipService.addPeer(peer);
  };

  // When a peer is removed, remove it from heartbeat + gossip
  registry.onPeerRemoved = (brokerId) => {
    heartbeatService.removePeer(brokerId);
    gossipService.removePeer(brokerId);
  };

  // When a heartbeat succeeds, refresh that peer's lastSeen in the registry
  heartbeatService.onHeartbeatSuccess = (brokerId) => {
    registry.refreshPeer(brokerId);
  };

  // When a broker is confirmed dead (10 consecutive missed heartbeats), remove from registry
  heartbeatService.onBrokerDead = (brokerId) => {
    console.log(`[${BROKER_ID}] Broker ${brokerId} confirmed dead, removing from cluster`);
    registry.removePeer(brokerId);
  };

  // Wire queue size into heartbeat reporting
  heartbeatService.queueSizeCallback = () => gossipService.getQueueSize();

  // Pre-populate the registry with any initial (legacy) peers
  for (const peer of INITIAL_PEERS) {
    registry.addPeer(peer);
  }

  // Initialize Controllers — pass registry to HeartbeatController
  const heartbeatController = new HeartbeatController(heartbeatService, lamportClock, registry);
  const replicationController = new ReplicationController(gossipService, heartbeatService, lamportClock);
  console.log(`[${BROKER_ID}] Controllers initialized`);

  // ==================== User API Routes ====================
  app.use('/auth', authRoutes);
  app.use('/subscriptions', subscriptionRouteFactory(lamportClock));
  app.use('/events', eventRouteFactory(gossipService, lamportClock));
  app.use('/agents', agentRouteFactory(BROKER_ID, PORT, registry));

  // ==================== Broker API Routes ====================
  // Health check is public (used by Docker HEALTHCHECK)
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', broker_id: BROKER_ID, timestamp: Date.now() });
  });

  // All /api/* routes require broker-to-broker authentication
  app.post('/api/heartbeat', authenticateBroker, (req, res) => {
    heartbeatController.receiveHeartbeat(req, res);
  });

  app.get('/api/health-status', authenticateBroker, (req, res) => {
    heartbeatController.getHealthStatus(req, res);
  });

  app.post('/api/gossip', authenticateBroker, (req, res) => {
    replicationController.receiveGossip(req, res);
  });

  app.post('/api/sync-request', authenticateBroker, (req, res) => {
    replicationController.handleSyncRequest(req, res);
  });

  app.get('/api/replication-status', authenticateBroker, (req, res) => {
    replicationController.getReplicationStatus(req, res);
  });

  // ==================== Dynamic Cluster Discovery API ====================

  app.post('/api/cluster/register', authenticateBroker, (req, res) => {
    const { broker_id, host, port: peerPort, lamport_clock: remoteClock } = req.body;
    if (!broker_id || !host || !peerPort) {
      return res.status(400).json({ error: 'broker_id, host, and port are required' });
    }
    if (remoteClock) {
      lamportClock.receive(remoteClock);
    }

    registry.addPeer({ id: broker_id, host, port: peerPort });

    const allPeers = registry.getPeers().map(p => ({ id: p.id, host: p.host, port: p.port }));
    allPeers.push({ id: BROKER_ID, host: BROKER_HOST, port: PORT });

    res.json({
      status: 'ok',
      broker_id: BROKER_ID,
      lamport_clock: lamportClock.getValue(),
      peers: allPeers,
      cluster_size: registry.getPeerCount() + 1,
    });
  });

  app.get('/api/cluster/members', authenticateBroker, (_req, res) => {
    res.json({
      self: { id: BROKER_ID, port: PORT },
      peers: registry.getPeers(),
      cluster_size: registry.getPeerCount() + 1,
    });
  });

  // ==================== Startup ====================
  const tlsOptions = {
    key: fs.readFileSync('/app/certs/broker-key.pem'),
    cert: fs.readFileSync('/app/certs/broker-cert.pem'),
  };
  const server = https.createServer(tlsOptions, app);
  setupWebSocket(server);

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n[${BROKER_ID}] Server running on https://0.0.0.0:${PORT}`);

    try {
      heartbeatService.startHeartbeats();
      console.log(`[${BROKER_ID}] Heartbeat service started`);
    } catch (err) {
      console.error(`[${BROKER_ID}] Error starting heartbeat service:`, err);
    }

    try {
      gossipService.startGossip();
      console.log(`[${BROKER_ID}] Gossip service started`);
    } catch (err) {
      console.error(`[${BROKER_ID}] Error starting gossip service:`, err);
    }

    registry.start();

    if (SEED_BROKER && !IS_SEED) {
      const [seedHost, seedPort] = SEED_BROKER.split(':');
      console.log(`[${BROKER_ID}] Will join cluster via seed ${seedHost}:${seedPort} in 3 seconds...`);
      setTimeout(() => {
        registry.joinCluster(seedHost, parseInt(seedPort, 10));
      }, 3000);
    }

    console.log(`[${BROKER_ID}] All services started. Waiting for connections...`);
  });

  server.on('error', (err) => {
    console.error(`[${BROKER_ID}] Server error:`, err);
  });

  process.on('SIGTERM', () => {
    console.log(`\n[${BROKER_ID}] SIGTERM received: closing HTTP server`);
    server.close(() => {
      console.log(`[${BROKER_ID}] HTTP server closed`);
      process.exit(0);
    });
  });

} catch (error) {
  console.error(`[${BROKER_ID}] FATAL ERROR during initialization:`, error);
  console.error(error.stack);
  process.exit(1);
}

// ==================== Helpers ====================
function parsePeerBrokers(peerBrokersStr) {
  return peerBrokersStr.split(',').map((item) => {
    const [id, port] = item.trim().split(':');
    return {
      id: id.trim(),
      host: id.trim(),
      port: parseInt(port.trim(), 10),
    };
  });
}

module.exports = app;