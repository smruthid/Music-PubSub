/**
 * Unified Music PubSub Application
 * Combines broker replication endpoints with user-facing API routes.
 * Supports dynamic cluster scaling via seed-broker discovery.
 */

require('dotenv').config();

const os = require('os');
const express = require('express');
const http = require('http');
const pool = require('./db');
const LamportClock = require('./src/utils/lamportClock');
const BrokerRegistry = require('./src/services/BrokerRegistry');
const HeartbeatService = require('./src/services/HeartbeatService');
const GossipService = require('./src/services/GossipService');
const HeartbeatController = require('./src/controllers/HeartbeatController');
const ReplicationController = require('./src/controllers/ReplicationController');
const { setupWebSocket, sendNotification } = require('./websocket');

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
// Docker Compose gives each scaled container a unique hostname (e.g. music-pubsub-broker-1).
const BROKER_ID = process.env.BROKER_ID || `broker-${os.hostname()}`;

// Seed broker for dynamic discovery (non-seed brokers use this to join the cluster)
const SEED_BROKER = process.env.SEED_BROKER || null; // e.g. "seed-broker:5000"
const IS_SEED = process.env.IS_SEED === 'true';

// Legacy support: if PEER_BROKERS is provided, parse it as the initial peer list
const INITIAL_PEERS = process.env.PEER_BROKERS ? parsePeerBrokers(process.env.PEER_BROKERS) : [];

console.log(`
╔════════════════════════════════════════════════╗
║     Music PubSub Broker - Distributed Core     ║
║                                                ║
║ Broker ID: ${BROKER_ID.padEnd(37)}║
║ Port: ${PORT.toString().padEnd(43)}║
║ Seed Broker: ${(SEED_BROKER || 'none (I am seed)').padEnd(35)}║
║ Initial Peers: ${INITIAL_PEERS.length.toString().padEnd(33)}║
╚════════════════════════════════════════════════╝
`);

try {
  // Initialize Lamport Clock
  const lamportClock = new LamportClock(0, BROKER_ID);
  console.log(`[${BROKER_ID}] Lamport Clock initialized`);

  // Initialize Broker Registry (manages dynamic cluster membership)
  const registry = new BrokerRegistry(BROKER_ID, PORT, lamportClock);
  console.log(`[${BROKER_ID}] BrokerRegistry initialized`);

  // Initialize Services — start with INITIAL_PEERS (may be empty for dynamic mode)
  const heartbeatService = new HeartbeatService(BROKER_ID, PORT, [...INITIAL_PEERS], lamportClock);
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

  // Initialize Controllers
  const heartbeatController = new HeartbeatController(heartbeatService, lamportClock);
  const replicationController = new ReplicationController(gossipService, heartbeatService, lamportClock);
  console.log(`[${BROKER_ID}] Controllers initialized`);

  // ==================== User API Routes ====================
  app.use('/auth', authRoutes);
  app.use('/subscriptions', subscriptionRouteFactory(lamportClock));
  app.use('/events', eventRouteFactory(gossipService, lamportClock));
  app.use('/agents', agentRouteFactory(BROKER_ID, PORT, registry));

  // ==================== Broker API Routes ====================
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', broker_id: BROKER_ID, timestamp: Date.now() });
  });

  app.post('/api/heartbeat', (req, res) => {
    heartbeatController.receiveHeartbeat(req, res);
  });

  app.get('/api/health-status', (req, res) => {
    heartbeatController.getHealthStatus(req, res);
  });

  app.post('/api/gossip', (req, res) => {
    replicationController.receiveGossip(req, res);
  });

  app.post('/api/sync-request', (req, res) => {
    replicationController.handleSyncRequest(req, res);
  });

  app.get('/api/replication-status', (req, res) => {
    replicationController.getReplicationStatus(req, res);
  });

  // ==================== Dynamic Cluster Discovery API ====================

  /**
   * POST /api/cluster/register
   * Called by a new broker to register itself with this broker.
   * This broker responds with its full list of known peers so the
   * new broker can announce itself to everyone.
   */
  app.post('/api/cluster/register', (req, res) => {
    const { broker_id, host, port: peerPort, lamport_clock: remoteClock } = req.body;
    if (!broker_id || !host || !peerPort) {
      return res.status(400).json({ error: 'broker_id, host, and port are required' });
    }
    if (remoteClock) {
      lamportClock.receive(remoteClock);
    }

    // Add the new peer to our registry (this also triggers onPeerAdded → heartbeat + gossip)
    registry.addPeer({ id: broker_id, host, port: peerPort });

    // Build the full peer list: all known peers + ourselves
    const allPeers = registry.getPeers().map(p => ({ id: p.id, host: p.host, port: p.port }));
    allPeers.push({ id: BROKER_ID, host: BROKER_ID, port: PORT });

    res.json({
      status: 'ok',
      broker_id: BROKER_ID,
      lamport_clock: lamportClock.getValue(),
      peers: allPeers,
      cluster_size: registry.getPeerCount() + 1,
    });
  });

  /**
   * GET /api/cluster/members
   * Returns the current cluster membership as seen by this broker.
   */
  app.get('/api/cluster/members', (_req, res) => {
    res.json({
      self: { id: BROKER_ID, port: PORT },
      peers: registry.getPeers(),
      cluster_size: registry.getPeerCount() + 1,
    });
  });

  // ==================== Startup ====================
  const server = http.createServer(app);
  setupWebSocket(server);

  server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n[${BROKER_ID}] Server running on http://0.0.0.0:${PORT}`);

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

    // Start the registry's dead-peer cleanup loop
    registry.start();

    // If we're not the seed broker, join the cluster via the seed
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