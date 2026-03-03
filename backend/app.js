/**
 * Unified Music PubSub Application
 * Combines broker replication endpoints with user-facing API routes
 */

require('dotenv').config();

const express = require('express');
const http = require('http');
const pool = require('./db');
const LamportClock = require('./src/utils/lamportClock');
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
const BROKER_ID = process.env.BROKER_ID || 'broker-unknown';
const BROKER_PORT = parseInt(process.env.BROKER_PORT, 10) || 5000;
const PORT = parseInt(process.env.PORT, 10) || BROKER_PORT;
const PEER_BROKERS = process.env.PEER_BROKERS ? parsePeerBrokers(process.env.PEER_BROKERS) : [];

console.log(`
╔════════════════════════════════════════════════╗
║     Music PubSub Broker - Distributed Core     ║
║                                                ║
║ Broker ID: ${BROKER_ID.padEnd(37)}║
║ Port: ${PORT.toString().padEnd(43)}║
║ Peer Brokers: ${PEER_BROKERS.length.toString().padEnd(38)}║
╚════════════════════════════════════════════════╝
`);

try {
  // Initialize Lamport Clock
  const lamportClock = new LamportClock(0, BROKER_ID);
  console.log(`[${BROKER_ID}] Lamport Clock initialized`);

  // Initialize Services
  const heartbeatService = new HeartbeatService(BROKER_ID, PORT, PEER_BROKERS, lamportClock);
  console.log(`[${BROKER_ID}] HeartbeatService initialized`);

  const gossipService = new GossipService(BROKER_ID, PORT, PEER_BROKERS, lamportClock, {
    saveEvent: async (eventData) => {
      const { title, artist, genre, city, state, venue, event_date_time, priority } = eventData.payload;

      // Skip if a matching event already exists (idempotency for replicated events)
      // All brokers share the same DB, so the event may already exist (saved by the
      // originating broker). Get it if present, otherwise insert it.
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

      // Find subscribers matching this event on the shared DB
      const matchingResult = await pool.query(
        `SELECT DISTINCT s.user_id
         FROM subscriptions s
         WHERE s.city = $1 AND s.state = $2
         AND $5::DATE BETWEEN s.start_date AND s.end_date
         AND (
            (s.genre IS NULL AND s.artist IS NULL) OR
            (s.genre IS NOT NULL AND s.genre = $3) OR
            (s.artist IS NOT NULL AND s.artist = $4)
         )`,
        [city, state, genre, artist, event_date_time]
      );

      for (const row of matchingResult.rows) {
        // Avoid duplicate notification records (originating broker may have already inserted one)
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
        // Always push via WebSocket — this broker may have clients the originating broker doesn't
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

  // Wire queue size into heartbeat reporting
  heartbeatService.queueSizeCallback = () => gossipService.getQueueSize();

  // Initialize Controllers
  const heartbeatController = new HeartbeatController(heartbeatService, lamportClock);
  const replicationController = new ReplicationController(gossipService, heartbeatService, lamportClock);
  console.log(`[${BROKER_ID}] Controllers initialized`);

  // ==================== User API Routes ====================
  app.use('/auth', authRoutes);
  app.use('/subscriptions', subscriptionRouteFactory(lamportClock));
  app.use('/events', eventRouteFactory(gossipService, lamportClock));
  app.use('/agents', agentRouteFactory(BROKER_ID, PORT, PEER_BROKERS));

  // ==================== Broker API Routes ====================
  app.get('/health', (req, res) => {
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
