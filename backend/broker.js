/**
 * Music PubSub Broker - Distributed Pub/Sub System
 * Main entry point for broker instance
 */

require('dotenv').config();

const express = require('express');
const os = require('os');
const LamportClock = require('./src/utils/lamportClock');
const HeartbeatService = require('./src/services/HeartbeatService');
const GossipService = require('./src/services/GossipService');
const HeartbeatController = require('./src/controllers/HeartbeatController');
const ReplicationController = require('./src/controllers/ReplicationController');

const app = express();
app.use(express.json());

// Configuration from environment variables
const BROKER_ID = process.env.BROKER_ID || 'broker-unknown';
const BROKER_PORT = parseInt(process.env.BROKER_PORT) || 5000;
const PEER_BROKERS = process.env.PEER_BROKERS ? parsePeerBrokers(process.env.PEER_BROKERS) : [];

console.log(`
╔════════════════════════════════════════════════╗
║     Music PubSub Broker - Distributed Core     ║
║                                                ║
║ Broker ID: ${BROKER_ID.padEnd(37)}║
║ Port: ${BROKER_PORT.toString().padEnd(43)}║
║ Peer Brokers: ${PEER_BROKERS.length.toString().padEnd(38)}║
╚════════════════════════════════════════════════╝
`);

try {
  // Initialize Lamport Clock
  const lamportClock = new LamportClock(0, BROKER_ID);
  console.log(`[${BROKER_ID}] Lamport Clock initialized`);

  // Initialize Services
  const heartbeatService = new HeartbeatService(BROKER_ID, BROKER_PORT, PEER_BROKERS, lamportClock);
  console.log(`[${BROKER_ID}] HeartbeatService initialized`);

  const gossipService = new GossipService(BROKER_ID, BROKER_PORT, PEER_BROKERS, lamportClock, {
    saveEvent: async (eventData) => {
      console.log(`[${BROKER_ID}] Saving event:`, eventData.title);
      // TODO: Implement actual database saving
    },
  });
  console.log(`[${BROKER_ID}] GossipService initialized`);

  // Initialize Controllers
  const heartbeatController = new HeartbeatController(heartbeatService, lamportClock);
  const replicationController = new ReplicationController(gossipService, heartbeatService, lamportClock);
  console.log(`[${BROKER_ID}] Controllers initialized`);

  // ==================== API Routes ====================

  // Health check endpoint
  app.get('/health', (req, res) => {
    res.json({ status: 'ok', broker_id: BROKER_ID, timestamp: Date.now() });
  });

  // Heartbeat endpoint
  app.post('/api/heartbeat', (req, res) => {
    heartbeatController.receiveHeartbeat(req, res);
  });

  // Health status endpoint
  app.get('/api/health-status', (req, res) => {
    heartbeatController.getHealthStatus(req, res);
  });

  // Gossip/replication endpoint
  app.post('/api/gossip', (req, res) => {
    replicationController.receiveGossip(req, res);
  });

  // Sync request endpoint
  app.post('/api/sync-request', (req, res) => {
    replicationController.handleSyncRequest(req, res);
  });

  // Replication status endpoint
  app.get('/api/replication-status', (req, res) => {
    replicationController.getReplicationStatus(req, res);
  });

  // ==================== Startup ====================

  const server = app.listen(BROKER_PORT, () => {
    console.log(`\n[${BROKER_ID}] Server running on http://localhost:${BROKER_PORT}`);

    // Start services
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

    console.log(`[${BROKER_ID}] Services started: heartbeat, gossip`);
    console.log(`[${BROKER_ID}] Waiting for connections...`);
  });

  // Error handling
  server.on('error', (err) => {
    console.error(`[${BROKER_ID}] Server error:`, err);
  });

  // ==================== Graceful Shutdown ====================

  process.on('SIGTERM', () => {
    console.log(`\n[${BROKER_ID}] SIGTERM signal received: closing HTTP server`);
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

/**
 * Parse peer brokers from environment variable
 * Format: "broker-1:5001,broker-2:5002,broker-3:5003"
 */
function parsePeerBrokers(peerBrokersStr) {
  return peerBrokersStr.split(',').map((item, index) => {
    const [id, port] = item.trim().split(':');
    return {
      id: id.trim(),
      host: id.trim(), // Using hostname from docker-compose service name
      port: parseInt(port.trim()),
    };
  });
}

module.exports = app;