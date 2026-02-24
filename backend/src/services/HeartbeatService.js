const axios = require('axios');
const LamportClock = require('../utils/lamportClock');

/**
 * HeartbeatService - Manages broker liveness detection
 * Sends periodic heartbeats to peer brokers and tracks their health
 */

class HeartbeatService {
  constructor(brokerId, port, peerBrokers, lamportClock) {
    this.brokerId = brokerId;
    this.port = port;
    this.peerBrokers = peerBrokers; // Array of { id, host, port }
    this.lamportClock = lamportClock;
    
    this.heartbeatInterval = 5000; // 5 seconds
    this.heartbeatTimeout = 15000; // 15 seconds
    this.brokerHealth = new Map(); // Track health status of peers
    
    this.initializeBrokerHealth();
  }

  /**
   * Initialize health tracking for all peer brokers
   */
  initializeBrokerHealth() {
    this.peerBrokers.forEach(broker => {
      this.brokerHealth.set(broker.id, {
        healthy: true,
        lastHeartbeatTime: Date.now(),
        missedHeartbeats: 0,
        consecutiveMisses: 0,
      });
    });
  }

  /**
   * Start sending heartbeats to all peer brokers
   */
  startHeartbeats() {
    console.log(`[${this.brokerId}] Starting heartbeat service`);
    
    setInterval(() => {
      this.sendHeartbeats();
    }, this.heartbeatInterval);

    // Also check for dead brokers periodically
    setInterval(() => {
      this.checkBrokerHealth();
    }, this.heartbeatTimeout);
  }

  /**
   * Send heartbeat to all peer brokers
   */
  async sendHeartbeats() {
    const message = {
      type: 'heartbeat',
      broker_id: this.brokerId,
      timestamp: Date.now(),
      lamport_clock: this.lamportClock.increment(),
      status: 'healthy',
      message_queue_length: 0, // TODO: Get from actual queue
    };

    for (const broker of this.peerBrokers) {
      this.sendHeartbeatToBroker(broker, message);
    }
  }

  /**
   * Send heartbeat to a specific broker
   */
  async sendHeartbeatToBroker(broker, message) {
    try {
      const url = `http://${broker.host}:${broker.port}/api/heartbeat`;
      const response = await axios.post(url, message, {
        timeout: 3000,
      });

      // Update Lamport clock based on response
      if (response.data && response.data.lamport_clock) {
        this.lamportClock.receive(response.data.lamport_clock);
      }

      // Mark broker as healthy
      if (this.brokerHealth.has(broker.id)) {
        const health = this.brokerHealth.get(broker.id);
        health.healthy = true;
        health.lastHeartbeatTime = Date.now();
        health.consecutiveMisses = 0;
      }

      console.log(`[${this.brokerId}] ✓ Heartbeat sent to ${broker.id}`);
    } catch (error) {
      this.handleHeartbeatFailure(broker);
    }
  }

  /**
   * Handle heartbeat failure - increment miss counter
   */
  handleHeartbeatFailure(broker) {
    if (this.brokerHealth.has(broker.id)) {
      const health = this.brokerHealth.get(broker.id);
      health.consecutiveMisses += 1;
      health.missedHeartbeats += 1;

      // Mark as unhealthy after 3 consecutive misses
      if (health.consecutiveMisses >= 3) {
        health.healthy = false;
        console.warn(
          `[${this.brokerId}] ⚠ Broker ${broker.id} marked as unhealthy (${health.missedHeartbeats} misses)`
        );
      }
    }
  }

  /**
   * Check overall health of broker cluster
   */
  checkBrokerHealth() {
    let healthyBrokers = 0;
    this.brokerHealth.forEach((health, brokerId) => {
      if (health.healthy) {
        healthyBrokers += 1;
      }
    });

    const totalBrokers = this.peerBrokers.length + 1; // +1 for self
    console.log(
      `[${this.brokerId}] Cluster health: ${healthyBrokers}/${totalBrokers} brokers healthy`
    );
  }

  /**
   * Get health status of a specific broker
   */
  getBrokerHealth(brokerId) {
    return this.brokerHealth.get(brokerId);
  }

  /**
   * Get all broker health statuses
   */
  getAllBrokerHealth() {
    return Object.fromEntries(this.brokerHealth);
  }

  /**
   * Is the cluster in a healthy state?
   */
  isClusterHealthy() {
    let healthyCount = 0;
    this.brokerHealth.forEach(health => {
      if (health.healthy) healthyCount += 1;
    });
    // Cluster is healthy if majority are healthy
    return healthyCount >= Math.ceil(this.peerBrokers.length / 2);
  }
}

module.exports = HeartbeatService;