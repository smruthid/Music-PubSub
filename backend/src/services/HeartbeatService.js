const axios = require('axios');

class HeartbeatService {
    constructor(brokerId, port, peerBrokers, lamportClock) {
        this.brokerId = brokerId;
        this.port = port;
        this.peerBrokers = peerBrokers; // [{ id, host, port }]
        this.lamportClock = lamportClock;

        this.heartbeatInterval = 5000;
        this.heartbeatTimeout = 15000;
        this.brokerHealth = new Map();

        // Callback fired when a broker is confirmed dead (10 consecutive misses)
        this.onBrokerDead = null;

        // Callback to get gossip queue size for heartbeat messages
        this.queueSizeCallback = null;

        // Callback to refresh peer's lastSeen in the registry
        this.onHeartbeatSuccess = null;

        this.initializeBrokerHealth();
    }

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

    startHeartbeats() {
        console.log(`[${this.brokerId}] Starting heartbeat service`);

        setInterval(() => this.sendHeartbeats(), this.heartbeatInterval);

        // periodic cluster health summary
        setInterval(() => this.checkBrokerHealth(), this.heartbeatTimeout);
    }

    async sendHeartbeats() {
        const message = {
            type: 'heartbeat',
            broker_id: this.brokerId,
            host: this.brokerId,
            port: this.port,
            timestamp: Date.now(),
            lamport_clock: this.lamportClock.increment(),
            status: 'healthy',
            message_queue_length: this.queueSizeCallback ? this.queueSizeCallback() : 0,
        };

        for (const broker of this.peerBrokers) {
            this.sendHeartbeatToBroker(broker, message);
        }
    }

    async sendHeartbeatToBroker(broker, message) {
        try {
            const url = `http://${broker.host}:${broker.port}/api/heartbeat`;
            const response = await axios.post(url, message, { timeout: 3000 });

            if (response.data?.lamport_clock) {
                this.lamportClock.receive(response.data.lamport_clock);
            }

            if (this.brokerHealth.has(broker.id)) {
                const health = this.brokerHealth.get(broker.id);
                health.healthy = true;
                health.lastHeartbeatTime = Date.now();
                health.consecutiveMisses = 0;
            }

            // Notify registry that this peer is alive
            if (this.onHeartbeatSuccess) {
                this.onHeartbeatSuccess(broker.id);
            }

            console.log(`[${this.brokerId}] ✓ Heartbeat sent to ${broker.id}`);
        } catch (error) {
            this.handleHeartbeatFailure(broker);
        }
    }

    handleHeartbeatFailure(broker) {
        if (!this.brokerHealth.has(broker.id)) return;

        const health = this.brokerHealth.get(broker.id);
        health.consecutiveMisses += 1;
        health.missedHeartbeats += 1;

        // mark unhealthy after 3 consecutive misses
        if (health.consecutiveMisses >= 3) {
            health.healthy = false;
            console.warn(
                `[${this.brokerId}] ⚠ Broker ${broker.id} marked as unhealthy (${health.missedHeartbeats} misses)`
            );
        }

        // After 10 consecutive misses, consider the broker dead
        if (health.consecutiveMisses >= 10 && this.onBrokerDead) {
            console.log(`[${this.brokerId}] ✗ Broker ${broker.id} confirmed dead after ${health.consecutiveMisses} misses`);
            this.onBrokerDead(broker.id);
        }
    }

    checkBrokerHealth() {
        let healthy = 0;
        this.brokerHealth.forEach(h => { if (h.healthy) healthy++; });
        const total = this.peerBrokers.length + 1; // +1 for self
        console.log(`[${this.brokerId}] Cluster health: ${healthy + 1}/${total} brokers healthy (including self)`);
    }

    getBrokerHealth(brokerId) {
        return this.brokerHealth.get(brokerId);
    }

    getAllBrokerHealth() {
        return Object.fromEntries(this.brokerHealth);
    }

    isClusterHealthy() {
        let healthyCount = 0;
        this.brokerHealth.forEach(h => { if (h.healthy) healthyCount++; });
        return healthyCount >= Math.ceil(this.peerBrokers.length / 2);
    }

    /**
     * Dynamically add a new peer broker at runtime.
     */
    addPeer(broker) {
        if (!this.peerBrokers.find(b => b.id === broker.id)) {
            this.peerBrokers.push(broker);
            this.brokerHealth.set(broker.id, {
                healthy: true,
                lastHeartbeatTime: Date.now(),
                missedHeartbeats: 0,
                consecutiveMisses: 0,
            });
            console.log(`[${this.brokerId}] HeartbeatService: peer added → ${broker.id}`);
        }
    }

    /**
     * Dynamically remove a peer broker at runtime.
     */
    removePeer(brokerId) {
        this.peerBrokers = this.peerBrokers.filter(b => b.id !== brokerId);
        this.brokerHealth.delete(brokerId);
        console.log(`[${this.brokerId}] HeartbeatService: peer removed → ${brokerId}`);
    }
}

module.exports = HeartbeatService;