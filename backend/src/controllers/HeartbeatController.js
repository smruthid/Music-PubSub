class HeartbeatController {
    constructor(heartbeatService, lamportClock, registry) {
        this.heartbeatService = heartbeatService;
        this.lamportClock = lamportClock;
        this.registry = registry;
    }

    receiveHeartbeat(req, res) {
        try {
            const { broker_id, host, port, lamport_clock } = req.body;

            if (lamport_clock) {
                this.lamportClock.receive(lamport_clock);
            }

            if (broker_id) {
                if (this.registry) {
                    this.registry.refreshPeer(broker_id);

                    if (!this.registry.peers.has(broker_id)) {
                        this.registry.addPeer({
                            id: broker_id,
                            host: host || broker_id,
                            port: port || 5000,
                        });
                    }
                }

                if (this.heartbeatService.brokerHealth.has(broker_id)) {
                    const health = this.heartbeatService.brokerHealth.get(broker_id);
                    health.healthy = true;
                    health.lastHeartbeatTime = Date.now();
                    health.consecutiveMisses = 0;
                }
            }

            console.log(`[Heartbeat] Received from ${broker_id} at clock ${lamport_clock}`);

            res.json({
                status: 'ok',
                broker_id: this.heartbeatService.brokerId,
                lamport_clock: this.lamportClock.getValue(),
                timestamp: Date.now(),
            });
        } catch (error) {
            console.error('[HeartbeatController] Error:', error);
            res.status(500).json({ error: error.message });
        }
    }

    getHealthStatus(req, res) {
        try {
            res.json({
                broker_id: this.heartbeatService.brokerId,
                status: 'healthy',
                cluster_health: this.heartbeatService.getAllBrokerHealth(),
                lamport_clock: this.lamportClock.getValue(),
                timestamp: Date.now(),
            });
        } catch (error) {
            console.error('[HeartbeatController] Error:', error);
            res.status(500).json({ error: error.message });
        }
    }
}

module.exports = HeartbeatController;