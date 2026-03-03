class HeartbeatController {
    constructor(heartbeatService, lamportClock, registry) {
        this.heartbeatService = heartbeatService;
        this.lamportClock = lamportClock;
        this.registry = registry;
    }

    receiveHeartbeat(req, res) {
        try {
            const { broker_id, lamport_clock } = req.body;

            if (lamport_clock) {
                this.lamportClock.receive(lamport_clock);
            }

            // When we RECEIVE a heartbeat from a peer, refresh their lastSeen
            // in the registry so they don't get cleaned up.
            // Also re-register them if they were previously removed.
            if (broker_id && this.registry) {
                const ip = req.ip || req.connection.remoteAddress;
                this.registry.refreshPeer(broker_id);

                // If this broker isn't in the registry yet (e.g. it was cleaned up),
                // re-add it using info from the heartbeat
                if (!this.registry.peers.has(broker_id)) {
                    const port = req.body.port || 5000;
                    const host = req.body.host || broker_id;
                    this.registry.addPeer({ id: broker_id, host, port });
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