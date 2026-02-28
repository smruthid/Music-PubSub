class HeartbeatController {
    constructor(heartbeatService, lamportClock) {
        this.heartbeatService = heartbeatService;
        this.lamportClock = lamportClock;
    }

    receiveHeartbeat(req, res) {
        try {
            const { broker_id, lamport_clock } = req.body;

            if (lamport_clock) {
                this.lamportClock.receive(lamport_clock);
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
