class ReplicationController {
    constructor(gossipService, heartbeatService, lamportClock, metricsCollector) {
        this.gossipService = gossipService;
        this.heartbeatService = heartbeatService;
        this.lamportClock = lamportClock;
        this.metricsCollector = metricsCollector || null;
    }

    async receiveGossip(req, res) {
        try {
            const result = await this.gossipService.receiveGossip(req.body);

            res.json({
                status: 'success',
                broker_id: this.gossipService.brokerId,
                lamport_clock: this.lamportClock.getValue(),
                processed: result.processed,
                duplicates: result.duplicates || 0,
            });
        } catch (error) {
            console.error('[ReplicationController] Gossip error:', error);
            res.status(500).json({ error: error.message });
        }
    }

    async handleSyncRequest(req, res) {
        try {
            const { last_known_seq } = req.body;

            const events = this.gossipService.messageQueue.filter(
                msg => msg.seq_number > (last_known_seq || 0)
            );

            res.json({
                type: 'sync_response',
                broker_id: this.gossipService.brokerId,
                lamport_clock: this.lamportClock.increment(),
                timestamp: Date.now(),
                current_seq: this.gossipService.getCurrentSeq(),
                events: events.map(msg => msg.toJSON()),
            });
        } catch (error) {
            console.error('[ReplicationController] Sync error:', error);
            res.status(500).json({ error: error.message });
        }
    }

    getReplicationStatus(_req, res) {
        try {
            res.json({
                broker_id: this.gossipService.brokerId,
                queue_size: this.gossipService.getQueueSize(),
                current_seq: this.gossipService.getCurrentSeq(),
                seen_messages: this.gossipService.seenMessages.size,
                max_hops: this.gossipService.maxHops,
                broker_health: this.heartbeatService.getAllBrokerHealth(),
                lamport_clock: this.lamportClock.getValue(),
            });
        } catch (error) {
            console.error('[ReplicationController] Status error:', error);
            res.status(500).json({ error: error.message });
        }
    }

    // ─── Performance Metrics Endpoint ────────────────────────────
    getMetrics(_req, res) {
        try {
            if (!this.metricsCollector) {
                return res.status(501).json({ error: 'Metrics collector not initialized' });
            }
            res.json(this.metricsCollector.getMetrics());
        } catch (error) {
            console.error('[ReplicationController] Metrics error:', error);
            res.status(500).json({ error: error.message });
        }
    }

    // ─── Reset Metrics (for clean test runs) ─────────────────────
    resetMetrics(_req, res) {
        try {
            if (!this.metricsCollector) {
                return res.status(501).json({ error: 'Metrics collector not initialized' });
            }
            this.metricsCollector.reset();
            res.json({ status: 'ok', message: 'Metrics reset successfully' });
        } catch (error) {
            console.error('[ReplicationController] Metrics reset error:', error);
            res.status(500).json({ error: error.message });
        }
    }
}

module.exports = ReplicationController;