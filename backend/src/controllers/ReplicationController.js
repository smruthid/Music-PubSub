/**
 * ReplicationController - Handles replication endpoints for inter-broker communication
 */

class ReplicationController {
    constructor(gossipService, heartbeatService, lamportClock) {
      this.gossipService = gossipService;
      this.heartbeatService = heartbeatService;
      this.lamportClock = lamportClock;
    }
  
    /**
     * Receive gossip/replication message from peer broker
     */
    async receiveGossip(req, res) {
      try {
        const gossipMessage = req.body;
  
        // Process gossip
        const result = await this.gossipService.receiveGossip(gossipMessage);
  
        res.json({
          status: 'success',
          broker_id: this.gossipService.brokerId,
          lamport_clock: this.lamportClock.getValue(),
          processed: result.processed,
        });
      } catch (error) {
        console.error('[ReplicationController] Gossip error:', error);
        res.status(500).json({ error: error.message });
      }
    }
  
    /**
     * Handle sync request from peer broker
     */
    async handleSyncRequest(req, res) {
      try {
        const { broker_id, last_known_seq } = req.body;
  
        // Get all messages since last_known_seq
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
  
    /**
     * Get replication status
     */
    getReplicationStatus(req, res) {
      try {
        res.json({
          broker_id: this.gossipService.brokerId,
          queue_size: this.gossipService.getQueueSize(),
          current_seq: this.gossipService.getCurrentSeq(),
          broker_health: this.heartbeatService.getAllBrokerHealth(),
          lamport_clock: this.lamportClock.getValue(),
        });
      } catch (error) {
        console.error('[ReplicationController] Status error:', error);
        res.status(500).json({ error: error.message });
      }
    }
  }
  
  module.exports = ReplicationController;