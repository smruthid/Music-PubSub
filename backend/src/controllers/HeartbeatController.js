/**
 * HeartbeatController - Handles incoming heartbeat requests
 */

class HeartbeatController {
    constructor(heartbeatService, lamportClock) {
      this.heartbeatService = heartbeatService;
      this.lamportClock = lamportClock;
    }
  
    /**
     * Handle incoming heartbeat from peer broker
     */
    receiveHeartbeat(req, res) {
      try {
        const { broker_id, lamport_clock, timestamp, status } = req.body;
  
        // Update our Lamport clock
        if (lamport_clock) {
          this.lamportClock.receive(lamport_clock);
        }
  
        console.log(`[Heartbeat] Received from ${broker_id} at clock ${lamport_clock}`);
  
        // Respond with our current clock
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
  
    /**
     * Get broker health status
     */
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