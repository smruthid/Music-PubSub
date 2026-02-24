/**
 * Message model - Represents a gossip message in the pub/sub system
 */

class Message {
    constructor(type, brokerId, lamportClock, payload) {
      this.type = type; // 'event', 'ack', 'sync_request', 'sync_response'
      this.broker_id = brokerId;
      this.lamport_clock = lamportClock;
      this.timestamp = Date.now();
      this.payload = payload;
      this.seq_number = null; // Set by broker when queued
      this.replicated = false;
    }
  
    /**
     * Convert message to JSON for transmission
     */
    toJSON() {
      return {
        type: this.type,
        broker_id: this.broker_id,
        lamport_clock: this.lamport_clock,
        timestamp: this.timestamp,
        seq_number: this.seq_number,
        payload: this.payload,
        replicated: this.replicated,
      };
    }
  
    /**
     * Create message from incoming JSON
     */
    static fromJSON(json) {
      const msg = new Message(json.type, json.broker_id, json.lamport_clock, json.payload);
      msg.seq_number = json.seq_number;
      msg.replicated = json.replicated;
      msg.timestamp = json.timestamp;
      return msg;
    }
  }
  
  module.exports = Message;