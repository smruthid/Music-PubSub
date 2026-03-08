const crypto = require('crypto');

class Message {
    constructor(type, brokerId, lamportClock, payload) {
        this.message_id = crypto.randomUUID(); // globally unique ID for deduplication
        this.type = type; // 'event', 'ack', 'sync_request', 'sync_response'
        this.origin_broker = brokerId; // the broker that ORIGINALLY published the event
        this.broker_id = brokerId; // the broker currently forwarding the event
        this.lamport_clock = lamportClock;
        this.timestamp = Date.now();
        this.publish_timestamp = Date.now(); // wall-clock time at original publish (for latency measurement)
        this.payload = payload;
        this.seq_number = null; // assigned when added to the queue
        this.replicated = false;
        this.hops = 0; // how many times this message has been forwarded
    }

    toJSON() {
        return {
            message_id: this.message_id,
            type: this.type,
            origin_broker: this.origin_broker,
            broker_id: this.broker_id,
            lamport_clock: this.lamport_clock,
            timestamp: this.timestamp,
            publish_timestamp: this.publish_timestamp,
            seq_number: this.seq_number,
            payload: this.payload,
            replicated: this.replicated,
            hops: this.hops,
        };
    }

    static fromJSON(json) {
        const msg = new Message(json.type, json.broker_id, json.lamport_clock, json.payload);
        msg.message_id = json.message_id || crypto.randomUUID();
        msg.origin_broker = json.origin_broker || json.broker_id;
        msg.seq_number = json.seq_number;
        msg.replicated = json.replicated;
        msg.timestamp = json.timestamp;
        msg.publish_timestamp = json.publish_timestamp || json.timestamp;
        msg.hops = json.hops || 0;
        return msg;
    }
}

module.exports = Message;