const axios = require('axios');
const Message = require('../models/Message');

class GossipService {
    constructor(brokerId, port, peerBrokers, lamportClock, database) {
        this.brokerId = brokerId;
        this.port = port;
        this.peerBrokers = peerBrokers;
        this.lamportClock = lamportClock;
        this.database = database;

        this.messageQueue = [];
        this.seqNumber = 0;

        // track the highest seq number each peer has acknowledged
        this.brokerSeqTracking = new Map();
        this.peerBrokers.forEach(b => this.brokerSeqTracking.set(b.id, 0));

        this.fanout = 2;
        this.gossipInterval = 2000;
    }

    startGossip() {
        console.log(`[${this.brokerId}] Starting gossip service`);
        setInterval(() => this.runGossipRound(), this.gossipInterval);
    }

    publishEvent(event) {
        this.seqNumber += 1;

        const message = new Message('event', this.brokerId, this.lamportClock.increment(), {
            event_id: event.id,
            title: event.title,
            artist: event.artist,
            genre: event.genre,
            city: event.city,
            state: event.state,
            venue: event.venue,
            event_date_time: event.event_date_time,
            priority: event.priority,
            published_at: event.published_at,
        });

        message.seq_number = this.seqNumber;
        this.messageQueue.push(message);

        console.log(`[${this.brokerId}] Published event seq=${this.seqNumber}: ${event.title}`);
        return message;
    }

    async runGossipRound() {
        if (this.messageQueue.length === 0) return;

        const targets = this.selectRandomPeers(this.fanout);
        for (const broker of targets) {
            this.sendGossipToBroker(broker);
        }
    }

    selectRandomPeers(count) {
        const shuffled = [...this.peerBrokers].sort(() => Math.random() - 0.5);
        return shuffled.slice(0, Math.min(count, shuffled.length));
    }

    async sendGossipToBroker(broker) {
        try {
            const lastSeq = this.brokerSeqTracking.get(broker.id) || 0;
            const newMessages = this.messageQueue.filter(msg => msg.seq_number > lastSeq);

            if (newMessages.length === 0) return;

            const payload = {
                type: 'gossip',
                broker_id: this.brokerId,
                lamport_clock: this.lamportClock.increment(),
                timestamp: Date.now(),
                events: newMessages.map(msg => msg.toJSON()),
                seq_number: this.seqNumber,
            };

            const url = `http://${broker.host}:${broker.port}/api/gossip`;
            const response = await axios.post(url, payload, { timeout: 3000 });

            if (response.data?.status === 'success') {
                this.brokerSeqTracking.set(broker.id, this.seqNumber);
                console.log(`[${this.brokerId}] ✓ Gossiped to ${broker.id}, seq=${this.seqNumber}`);
            }

            if (response.data?.lamport_clock) {
                this.lamportClock.receive(response.data.lamport_clock);
            }
        } catch (error) {
            console.warn(`[${this.brokerId}] Failed to gossip to ${broker.id}:`, error.message);
        }
    }

    async receiveGossip(gossipMessage) {
        try {
            if (gossipMessage.lamport_clock) {
                this.lamportClock.receive(gossipMessage.lamport_clock);
            }

            const events = gossipMessage.events || [];
            console.log(
                `[${this.brokerId}] Received gossip from ${gossipMessage.broker_id} with ${events.length} events`
            );

            for (const eventData of events) {
                await this.database.saveEvent(eventData);
            }

            return { status: 'success', processed: events.length };
        } catch (error) {
            console.error('[GossipService] Error processing gossip:', error);
            throw error;
        }
    }

    getQueueSize() {
        return this.messageQueue.length;
    }

    getCurrentSeq() {
        return this.seqNumber;
    }

    /**
     * Dynamically add a new peer broker at runtime.
     */
    addPeer(broker) {
        if (!this.peerBrokers.find(b => b.id === broker.id)) {
            this.peerBrokers.push(broker);
            this.brokerSeqTracking.set(broker.id, 0);
            console.log(`[${this.brokerId}] GossipService: peer added → ${broker.id}`);
        }
    }

    /**
     * Dynamically remove a peer broker at runtime.
     */
    removePeer(brokerId) {
        this.peerBrokers = this.peerBrokers.filter(b => b.id !== brokerId);
        this.brokerSeqTracking.delete(brokerId);
        console.log(`[${this.brokerId}] GossipService: peer removed → ${brokerId}`);
    }
}

module.exports = GossipService;