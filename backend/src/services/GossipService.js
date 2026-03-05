const axios = require('../utils/brokerClient');
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

        // Deduplication: track message_ids we've already processed
        this.seenMessages = new Set();

        // Maximum number of hops a message can travel before it stops being re-gossiped
        this.maxHops = 5;

        // Track the highest seq number each peer has acknowledged (for locally published messages)
        this.brokerSeqTracking = new Map();
        this.peerBrokers.forEach(b => this.brokerSeqTracking.set(b.id, 0));

        this.fanout = 2;
        this.gossipInterval = 2000;

        // Limit seenMessages set size to prevent unbounded memory growth
        this.maxSeenMessages = 10000;
    }

    startGossip() {
        console.log(`[${this.brokerId}] Starting gossip service`);
        this._gossipTimer = setInterval(() => this.runGossipRound(), this.gossipInterval);
        console.log(`[${this.brokerId}] Gossip service started`);
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

        // Mark as seen so we don't re-process our own message
        this.seenMessages.add(message.message_id);
        this.messageQueue.push(message);

        console.log(`[${this.brokerId}] Published event seq=${this.seqNumber}, msgId=${message.message_id.slice(0,8)}: ${event.title}`);
        return message;
    }

    async runGossipRound() {
        if (this.messageQueue.length === 0) return;

        const targets = this.selectRandomPeers(this.fanout);
        for (const broker of targets) {
            await this.sendGossipToBroker(broker);
        }

        // After sending, age every message in the queue by incrementing its hop count.
        // This ensures that even locally-published messages (which start at hops=0)
        // will eventually be pruned, stopping the "0 new, N duplicates" chatter.
        this.messageQueue.forEach(msg => { msg.hops += 1; });

        // Prune messages that have exceeded maxHops (they've spread far enough)
        this.messageQueue = this.messageQueue.filter(msg => msg.hops < this.maxHops);

        // Prune seenMessages if it gets too large (keep memory bounded)
        if (this.seenMessages.size > this.maxSeenMessages) {
            const entries = [...this.seenMessages];
            const toRemove = entries.slice(0, entries.length - this.maxSeenMessages / 2);
            toRemove.forEach(id => this.seenMessages.delete(id));
            console.log(`[${this.brokerId}] Pruned seenMessages set to ${this.seenMessages.size}`);
        }
    }

    selectRandomPeers(count) {
        const shuffled = [...this.peerBrokers].sort(() => Math.random() - 0.5);
        return shuffled.slice(0, Math.min(count, shuffled.length));
    }

    async sendGossipToBroker(broker) {
        try {
            // Send all messages currently in the queue (the receiver will deduplicate).
            // No need to filter by hops here — runGossipRound prunes after this call.
            const messagesToSend = this.messageQueue;

            if (messagesToSend.length === 0) return;

            const payload = {
                type: 'gossip',
                broker_id: this.brokerId,
                lamport_clock: this.lamportClock.increment(),
                timestamp: Date.now(),
                events: messagesToSend.map(msg => msg.toJSON()),
            };

            const url = `https://${broker.host}:${broker.port}/api/gossip`;
            const headers = {};
            if (process.env.BROKER_SECRET) {
                headers['X-Broker-Secret'] = process.env.BROKER_SECRET;
            }
            const response = await axios.post(url, payload, { timeout: 3000, headers });

            if (response.data?.status === 'success') {
                console.log(`[${this.brokerId}] ✓ Gossiped ${messagesToSend.length} msg(s) to ${broker.id}`);
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
            let newCount = 0;
            let duplicateCount = 0;

            for (const eventData of events) {
                const messageId = eventData.message_id;

                // DEDUPLICATION: skip messages we've already seen
                if (messageId && this.seenMessages.has(messageId)) {
                    duplicateCount++;
                    continue;
                }

                // Mark as seen
                if (messageId) {
                    this.seenMessages.add(messageId);
                }

                // Save to database (idempotent — the DB check in app.js handles duplicates)
                await this.database.saveEvent(eventData);
                newCount++;

                // RE-GOSSIP: Add to our own messageQueue so we forward it to other peers.
                // This is the key fix — without this, messages die after one hop.
                if ((eventData.hops || 0) < this.maxHops) {
                    const forwarded = Message.fromJSON(eventData);
                    forwarded.hops = (eventData.hops || 0) + 1; // increment hop count
                    forwarded.broker_id = this.brokerId; // we are now the forwarder
                    this.seqNumber += 1;
                    forwarded.seq_number = this.seqNumber;
                    this.messageQueue.push(forwarded);
                }
            }

            console.log(
                `[${this.brokerId}] Received gossip from ${gossipMessage.broker_id}: ${newCount} new, ${duplicateCount} duplicates`
            );

            return { status: 'success', processed: newCount, duplicates: duplicateCount };
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