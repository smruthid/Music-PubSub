const axios = require('axios');
const Message = require('../models/Message');

/**
 * GossipService - Implements gossip protocol for event propagation
 * Ensures all brokers receive published events through epidemic dissemination
 */

class GossipService {
  constructor(brokerId, port, peerBrokers, lamportClock, database) {
    this.brokerId = brokerId;
    this.port = port;
    this.peerBrokers = peerBrokers;
    this.lamportClock = lamportClock;
    this.database = database;

    // Message queue for events to be gossiped
    this.messageQueue = [];
    this.seqNumber = 0;

    // Track which brokers have seen which seq numbers
    this.brokerSeqTracking = new Map();
    this.initializeBrokerTracking();

    // Configuration
    this.fanout = 2; // Number of brokers to gossip to (configurable)
    this.gossipInterval = 2000; // 2 seconds
  }

  /**
   * Initialize sequence tracking for all peer brokers
   */
  initializeBrokerTracking() {
    this.peerBrokers.forEach(broker => {
      this.brokerSeqTracking.set(broker.id, 0); // They start at seq 0
    });
  }

  /**
   * Start gossip protocol
   */
  startGossip() {
    console.log(`[${this.brokerId}] Starting gossip service`);

    setInterval(() => {
      this.runGossipRound();
    }, this.gossipInterval);
  }

  /**
   * Publish a new event to the message queue
   */
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

  /**
   * Run one round of gossip
   * Select random peers and send undelivered messages
   */
  async runGossipRound() {
    if (this.messageQueue.length === 0) {
      return;
    }

    // Select random peers (fanout)
    const targets = this.selectRandomPeers(this.fanout);

    for (const broker of targets) {
      this.sendGossipToBroker(broker);
    }
  }

  /**
   * Select random peer brokers
   */
  selectRandomPeers(count) {
    const shuffled = [...this.peerBrokers].sort(() => Math.random() - 0.5);
    return shuffled.slice(0, Math.min(count, shuffled.length));
  }

  /**
   * Send gossip to a specific broker
   */
  async sendGossipToBroker(broker) {
    try {
      // Get messages this broker hasn't seen yet
      const lastSeqBrokerHas = this.brokerSeqTracking.get(broker.id) || 0;
      const newMessages = this.messageQueue.filter(msg => msg.seq_number > lastSeqBrokerHas);

      if (newMessages.length === 0) {
        return;
      }

      const gossipMessage = {
        type: 'gossip',
        broker_id: this.brokerId,
        lamport_clock: this.lamportClock.increment(),
        timestamp: Date.now(),
        events: newMessages.map(msg => msg.toJSON()),
        seq_number: this.seqNumber,
      };

      const url = `http://${broker.host}:${broker.port}/api/gossip`;
      const response = await axios.post(url, gossipMessage, {
        timeout: 3000,
      });

      if (response.data && response.data.status === 'success') {
        // Update tracking - broker has now seen up to seqNumber
        this.brokerSeqTracking.set(broker.id, this.seqNumber);
        console.log(`[${this.brokerId}] ✓ Gossiped to ${broker.id}, seq=${this.seqNumber}`);
      }

      // Update Lamport clock from response
      if (response.data && response.data.lamport_clock) {
        this.lamportClock.receive(response.data.lamport_clock);
      }
    } catch (error) {
      console.warn(`[${this.brokerId}] Failed to gossip to ${broker.id}:`, error.message);
    }
  }

  /**
   * Receive gossip from peer broker
   */
  async receiveGossip(gossipMessage) {
    try {
      // Update Lamport clock
      if (gossipMessage.lamport_clock) {
        this.lamportClock.receive(gossipMessage.lamport_clock);
      }

      const events = gossipMessage.events || [];
      console.log(
        `[${this.brokerId}] Received gossip from ${gossipMessage.broker_id} with ${events.length} events`
      );

      // Process and persist events (delegate to DB layer)
      for (const eventData of events) {
        await this.database.saveEvent(eventData);
      }

      return { status: 'success', processed: events.length };
    } catch (error) {
      console.error('[GossipService] Error processing gossip:', error);
      throw error;
    }
  }

  /**
   * Get current queue size
   */
  getQueueSize() {
    return this.messageQueue.length;
  }

  /**
   * Get current sequence number
   */
  getCurrentSeq() {
    return this.seqNumber;
  }
}

module.exports = GossipService;