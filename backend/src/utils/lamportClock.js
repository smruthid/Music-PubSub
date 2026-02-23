/**
 * LamportClock - Implementation of Lamport Timestamps for causal ordering
 * Used to establish total ordering of events in the distributed system
 */

class LamportClock {
    constructor(initialValue = 0, brokerId = 'broker-unknown') {
      this.clock = initialValue;
      this.brokerId = brokerId;
      this.locks = new Map(); // Store locks per operation for thread safety
    }
  
    /**
     * Increment the clock before sending a message
     * @returns {number} - The new clock value
     */
    increment() {
      this.clock += 1;
      return this.clock;
    }
  
    /**
     * Update clock on receiving a message
     * Ensures causal ordering: max(local_clock, received_clock) + 1
     * @param {number} receivedClock - Clock value from received message
     * @returns {number} - The updated clock value
     */
    receive(receivedClock) {
      this.clock = Math.max(this.clock, receivedClock) + 1;
      return this.clock;
    }
  
    /**
     * Get current clock value without incrementing
     * @returns {number} - Current clock value
     */
    getValue() {
      return this.clock;
    }
  
    /**
     * Get current timestamp with clock value for logging/tracking
     * @returns {Object} - { lamport_clock, timestamp, broker_id }
     */
    getTimestamp() {
      return {
        lamport_clock: this.clock,
        timestamp: Date.now(),
        broker_id: this.brokerId,
      };
    }
  
    /**
     * Compare two clock values for ordering
     * @param {number} clock1
     * @param {number} clock2
     * @returns {number} - -1 if clock1 < clock2, 0 if equal, 1 if clock1 > clock2
     */
    static compare(clock1, clock2) {
      if (clock1 < clock2) return -1;
      if (clock1 > clock2) return 1;
      return 0;
    }
  
    /**
     * Check if an event should be delivered before another based on clock
     * @param {number} event1Clock
     * @param {number} event2Clock
     * @returns {boolean} - true if event1 should come before event2
     */
    shouldDeliver(event1Clock, event2Clock) {
      return event1Clock < event2Clock;
    }
  }
  
  module.exports = LamportClock;