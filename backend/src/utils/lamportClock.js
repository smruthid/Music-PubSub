class LamportClock {
    constructor(initialValue = 0, brokerId = 'broker-unknown') {
        this.clock = initialValue;
        this.brokerId = brokerId;
        this.locks = new Map();
    }

    increment() {
        this.clock += 1;
        return this.clock;
    }

    // on receive: max(local, received) + 1 to preserve causal ordering
    receive(receivedClock) {
        this.clock = Math.max(this.clock, receivedClock) + 1;
        return this.clock;
    }

    getValue() {
        return this.clock;
    }

    getTimestamp() {
        return {
            lamport_clock: this.clock,
            timestamp: Date.now(),
            broker_id: this.brokerId,
        };
    }

    static compare(clock1, clock2) {
        if (clock1 < clock2) return -1;
        if (clock1 > clock2) return 1;
        return 0;
    }

    shouldDeliver(event1Clock, event2Clock) {
        return event1Clock < event2Clock;
    }
}

module.exports = LamportClock;
