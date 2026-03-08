/**
 * MetricsCollector — Performance evaluation for the distributed pub-sub system.
 *
 * Tracks:
 *  - Publish-to-delivery LATENCY (wall-clock ms between origin publish and gossip receipt)
 *  - Message RELIABILITY (delivered-vs-published ratio)
 *  - THROUGHPUT (publishes/sec and deliveries/sec over a sliding window)
 *  - Per-broker delivery breakdown
 *  - Gossip protocol overhead (duplicate counts)
 */
class MetricsCollector {
    constructor(brokerId) {
        this.brokerId = brokerId;

        // ── Latency ──────────────────────────────────────────────
        this.latencySamples = [];          // raw latency values (ms)
        this.maxSamples = 10000;           // cap to prevent unbounded growth

        // ── Reliability ──────────────────────────────────────────
        this.eventsPublished = 0;          // events originally published on THIS broker
        this.eventsDelivered = 0;          // events received via gossip on THIS broker
        this.duplicatesReceived = 0;       // gossip duplicates (already-seen messages)
        this.gossipRoundsSent = 0;         // number of outbound gossip rounds completed
        this.gossipMessagesSent = 0;       // total individual messages sent in gossip

        // ── Throughput (sliding window) ──────────────────────────
        this.publishTimestamps = [];       // timestamps of local publishes
        this.deliveryTimestamps = [];      // timestamps of gossip deliveries
        this.windowMs = 60000;             // 60-second sliding window

        // ── Per-broker breakdown ─────────────────────────────────
        this.deliveriesByOrigin = {};      // { origin_broker_id: count }

        // ── Start time ───────────────────────────────────────────
        this.startTime = Date.now();
    }

    // ─── Called when THIS broker publishes an event ───────────────
    recordPublish() {
        this.eventsPublished += 1;
        this.publishTimestamps.push(Date.now());
        this._pruneWindow(this.publishTimestamps);
    }

    // ─── Called when THIS broker receives a NEW event via gossip ──
    recordDelivery(message) {
        this.eventsDelivered += 1;
        this.deliveryTimestamps.push(Date.now());
        this._pruneWindow(this.deliveryTimestamps);

        // Latency: compare publish_timestamp (set at origin) to now
        if (message.publish_timestamp) {
            const latency = Date.now() - message.publish_timestamp;
            if (latency >= 0) {
                this.latencySamples.push(latency);
                if (this.latencySamples.length > this.maxSamples) {
                    // drop oldest half
                    this.latencySamples = this.latencySamples.slice(
                        this.latencySamples.length - Math.floor(this.maxSamples / 2)
                    );
                }
            }
        }

        // Per-origin tracking
        const origin = message.origin_broker || message.broker_id || 'unknown';
        this.deliveriesByOrigin[origin] = (this.deliveriesByOrigin[origin] || 0) + 1;
    }

    // ─── Called for each duplicate detected during gossip receive ─
    recordDuplicate() {
        this.duplicatesReceived += 1;
    }

    // ─── Called after each outbound gossip round ─────────────────
    recordGossipRound(messageCount) {
        this.gossipRoundsSent += 1;
        this.gossipMessagesSent += messageCount;
    }

    // ─── Compute latency percentiles ─────────────────────────────
    _percentile(sortedArr, p) {
        if (sortedArr.length === 0) return 0;
        const idx = Math.ceil((p / 100) * sortedArr.length) - 1;
        return sortedArr[Math.max(0, idx)];
    }

    getLatencyStats() {
        if (this.latencySamples.length === 0) {
            return { count: 0, min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 };
        }
        const sorted = [...this.latencySamples].sort((a, b) => a - b);
        const sum = sorted.reduce((a, b) => a + b, 0);
        return {
            count: sorted.length,
            min: sorted[0],
            max: sorted[sorted.length - 1],
            avg: parseFloat((sum / sorted.length).toFixed(2)),
            p50: this._percentile(sorted, 50),
            p95: this._percentile(sorted, 95),
            p99: this._percentile(sorted, 99),
        };
    }

    // ─── Sliding-window throughput ────────────────────────────────
    _pruneWindow(arr) {
        const cutoff = Date.now() - this.windowMs;
        while (arr.length > 0 && arr[0] < cutoff) {
            arr.shift();
        }
    }

    getThroughput() {
        this._pruneWindow(this.publishTimestamps);
        this._pruneWindow(this.deliveryTimestamps);
        const windowSec = this.windowMs / 1000;
        return {
            window_seconds: windowSec,
            publishes_in_window: this.publishTimestamps.length,
            deliveries_in_window: this.deliveryTimestamps.length,
            publish_rate_per_sec: parseFloat((this.publishTimestamps.length / windowSec).toFixed(2)),
            delivery_rate_per_sec: parseFloat((this.deliveryTimestamps.length / windowSec).toFixed(2)),
        };
    }

    // ─── Full metrics snapshot ────────────────────────────────────
    getMetrics() {
        const latency = this.getLatencyStats();
        const throughput = this.getThroughput();
        const uptimeMs = Date.now() - this.startTime;

        // Reliability ratio: for non-publisher brokers (eventsPublished === 0),
        // we can't compute a local ratio — but we report the raw counts so the
        // test harness can compare across the cluster.
        const deliveryRatio = this.eventsPublished > 0
            ? parseFloat((this.eventsDelivered / this.eventsPublished).toFixed(4))
            : null;

        return {
            broker_id: this.brokerId,
            uptime_ms: uptimeMs,
            uptime_human: `${Math.floor(uptimeMs / 1000)}s`,

            reliability: {
                events_published: this.eventsPublished,
                events_delivered: this.eventsDelivered,
                delivery_ratio: deliveryRatio,
                duplicates_received: this.duplicatesReceived,
                gossip_rounds_sent: this.gossipRoundsSent,
                gossip_messages_sent: this.gossipMessagesSent,
            },

            latency,
            throughput,

            per_broker_deliveries: this.deliveriesByOrigin,
        };
    }

    // ─── Reset for clean test runs ───────────────────────────────
    reset() {
        this.latencySamples = [];
        this.eventsPublished = 0;
        this.eventsDelivered = 0;
        this.duplicatesReceived = 0;
        this.gossipRoundsSent = 0;
        this.gossipMessagesSent = 0;
        this.publishTimestamps = [];
        this.deliveryTimestamps = [];
        this.deliveriesByOrigin = {};
        this.startTime = Date.now();
    }
}

module.exports = MetricsCollector;