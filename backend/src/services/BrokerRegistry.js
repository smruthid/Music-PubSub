const axios = require('../utils/brokerClient');
const os = require('os');

class BrokerRegistry {
    constructor(brokerId, brokerHost, port, lamportClock) {
        this.brokerId = brokerId;
        this.brokerHost = brokerHost; // Routable IP address
        this.port = port;
        this.lamportClock = lamportClock;
        this.peers = new Map(); // brokerId -> { id, host, port, lastSeen }
        this.onPeerAdded = null;   // callback
        this.onPeerRemoved = null; // callback
        this.cleanupInterval = 30000; // remove dead peers after 30s
    }

    start() {
        // Periodically clean up dead peers
        setInterval(() => this.cleanupDeadPeers(), this.cleanupInterval);
    }

    /**
     * Join the cluster by contacting a seed broker.
     * The seed tells us about all existing peers, and we announce ourselves.
     */
    async joinCluster(seedHost, seedPort) {
        try {
            console.log(`[${this.brokerId}] Joining cluster via seed ${seedHost}:${seedPort}...`);

            // Step 1: Register ourselves with the seed
            const headers = {};
            if (process.env.BROKER_SECRET) {
                headers['X-Broker-Secret'] = process.env.BROKER_SECRET;
            }
            const response = await axios.post(
                `https://${seedHost}:${seedPort}/api/cluster/register`,
                {
                    broker_id: this.brokerId,
                    host: this.brokerHost,
                    port: this.port,
                    lamport_clock: this.lamportClock.increment(),
                },
                { timeout: 10000, headers }
            );

            // Step 2: The seed responds with all known peers
            const existingPeers = response.data.peers || [];
            console.log(`[${this.brokerId}] Seed knows ${existingPeers.length} peer(s)`);

            for (const peer of existingPeers) {
                if (peer.id !== this.brokerId) {
                    this.addPeer(peer);

                    // Step 3: Announce ourselves to each existing peer
                    this.announceToPeer(peer).catch(() => {
                        console.warn(`[${this.brokerId}] Could not announce to ${peer.id}`);
                    });
                }
            }

            // Also add the seed itself as a peer
            const seedPeer = { id: response.data.broker_id, host: seedHost, port: seedPort };
            this.addPeer(seedPeer);

            console.log(`[${this.brokerId}] ✓ Joined cluster with ${this.peers.size} peer(s)`);
        } catch (error) {
            console.error(`[${this.brokerId}] Failed to join cluster:`, error.message);
            console.log(`[${this.brokerId}] Starting as standalone broker, will retry...`);
            // Retry joining after a delay
            setTimeout(() => this.joinCluster(seedHost, seedPort), 5000);
        }
    }

    async announceToPeer(peer) {
        try {
            const headers = {};
            if (process.env.BROKER_SECRET) {
                headers['X-Broker-Secret'] = process.env.BROKER_SECRET;
            }
            await axios.post(
                `https://${peer.host}:${peer.port}/api/cluster/register`,
                {
                    broker_id: this.brokerId,
                    host: this.brokerHost,
                    port: this.port,
                    lamport_clock: this.lamportClock.increment(),
                },
                { timeout: 5000, headers }
            );
            console.log(`[${this.brokerId}] ✓ Announced to ${peer.id}`);
        } catch (error) {
            console.warn(`[${this.brokerId}] Failed to announce to ${peer.id}:`, error.message);
        }
    }

    addPeer(peer) {
        const isNew = !this.peers.has(peer.id);
        this.peers.set(peer.id, {
            id: peer.id,
            host: peer.host,
            port: parseInt(peer.port, 10),
            lastSeen: Date.now(),
        });

        if (isNew && this.onPeerAdded) {
            this.onPeerAdded({ id: peer.id, host: peer.host, port: parseInt(peer.port, 10) });
            console.log(`[${this.brokerId}] New peer registered: ${peer.id} (cluster size: ${this.peers.size + 1})`);
        }
    }

    removePeer(brokerId) {
        if (this.peers.has(brokerId)) {
            this.peers.delete(brokerId);
            if (this.onPeerRemoved) {
                this.onPeerRemoved(brokerId);
            }
            console.log(`[${this.brokerId}] Peer removed: ${brokerId} (cluster size: ${this.peers.size + 1})`);
        }
    }

    refreshPeer(brokerId) {
        if (this.peers.has(brokerId)) {
            this.peers.get(brokerId).lastSeen = Date.now();
        }
    }

    cleanupDeadPeers() {
        const now = Date.now();
        for (const [id, peer] of this.peers) {
            if (now - peer.lastSeen > this.cleanupInterval * 2) {
                this.removePeer(id);
            }
        }
    }

    getPeers() {
        return Array.from(this.peers.values());
    }

    getPeerCount() {
        return this.peers.size;
    }
}

module.exports = BrokerRegistry;