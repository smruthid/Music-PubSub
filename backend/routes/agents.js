const express = require('express');
const pool = require('../db');
const axios = require('axios');
const authenticateToken = require('../middleware/auth');
const MobileAgent = require('../src/models/MobileAgent');

module.exports = (brokerId, port, registry) => {
    const router = express.Router();

    router.post('/', async (req, res) => {
        try {
            const agent = req.body;

            if (agent.visited_brokers.includes(brokerId)) {
                return res.json({ message: 'Agent completed', results: agent.results });
            }

            agent.visited_brokers.push(brokerId);

            const data = await runAgentTask(agent.task);
            if (!data) {
                return res.status(400).json({ message: 'Invalid task' });
            }

            agent.results.push({ brokerId: brokerId, data });

            
            const peerBrokers = registry.getPeers();
            const unvisited = peerBrokers.filter(b => !agent.visited_brokers.includes(b.id));

            for (const nextBroker of unvisited) {
                try {
                    const response = await axios.post(
                        `https://${nextBroker.host}:${nextBroker.port}/agents`,
                        agent,
                        { timeout: 3000 }
                    );
                    return res.json(response.data);
                } catch (err) {
                    agent.visited_brokers.push(nextBroker.id);
                    console.log(`[${brokerId}] Agent: peer ${nextBroker.id} unreachable, skipping`);
                }
            }

            
            return res.json({ message: 'Agent completed', results: agent.results });
        } catch (err) {
            console.error(`[${brokerId}] Agent error:`, err.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    
    router.get('/trending', authenticateToken, async (req, res) => {
        try {
            const { task } = req.query;
            if (!task) {
                return res.status(400).json({ message: 'Task query parameter is required' });
            }

            
            const localData = await runAgentTask(task);
            if (!localData) {
                return res.status(400).json({ message: 'Invalid task' });
            }

            const results = [{ brokerId: brokerId, data: localData }];
            const visited = [brokerId];

            const peerBrokers = registry.getPeers();

            if (peerBrokers.length > 0) {
                const agent = new MobileAgent(task, brokerId);
                const agentData = agent.toJSON();
                agentData.visited_brokers = [...visited];
                agentData.results = [];

                let agentSent = false;
                for (const peer of peerBrokers) {
                    if (visited.includes(peer.id)) continue;
                    try {
                        const response = await axios.post(
                            `https://${peer.host}:${peer.port}/agents`,
                            agentData,
                            { timeout: 10000 }
                        );

                        if (response.data?.results) {
                            results.push(...response.data.results);
                        }
                        agentSent = true;
                        break; 
                    } catch (err) {
                        console.log(`[${brokerId}] Agent: peer ${peer.id} unreachable, trying next`);
                        visited.push(peer.id);
                        agentData.visited_brokers.push(peer.id);
                    }
                }

                if (!agentSent) {
                    console.log(`[${brokerId}] Agent: no peers reachable, returning local results only`);
                }
            }

            res.json({ message: 'Agent completed', results });
        } catch (err) {
            console.error(`[${brokerId}] Trending agents error:`, err.message);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    return router;
};


async function runAgentTask(task) {
    if (task === 'trending_artists') {
        const result = await pool.query(
            `SELECT artist, COUNT(*) AS artist_count
             FROM events
             GROUP BY artist
             ORDER BY artist_count DESC
             LIMIT 5`
        );
        return result.rows;
    } else if (task === 'trending_genres') {
        const result = await pool.query(
            `SELECT genre, COUNT(*) AS genre_count
             FROM events
             GROUP BY genre
             ORDER BY genre_count DESC
             LIMIT 5`
        );
        return result.rows;
    } else if (task === 'trending_locations') {
        const result = await pool.query(
            `SELECT city, state, COUNT(*) AS location_count
             FROM events
             GROUP BY city, state
             ORDER BY location_count DESC
             LIMIT 5`
        );
        return result.rows;
    }
    return null;
}