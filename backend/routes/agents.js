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
            agent.visited_brokers.push(brokerId);

            let data;
            if(agent.task === 'trending_artists') {
                const result = await pool.query(
                    `SELECT artist, COUNT(*) AS artist_count
                    FROM events
                    GROUP BY artist
                    ORDER BY artist_count DESC
                    LIMIT 5`
                )
                data = result.rows;
            } else if(agent.task === 'trending_genres') {
                const result = await pool.query(
                    `SELECT genre, COUNT(*) AS genre_count
                     FROM events
                     GROUP BY genre
                     ORDER BY genre_count DESC
                     LIMIT 5`
                )
                data = result.rows;
            } else if(agent.task === 'trending_locations') {
                const result = await pool.query(
                    `SELECT city, state, COUNT(*) AS location_count
                     FROM events
                     GROUP BY city, state
                     ORDER BY location_count DESC
                     LIMIT 5`
                )
                data = result.rows;
            } else {
                return res.status(400).json({ message: 'Invalid task' });
            }

            agent.results.push({brokerId: brokerId, data});

            // Read the LIVE peer list from the registry instead of a static array
            const peerBrokers = registry.getPeers();

            const nextBroker = peerBrokers.find(b => !agent.visited_brokers.includes(b.id));
            if (nextBroker) {
                try {
                    const response = await axios.post(`http://${nextBroker.host}:${nextBroker.port}/agents`, agent, { timeout: 5000 });
                    return res.json(response.data);
                } catch (error) {
                    const tryAgain = peerBrokers.find(b => !agent.visited_brokers.includes(b.id) && b.id !== nextBroker.id);
                    if (tryAgain) {
                        try {
                            const response = await axios.post(`http://${tryAgain.host}:${tryAgain.port}/agents`, agent, { timeout: 5000 });
                            return res.json(response.data);
                        } catch (err) {
                            return res.status(500).json({ error: `Failed to forward to ${tryAgain.id}` });
                        }
                    } else {
                        // Can't reach any more brokers — return what we have so far
                        return res.json({ message: 'Agent completed (partial - some brokers unreachable)', results: agent.results });
                    }
                }
            } else {
                return res.json({ message: 'Agent completed', results: agent.results });
            }
        } catch (err) {
            console.error(`[${brokerId}] Agent error:`, err);
            res.status(500).json({ error: 'Internal server error' });
        }
    });

    router.get('/trending', authenticateToken, async (req, res) => {
        try {
            const { task } = req.query;
            if(!task) {
                return res.status(400).json({ message: 'Task query parameter is required' });
            }
            const agent = new MobileAgent(task, brokerId);

            // Use 127.0.0.1 instead of localhost to avoid IPv6 resolution issues in Docker
            const response = await axios.post(`http://127.0.0.1:${port}/agents`, agent.toJSON(), { timeout: 15000 });
            res.json(response.data);
        } catch (err) {
            console.error(`[${brokerId}] Trending agents error:`, err);
            res.status(500).json({ error: 'Internal server error' });
        }
    });
    return router;
}