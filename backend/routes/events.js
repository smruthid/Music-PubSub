const express = require('express');
const pool = require('../db');
const authenticateToken = require('../middleware/auth');
const { sendNotification } = require('../websocket');

module.exports = (gossipService, lamportClock) => {
    const router = express.Router();

    // publish an event
    router.post('/', authenticateToken, async (req, res) => {
        const { title, artist, genre, city, state, venue, event_date_time, priority } = req.body;

        if (!title || !artist || !genre || !city || !state || !venue || !event_date_time) {
            return res.status(400).json({ message: 'All fields are required' });
        }

        try {
            const clock = lamportClock.increment();
            const result = await pool.query(
                'INSERT INTO events (title, artist, genre, city, state, venue, event_date_time, priority, lamport_clock) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *',
                [title, artist, genre, city, state, venue, event_date_time, priority || 'normal', clock]
            );
            const event = result.rows[0];

            // Propagate event to peer brokers via gossip
            gossipService.publishEvent(event);

            const matchingResult = await pool.query(
                `SELECT DISTINCT s.user_id
                 FROM subscriptions s
                 WHERE s.city = $1 AND s.state = $2
                 AND $5::DATE BETWEEN s.start_date AND s.end_date
                 AND (
                    (s.genre IS NULL AND s.artist IS NULL) OR
                    (s.genre IS NOT NULL AND s.genre = $3) OR
                    (s.artist IS NOT NULL AND s.artist = $4)
                 )`,
                [city, state, genre, artist, event_date_time]
            );

            const matchedUsers = matchingResult.rows.map(row => row.user_id);
            for (const userId of matchedUsers) {
                await pool.query(
                    'INSERT INTO notifications (user_id, event_id) VALUES ($1, $2)',
                    [userId, event.id]
                );
                sendNotification(userId, {
                    type: priority === 'urgent' ? 'urgent_notification' : 'notification',
                    event,
                });
            }

            res.status(201).json({
                message: 'Event published successfully',
                event,
                notified_users: matchedUsers.length,
                lamport_clock: clock,
            });

        } catch (err) {
            console.error('Error publishing event:', err);
            res.status(500).json({ message: 'Internal server error' });
        }
    });

    // get all events
    router.get('/', authenticateToken, async (_req, res) => {
        try {
            const result = await pool.query(
                'SELECT * FROM events ORDER BY published_at DESC'
            );
            res.json({ events: result.rows });
        } catch (err) {
            console.error('Error fetching events:', err);
            res.status(500).json({ message: 'Internal server error' });
        }
    });

    return router;
};
