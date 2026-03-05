const express = require('express');
const pool = require('../db');
const authenticateToken = require('../middleware/auth');
const { sendNotification } = require('../websocket');


module.exports = (lamportClock) => {
    const router = express.Router();

    //create a subscription
    router.post('/', authenticateToken, async (req, res) => {
        const { genre, artist, city, state, start_date, end_date } = req.body;
        const userId = req.userId;

        if(!city || !state || !start_date || !end_date) {
            return res.status(400).json({ message: 'City, state, start date, and end date are required' });
        }

        if(genre && artist) {
            return res.status(400).json({ message: 'Please specify either genre or artist, not both' });
        }

        try {
            let lamp_clock_value = lamportClock.increment();
            const result = await pool.query(
                'INSERT INTO subscriptions (user_id, genre, artist, city, state, start_date, end_date, lamport_clock) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
                [userId, genre || null, artist || null, city, state, start_date, end_date, lamp_clock_value]
            );

            // Backfill: find existing events that match this new subscription and notify immediately
            const existingEvents = await pool.query(
                `SELECT * FROM events
                 WHERE city = $1 AND state = $2
                 AND event_date_time >= NOW()
                 AND (
                     ($3::VARCHAR IS NULL AND $4::VARCHAR IS NULL) OR
                     ($3::VARCHAR IS NOT NULL AND genre = $3) OR
                     ($4::VARCHAR IS NOT NULL AND artist = $4)
                 )
                 ORDER BY event_date_time ASC`,
                [city, state, genre || null, artist || null]
            );

            for (const event of existingEvents.rows) {
                const exists = await pool.query(
                    'SELECT id FROM notifications WHERE user_id = $1 AND event_id = $2',
                    [userId, event.id]
                );
                if (exists.rows.length === 0) {
                    await pool.query(
                        'INSERT INTO notifications (user_id, event_id) VALUES ($1, $2)',
                        [userId, event.id]
                    );
                }
                sendNotification(userId, {
                    type: event.priority === 'urgent' ? 'urgent_notification' : 'notification',
                    event,
                    source: 'backfill',
                });
            }

            res.status(201).json({ message: 'Subscription created successfully', subscription: result.rows[0] });
        } catch (err) {
            console.error('Error creating subscription:', err);
            if (err.code === '23514') { // Check constraint violation
                return res.status(400).json({ message: 'Invalid subscription criteria.' });
            }
            if (err.code === '23505') { // Unique violation
                return res.status(400).json({ message: 'You already have a subscription with these criteria.' });
            }
            res.status(500).json({ message: 'Internal server error' });
        }
    });

    //get all subscriptions for the authenticated user
    router.get('/', authenticateToken, async (req, res) => {
        try {
            const result = await pool.query(
                'SELECT * FROM subscriptions WHERE user_id = $1', 
                [req.userId]
            );
            res.json({ subscriptions: result.rows });

        } catch (err) {
            console.error('Error fetching subscriptions:', err);
            res.status(500).json({ message: 'Internal server error' });
        }
    });

    // get past notifications for the current user
    router.get('/notifications', authenticateToken, async (req, res) => {
        try {
            const result = await pool.query(
                `SELECT e.*, n.sent_at
                 FROM notifications n
                 JOIN events e ON n.event_id = e.id
                 WHERE n.user_id = $1
                 ORDER BY n.sent_at DESC`,
                [req.userId]
            );
            res.json({ notifications: result.rows });
        } catch (err) {
            console.error('Error fetching notifications:', err);
            res.status(500).json({ message: 'Internal server error' });
        }
    });

    //delete a subscription by id
    router.delete('/:id', authenticateToken, async (req, res) => {
        try {
            let lamp_clock_value = lamportClock.increment();
            const result = await pool.query(
                'DELETE FROM subscriptions WHERE id = $1 AND user_id = $2 RETURNING *', 
                [req.params.id, req.userId]
            );
            if (result.rows.length === 0) {
                return res.status(404).json({ message: 'Subscription not found' });
            }
            res.json({ message: 'Subscription deleted successfully', subscription: result.rows[0], lamport_clock: lamp_clock_value });
        } catch (err) {
            console.error('Error deleting subscription:', err);
            res.status(500).json({ message: 'Internal server error' });
        }
    });
    return router;
};