const express = require('express');
const pool = require('../db');
const authenticateToken = require('../middleware/auth');

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
        const result = await pool.query(
            'INSERT INTO subscriptions (user_id, genre, artist, city, state, start_date, end_date) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *',
            [userId, genre || null, artist || null, city, state, start_date, end_date]
        );
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

//delete a subscription by id
router.delete('/:id', authenticateToken, async (req, res) => {
    try {
        const result = await pool.query(
            'DELETE FROM subscriptions WHERE id = $1 AND user_id = $2 RETURNING *', 
            [req.params.id, req.userId]
        );
        if (result.rows.length === 0) {
            return res.status(404).json({ message: 'Subscription not found' });
        }
        res.json({ message: 'Subscription deleted successfully', subscription: result.rows[0] });
    } catch (err) {
        console.error('Error deleting subscription:', err);
        res.status(500).json({ message: 'Internal server error' });
    }
});

module.exports = router;