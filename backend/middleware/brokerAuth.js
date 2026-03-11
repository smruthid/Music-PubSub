 function authenticateBroker(req, res, next) {
    const secret = process.env.BROKER_SECRET;

    if (!secret) {
        return next();
    }

    const provided = req.headers['x-broker-secret'];

    if (!provided) {
        console.warn(`[BrokerAuth] Rejected request to ${req.path} — missing X-Broker-Secret header`);
        return res.status(403).json({ error: 'Broker authentication required' });
    }

    if (provided !== secret) {
        console.warn(`[BrokerAuth] Rejected request to ${req.path} — invalid secret`);
        return res.status(403).json({ error: 'Invalid broker secret' });
    }

    next();
}

module.exports = authenticateBroker;
