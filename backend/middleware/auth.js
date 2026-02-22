const jwt = require('jsonwebtoken');

function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];

    if (!authHeader) {
        return res.status(401).json({ message: 'Authorization header missing' });
    }

    const token = authHeader.split(' ')[1];
    if (!token) {
        return res.status(401).json({ message: 'Token missing' });
    }
    
    try {
        const verificationResult = jwt.verify(token, process.env.JWT_SECRET);
        req.userId = verificationResult.id; 
        next();
    } catch (err) {
        console.error('Error verifying token:', err);
        return res.status(403).json({ message: 'Invalid token' });
    }
}

module.exports = authenticateToken;