const WebSocket = require('ws');
const jwt = require('jsonwebtoken');

const clients = new Map();

function setupWebSocket(server) {
    const wss = new WebSocket.Server({ server });

    wss.on('connection', (ws, req) => {
        const url = new URL (req.url, `http://localhost`);
        const token = url.searchParams.get('token');

        if (!token) {
            ws.close(4001, 'Authentication token required');
            return;
        }

        try {
            const decoded = jwt.verify(token, process.env.JWT_SECRET);
            const userId = decoded.id;

            clients.set(userId, ws);
            console.log(`User ${userId} connected via WebSocket`);

            ws.send(JSON.stringify({
                type: 'connected',
                message: 'WebSocket connection established'
            }));

            ws.on('close', () => {
                clients.delete(userId);
                console.log(`User ${userId} disconnected from WebSocket`);
            });

        } catch (err) {
            console.error('WebSocket authentication failed:', err);
            ws.close(4002, 'Invalid authentication token');
        }
    });
}

function sendNotification(userId, notification) {
    const ws = clients.get(userId);
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(notification));
        return true;
    }
    return false;
}
module.exports = { setupWebSocket, sendNotification };