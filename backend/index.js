const express = require('express');
const http = require('http');
const { setupWebSocket } = require('./websocket');

require('dotenv').config();

const authRoutes = require('./routes/auth');
const subscriptionRoutes = require('./routes/subscriptions');
const eventRoutes = require('./routes/events');

const app = express();
app.use(express.json());

app.use('/auth', authRoutes);
app.use('/subscriptions', subscriptionRoutes);
app.use('/events', eventRoutes);

const server = http.createServer(app);
setupWebSocket(server);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});