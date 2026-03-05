/**
 * Pre-configured axios instance for broker-to-broker HTTPS calls.
 * Uses rejectUnauthorized: false to accept self-signed certificates
 * within the Docker network.
 */
const axios = require('axios');
const https = require('https');

const agent = new https.Agent({ rejectUnauthorized: false });

module.exports = axios.create({ httpsAgent: agent });
