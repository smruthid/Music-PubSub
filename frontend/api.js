// Change this to whichever broker you want to talk to (5001 = broker-1, 5002 = broker-2, 5003 = broker-3)
const BROKER_URL = 'https://localhost:5001';

// ── Auth helpers ─────────────────────────────────────────────────────────────

function getToken() {
    return localStorage.getItem('token');
}

function getUser() {
    const raw = localStorage.getItem('user');
    return raw ? JSON.parse(raw) : null;
}

function saveAuth(token, user) {
    localStorage.setItem('token', token);
    localStorage.setItem('user', JSON.stringify(user));
}

function logout() {
    localStorage.removeItem('token');
    localStorage.removeItem('user');
    window.location.href = 'login.html';
}

// Redirect to login if there's no token — call this at the top of protected pages
function requireAuth() {
    if (!getToken()) {
        window.location.href = 'login.html';
    }
}

// ── Fetch wrapper ─────────────────────────────────────────────────────────────

async function apiFetch(path, options = {}) {
    const token = getToken();
    const headers = {
        'Content-Type': 'application/json',
        ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
        ...(options.headers || {}),
    };

    const res = await fetch(`${BROKER_URL}${path}`, { ...options, headers });
    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Request failed');
    return data;
}

// ── WebSocket ─────────────────────────────────────────────────────────────────

// Returns a connected WebSocket that delivers events to onMessage(data).
// data.type is 'notification', 'urgent_notification', or 'connected'.
function connectWebSocket(onMessage) {
    const token = getToken();
    if (!token) return null;

    // Replace http:// with ws:// (or https:// with wss://)
    const wsBase = BROKER_URL.replace(/^http/, 'ws');
    const ws = new WebSocket(`${wsBase}?token=${token}`);

    ws.addEventListener('message', (e) => {
        try {
            onMessage(JSON.parse(e.data));
        } catch (_) { /* ignore malformed frames */ }
    });

    ws.addEventListener('error', () => console.warn('WebSocket error'));
    return ws;
}
