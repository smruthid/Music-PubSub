const BROKER_URL = 'https://localhost:5001';

//Auth helpers 
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

//Redirect to login if there's no token
function requireAuth() {
    if (!getToken()) {
        window.location.href = 'login.html';
    }
}

//Fetch wrapper 
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

//WebSocket helper for live notifications
function connectWebSocket(onMessage) {
    const token = getToken();
    if (!token) return null;

    const wsBase = BROKER_URL.replace(/^http/, 'ws');
    const ws = new WebSocket(`${wsBase}?token=${token}`);

    ws.addEventListener('message', (e) => {
        try {
            onMessage(JSON.parse(e.data));
        } catch (_) { }
    });

    ws.addEventListener('error', () => console.warn('WebSocket error'));
    return ws;
}
