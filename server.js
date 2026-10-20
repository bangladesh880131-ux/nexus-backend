/**
 * Nexus server: static hosting + Socket.io relay + WebRTC signaling.
 * State is in memory (restart = reset). Messages for offline users are
 * queued until they reconnect.
 */
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
// 25 MB so small media/files can travel as base64 over the socket
const io = new Server(server, { maxHttpBufferSize: 25e6, cors: { origin: '*' }, transports: ['websocket', 'polling'], pingInterval: 10000, pingTimeout: 20000 });

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_, res) => res.send('ok'));

const users = new Map();   // username -> { profile, sockets:Set, lastSeen }
const pending = new Map(); // username -> [{ev,data}]
const QUEUED = new Set(['msg', 'del', 'react', 'read']);

// Same rule as the client: no strict validation, just trim, lowercase and replace spaces
const clean = s => String(s || '').trim().toLowerCase().replace(/\s+/g, '_').slice(0, 30);
const list = () => [...users.values()].map(u => ({
  ...u.profile, online: u.sockets.size > 0, lastSeen: u.lastSeen
}));
const broadcastUsers = () => io.emit('users', list());

function sendTo(username, ev, data) {
  const u = users.get(username);
  if (u && u.sockets.size) {
    u.sockets.forEach(id => io.to(id).emit(ev, data));
    return true;
  }
  if (QUEUED.has(ev)) {
    if (!pending.has(username)) pending.set(username, []);
    const q = pending.get(username);
    q.push({ ev, data });
    if (q.length > 500) q.shift();
  }
  return false;
}

io.on('connection', socket => {
  socket.on('join', p => {
    const username = clean(p && p.username);
    if (!username) return;
    socket.user = username;
    const prev = users.get(username);
    const sockets = prev ? prev.sockets : new Set();
    sockets.add(socket.id);
    users.set(username, {
      sockets, lastSeen: Date.now(),
      profile: {
        username,
        name: String(p.name || username).slice(0, 40),
        bio: String(p.bio || '').slice(0, 140),
        avatar: typeof p.avatar === 'string' && p.avatar.length < 200000 ? p.avatar : ''
      }
    });
    (pending.get(username) || []).forEach(({ ev, data }) => socket.emit(ev, data));
    pending.delete(username);
    broadcastUsers();
  });

  socket.on('msg', (m, ack) => {
    if (!socket.user || !m || !m.to) return;
    m.from = socket.user;
    const delivered = sendTo(clean(m.to), 'msg', m);
    if (typeof ack === 'function') ack({ delivered });
  });

  // Plain relays (chat events + WebRTC signaling)
  ['typing', 'read', 'react', 'del', 'call:offer', 'call:answer', 'call:ice', 'call:end']
    .forEach(ev => socket.on(ev, d => {
      if (!socket.user || !d || !d.to) return;
      d.from = socket.user;
      const ok = sendTo(clean(d.to), ev, d);
      if (!ok && ev === 'call:offer') socket.emit('call:end', { from: d.to, offline: true });
    }));

  socket.on('disconnect', () => {
    const u = users.get(socket.user);
    if (!u) return;
    u.sockets.delete(socket.id);
    u.lastSeen = Date.now();
    broadcastUsers();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Nexus running on http://localhost:${PORT}`));
