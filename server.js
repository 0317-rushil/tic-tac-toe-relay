const http = require('http');
const { WebSocketServer } = require('ws');
const { URL } = require('url');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_MS = 25000;
const ROOM_TTL_MS = 2 * 60 * 60 * 1000; // rooms with no one connected get swept after 2h

// room code -> { sockets: Map<role, ws>, lastSeen: number }
const rooms = new Map();

function getRoom(code) {
  let room = rooms.get(code);
  if (!room) {
    room = { sockets: new Map(), lastSeen: Date.now() };
    rooms.set(code, room);
  }
  return room;
}

function sweepRooms() {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (room.sockets.size === 0 && now - room.lastSeen > ROOM_TTL_MS) {
      rooms.delete(code);
    }
  }
}
setInterval(sweepRooms, 10 * 60 * 1000).unref();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('tic-tac-toe relay ok\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const code = (url.searchParams.get('room') || '').trim().toUpperCase();

  if (!code) {
    ws.close(4000, 'missing room code');
    return;
  }

  const room = getRoom(code);
  room.lastSeen = Date.now();

  if (room.sockets.size >= 2) {
    ws.send(JSON.stringify({ type: 'room-full' }));
    ws.close(4001, 'room full');
    return;
  }

  const role = room.sockets.has('X') ? 'O' : 'X';
  room.sockets.set(role, ws);
  ws.isAlive = true;
  ws.role = role;
  ws.roomCode = code;

  ws.send(JSON.stringify({ type: 'assigned', role }));

  const other = room.sockets.get(role === 'X' ? 'O' : 'X');
  if (other && other.readyState === other.OPEN) {
    other.send(JSON.stringify({ type: 'peer-joined' }));
    ws.send(JSON.stringify({ type: 'peer-joined' }));
  }

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data) => {
    room.lastSeen = Date.now();
    const peer = room.sockets.get(ws.role === 'X' ? 'O' : 'X');
    if (peer && peer.readyState === peer.OPEN) {
      peer.send(data.toString());
    }
  });

  ws.on('close', () => {
    room.lastSeen = Date.now();
    if (room.sockets.get(ws.role) === ws) {
      room.sockets.delete(ws.role);
    }
    const peer = room.sockets.get(ws.role === 'X' ? 'O' : 'X');
    if (peer && peer.readyState === peer.OPEN) {
      peer.send(JSON.stringify({ type: 'peer-left' }));
    }
  });
});

const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, HEARTBEAT_MS);
heartbeat.unref();

server.listen(PORT, () => {
  console.log('relay listening on port ' + PORT);
});
