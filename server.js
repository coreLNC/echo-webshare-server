/**
 * Echo WebShare - Signaling Server
 * Copyright © 2025 David Ayobami Okeleye. All Rights Reserved.
 */

const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_MESSAGE_BYTES = Number(process.env.MAX_MESSAGE_BYTES || 16 * 1024 * 1024);
const MAX_MEDIA_BYTES = Number(process.env.MAX_MEDIA_BYTES || 8 * 1024 * 1024);

const clients = new Map();
const rooms = new Map();

function generateId() {
  return crypto.randomBytes(16).toString('hex');
}

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;

  do {
    code = Array.from({ length: 6 }, () =>
      chars[Math.floor(Math.random() * chars.length)]
    ).join('');
  } while (rooms.has(code));

  return code;
}

function send(clientId, msg) {
  const client = clients.get(clientId);
  if (client?.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(msg));
  }
}

function broadcast(roomCode, msg, excludeId = null) {
  const room = rooms.get(roomCode);
  if (!room) return;

  room.viewers.forEach(viewerId => {
    if (viewerId !== excludeId) send(viewerId, msg);
  });
}

function getDataUrlSize(dataUrl) {
  if (typeof dataUrl !== 'string') return 0;

  const commaIndex = dataUrl.indexOf(',');
  const payload = commaIndex === -1 ? dataUrl : dataUrl.slice(commaIndex + 1);
  const padding = payload.endsWith('==') ? 2 : payload.endsWith('=') ? 1 : 0;
  return Math.max(0, Math.floor((payload.length * 3) / 4) - padding);
}

function cleanupClient(clientId) {
  const ws = clients.get(clientId);

  for (const [code, room] of rooms.entries()) {
    if (room.hostId === clientId) {
      broadcast(code, { type: 'host-left' });
      rooms.delete(code);
    } else if (room.viewers.has(clientId)) {
      room.viewers.delete(clientId);
      send(room.hostId, { type: 'viewer-left', viewerId: clientId });
    }
  }

  if (ws) ws.roomCode = null;
  clients.delete(clientId);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    clients: clients.size,
    rooms: rooms.size
  }));
});

const wss = new WebSocket.Server({
  server,
  maxPayload: MAX_MESSAGE_BYTES
});

wss.on('connection', (ws) => {
  const clientId = generateId();
  clients.set(clientId, ws);
  ws.clientId = clientId;
  ws.roomCode = null;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  console.log(`Client connected: ${clientId}`);
  send(clientId, { type: 'connected', clientId });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      handleMessage(ws, clientId, msg);
    } catch (err) {
      console.error('Message error:', err);
      send(clientId, { type: 'error', message: 'Invalid message format' });
    }
  });

  ws.on('error', (err) => {
    console.error(`Socket error for ${clientId}:`, err.message);
  });

  ws.on('close', () => {
    console.log(`Client disconnected: ${clientId}`);
    cleanupClient(clientId);
  });
});

function handleMessage(ws, clientId, msg) {
  switch (msg.type) {
    case 'create-room': {
      if (ws.roomCode) {
        handleMessage(ws, clientId, { type: 'leave-room' });
      }

      const code = generateRoomCode();
      rooms.set(code, {
        hostId: clientId,
        viewers: new Set(),
        sharedMedia: null
      });

      ws.roomCode = code;
      send(clientId, { type: 'room-created', roomCode: code });
      console.log(`Room created: ${code}`);
      break;
    }

    case 'join-room': {
      const roomCode = String(msg.roomCode || '').trim().toUpperCase();
      const room = rooms.get(roomCode);

      if (!room) {
        send(clientId, { type: 'error', message: 'Room not found' });
        return;
      }

      if (room.hostId === clientId) {
        send(clientId, { type: 'error', message: 'Host cannot join their own room' });
        return;
      }

      if (ws.roomCode && ws.roomCode !== roomCode) {
        handleMessage(ws, clientId, { type: 'leave-room' });
      }

      room.viewers.add(clientId);
      ws.roomCode = roomCode;

      send(clientId, { type: 'joined-room', roomCode });
      send(room.hostId, { type: 'viewer-joined', viewerId: clientId });

      if (room.sharedMedia) {
        send(clientId, { type: 'media-shared', media: room.sharedMedia });
      }

      console.log(`Client ${clientId} joined ${roomCode}`);
      break;
    }

    case 'leave-room': {
      if (!ws.roomCode) return;

      const leaveRoom = rooms.get(ws.roomCode);
      if (!leaveRoom) {
        ws.roomCode = null;
        return;
      }

      if (leaveRoom.hostId === clientId) {
        broadcast(ws.roomCode, { type: 'host-left' });
        rooms.delete(ws.roomCode);
      } else {
        leaveRoom.viewers.delete(clientId);
        send(leaveRoom.hostId, { type: 'viewer-left', viewerId: clientId });
      }

      ws.roomCode = null;
      break;
    }

    case 'offer':
      send(msg.target, { type: 'offer', from: clientId, offer: msg.offer });
      break;

    case 'answer':
      send(msg.target, { type: 'answer', from: clientId, answer: msg.answer });
      break;

    case 'ice-candidate':
      send(msg.target, { type: 'ice-candidate', from: clientId, candidate: msg.candidate });
      break;

    case 'chat': {
      if (!ws.roomCode) return;

      const chatRoom = rooms.get(ws.roomCode);
      if (!chatRoom) return;

      const chatMsg = {
        type: 'chat',
        nick: String(msg.nick || 'Guest').slice(0, 40),
        text: String(msg.text || '').slice(0, 1000)
      };

      send(chatRoom.hostId, chatMsg);
      broadcast(ws.roomCode, chatMsg);
      break;
    }

    case 'share-media': {
      if (!ws.roomCode) return;

      const mediaRoom = rooms.get(ws.roomCode);
      if (!mediaRoom || mediaRoom.hostId !== clientId) return;

      const media = msg.media;
      if (!media?.data || typeof media.name !== 'string') {
        send(clientId, { type: 'error', message: 'Invalid media payload' });
        return;
      }

      if (getDataUrlSize(media.data) > MAX_MEDIA_BYTES) {
        send(clientId, {
          type: 'error',
          message: `File is too large. Maximum size is ${Math.floor(MAX_MEDIA_BYTES / (1024 * 1024))} MB.`
        });
        return;
      }

      mediaRoom.sharedMedia = {
        name: media.name.slice(0, 180),
        type: String(media.type || 'application/octet-stream').slice(0, 120),
        size: Number(media.size || getDataUrlSize(media.data)),
        data: media.data
      };

      broadcast(ws.roomCode, { type: 'media-shared', media: mediaRoom.sharedMedia });
      console.log(`Media shared in ${ws.roomCode}`);
      break;
    }

    default:
      send(clientId, { type: 'error', message: 'Unknown message type' });
      break;
  }
}

setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) {
      cleanupClient(ws.clientId);
      ws.terminate();
      return;
    }

    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════╗
║  Echo WebShare Server              ║
║  Port: ${PORT}                     ║
║  Status: Running ✓                 ║
╚════════════════════════════════════╝
  `);
});
