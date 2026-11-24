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

// Optional invite secret required to create rooms. Gate is off when unset.
const ROOM_SECRET = process.env.ROOM_SECRET || '';

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

function secretMatches(provided) {
  if (!ROOM_SECRET) return true;
  const a = Buffer.from(String(provided || ''));
  const b = Buffer.from(ROOM_SECRET);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function inSameRoom(clientId, targetId) {
  const sender = clients.get(clientId);
  const target = clients.get(targetId);
  return !!(sender?.roomCode && target && target.roomCode === sender.roomCode);
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

wss.on('connection', (ws, req) => {
  const clientId = generateId();
  clients.set(clientId, ws);
  ws.clientId = clientId;
  ws.roomCode = null;
  ws.isAlive = true;
  // Only trust x-forwarded-for when running behind a proxy you control.
  ws.ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown')
    .split(',')[0]
    .trim();

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

/* ---------------- TURN credential issuance (Cloudflare Realtime) ---------------- */

const TURN_TTL_SECONDS = 3600;
const TURN_WINDOW_MS = 60_000;
const TURN_SOCKET_COOLDOWN_MS = 60_000;
const TURN_PER_IP = 10;
const TURN_GLOBAL = 120;

const turnByIp = new Map();
let turnGlobal = { start: 0, count: 0 };

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of turnByIp) {
    if (now - entry.start >= TURN_WINDOW_MS) turnByIp.delete(ip);
  }
}, TURN_WINDOW_MS).unref();

function allowTurnRequest(ws, now) {
  if (now < (ws.nextTurnRequestAt ?? 0)) return false;

  if (now - turnGlobal.start >= TURN_WINDOW_MS) turnGlobal = { start: now, count: 0 };
  if (turnGlobal.count >= TURN_GLOBAL) return false;

  const ip = ws.ip || 'unknown';
  let entry = turnByIp.get(ip);
  if (!entry || now - entry.start >= TURN_WINDOW_MS) {
    entry = { start: now, count: 0 };
    turnByIp.set(ip, entry);
  }
  if (entry.count >= TURN_PER_IP) return false;

  entry.count++;
  turnGlobal.count++;
  ws.nextTurnRequestAt = now + TURN_SOCKET_COOLDOWN_MS;
  return true;
}

async function sendTurnCredentials(ws, clientId) {
  const roomCode = ws.roomCode;
  const room = rooms.get(roomCode);
  const isStillInRoom = () =>
    clients.get(clientId) === ws &&
    ws.roomCode === roomCode &&
    room &&
    rooms.get(roomCode) === room &&
    (room.hostId === clientId || room.viewers.has(clientId));

  const replyError = (message) =>
    send(clientId, { type: 'turn-credentials-error', message });

  if (!isStillInRoom()) {
    replyError('Create or join a room before requesting TURN credentials.');
    return;
  }

  const keyId = process.env.TURN_KEY_ID;
  const apiToken = process.env.TURN_KEY_API_TOKEN;
  if (!keyId || !apiToken) {
    replyError('TURN is not configured.');
    return;
  }

  if (!allowTurnRequest(ws, Date.now())) {
    replyError('Too many TURN requests. Try again shortly.');
    return;
  }

  try {
    const response = await fetch(
      `https://rtc.live.cloudflare.com/v1/turn/keys/${encodeURIComponent(keyId)}/credentials/generate-ice-servers`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ ttl: TURN_TTL_SECONDS }),
        signal: AbortSignal.timeout(5000)
      }
    );

    if (!response.ok) throw new Error(`TURN provider returned ${response.status}`);

    const { iceServers } = await response.json();
    if (!Array.isArray(iceServers) || iceServers.length === 0) {
      throw new Error('TURN provider returned no ICE servers');
    }

    if (isStillInRoom()) {
      send(clientId, {
        type: 'turn-credentials',
        iceServers,
        ttlSeconds: TURN_TTL_SECONDS
      });
    }
  } catch {
    ws.nextTurnRequestAt = 0; // allow an immediate retry after a failure
    console.warn('TURN credential request failed.');
    if (isStillInRoom()) replyError('Could not get TURN credentials.');
  }
}

/* ---------------- Message handling ---------------- */

function handleMessage(ws, clientId, msg) {
  switch (msg.type) {
    case 'create-room': {
      if (!secretMatches(msg.secret)) {
        send(clientId, { type: 'error', message: 'Not authorized to create rooms' });
        return;
      }

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

    case 'request-turn-credentials':
      void sendTurnCredentials(ws, clientId);
      break;

    case 'offer':
      if (inSameRoom(clientId, msg.target)) {
        send(msg.target, { type: 'offer', from: clientId, offer: msg.offer });
      }
      break;

    case 'answer':
      if (inSameRoom(clientId, msg.target)) {
        send(msg.target, { type: 'answer', from: clientId, answer: msg.answer });
      }
      break;

    case 'ice-candidate':
      if (inSameRoom(clientId, msg.target)) {
        send(msg.target, { type: 'ice-candidate', from: clientId, candidate: msg.candidate });
      }
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
