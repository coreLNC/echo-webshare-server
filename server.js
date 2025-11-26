/**
 * Echo WebShare - Signaling Server
 * Copyright © 2025 David Ayobami Okeleye. All Rights Reserved.
 */

const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;

// Storage
const clients = new Map();
const rooms = new Map();

// Generate IDs
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

// Send to client
function send(clientId, msg) {
  const client = clients.get(clientId);
  if (client?.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(msg));
  }
}

// Broadcast to room
function broadcast(roomCode, msg, excludeId = null) {
  const room = rooms.get(roomCode);
  if (!room) return;
  
  room.viewers.forEach(viewerId => {
    if (viewerId !== excludeId) send(viewerId, msg);
  });
}

// Health check server
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    clients: clients.size,
    rooms: rooms.size
  }));
});

// WebSocket server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  const clientId = generateId();
  clients.set(clientId, ws);
  ws.clientId = clientId;
  ws.roomCode = null;
  
  console.log(`Client connected: ${clientId}`);
  
  send(clientId, { type: 'connected', clientId });
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      handleMessage(ws, clientId, msg);
    } catch (err) {
      console.error('Message error:', err);
    }
  });
  
  ws.on('close', () => {
    console.log(`Client disconnected: ${clientId}`);
    
    // Clean up rooms
    for (const [code, room] of rooms.entries()) {
      if (room.hostId === clientId) {
        broadcast(code, { type: 'host-left' });
        rooms.delete(code);
      } else if (room.viewers.has(clientId)) {
        room.viewers.delete(clientId);
        send(room.hostId, { type: 'viewer-left', viewerId: clientId });
      }
    }
    
    clients.delete(clientId);
  });
});

function handleMessage(ws, clientId, msg) {
  switch (msg.type) {
    case 'create-room':
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
      
    case 'join-room':
      const roomCode = msg.roomCode?.toUpperCase();
      const room = rooms.get(roomCode);
      
      if (!room) {
        send(clientId, { type: 'error', message: 'Room not found' });
        return;
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
      
    case 'leave-room':
      if (!ws.roomCode) return;
      
      const leaveRoom = rooms.get(ws.roomCode);
      if (!leaveRoom) return;
      
      if (leaveRoom.hostId === clientId) {
        broadcast(ws.roomCode, { type: 'host-left' });
        rooms.delete(ws.roomCode);
      } else {
        leaveRoom.viewers.delete(clientId);
        send(leaveRoom.hostId, { type: 'viewer-left', viewerId: clientId });
      }
      
      ws.roomCode = null;
      break;
      
    case 'offer':
      send(msg.target, { type: 'offer', from: clientId, offer: msg.offer });
      break;
      
    case 'answer':
      send(msg.target, { type: 'answer', from: clientId, answer: msg.answer });
      break;
      
    case 'ice-candidate':
      send(msg.target, { type: 'ice-candidate', from: clientId, candidate: msg.candidate });
      break;
      
    case 'chat':
      if (!ws.roomCode) return;
      
      const chatRoom = rooms.get(ws.roomCode);
      if (!chatRoom) return;
      
      const chatMsg = { type: 'chat', nick: msg.nick || 'Guest', text: msg.text };
      send(chatRoom.hostId, chatMsg);
      broadcast(ws.roomCode, chatMsg);
      break;
      
    case 'share-media':
      if (!ws.roomCode) return;
      
      const mediaRoom = rooms.get(ws.roomCode);
      if (!mediaRoom || mediaRoom.hostId !== clientId) return;
      
      mediaRoom.sharedMedia = msg.media;
      broadcast(ws.roomCode, { type: 'media-shared', media: msg.media });
      console.log(`Media shared in ${ws.roomCode}`);
      break;
  }
}

// Heartbeat
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
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
