const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

let players = {};

io.on('connection', (socket) => {
  if (Object.keys(players).length < 2) {
    players[socket.id] = { id: socket.id, x: 0, y: 1, z: 0 };
    socket.emit('init', { id: socket.id, players });
    socket.broadcast.emit('playerJoined', players[socket.id]);
  }

  socket.on('move', (data) => {
    if (players[socket.id]) {
      players[socket.id] = data;
      socket.broadcast.emit('playerMoved', data);
    }
  });

  socket.on('disconnect', () => {
    delete players[socket.id];
    io.emit('playerLeft', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
