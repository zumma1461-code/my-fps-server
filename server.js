const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

// 방 관리를 위한 객체 { roomCode: { players: {}, score: { p1: 0, p2: 0 }, round: 1, status: 'waiting' } }
const rooms = {};

// 3자리 방 코드 생성 함수 (100~999)
function generateRoomCode() {
  let code;
  do {
    code = Math.floor(100 + Math.random() * 900).toString();
  } while (rooms[code]);
  return code;
}

io.on('connection', (socket) => {
  console.log('클라이언트 접속:', socket.id);

  // 1. 서버 목록 요청
  socket.on('getRoomList', () => {
    const roomList = Object.keys(rooms).map(code => ({
      code: code,
      playerCount: Object.keys(rooms[code].players).length,
      status: rooms[code].status
    }));
    socket.emit('roomListUpdate', roomList);
  });

  // 2. 방 생성 요청
  socket.on('createRoom', () => {
    const roomCode = generateRoomCode();
    rooms[roomCode] = {
      code: roomCode,
      players: {},
      score: {}, // socket.id -> round count
      status: 'waiting'
    };

    joinRoomLogic(socket, roomCode);
  });

  // 3. 코드로 방 참가 요청
  socket.on('joinRoom', (roomCode) => {
    if (!rooms[roomCode]) {
      socket.emit('errorMsg', '존재하지 않는 방 코드입니다.');
      return;
    }
    if (Object.keys(rooms[roomCode].players).length >= 2) {
      socket.emit('errorMsg', '방이 이미 가득 찼습니다 (1v1만 가능).');
      return;
    }
    joinRoomLogic(socket, roomCode);
  });

  function joinRoomLogic(socket, roomCode) {
    const room = rooms[roomCode];
    socket.join(roomCode);
    socket.roomCode = roomCode;

    const isFirst = Object.keys(room.players).length === 0;
    const startX = isFirst ? -10 : 10;

    room.players[socket.id] = {
      id: socket.id,
      x: startX,
      y: 1.6,
      z: 0,
      yaw: 0,
      pitch: 0,
      hp: 100,
      team: isFirst ? 1 : 2
    };
    room.score[socket.id] = 0;

    socket.emit('roomJoined', {
      roomCode: roomCode,
      myId: socket.id,
      players: room.players,
      score: room.score
    });

    // 상대방에게 알림
    socket.to(roomCode).emit('playerJoined', room.players[socket.id]);

    // 2명 모두 참여시 게임 시작
    if (Object.keys(room.players).length === 2) {
      room.status = 'playing';
      io.to(roomCode).emit('gameStart', {
        players: room.players,
        score: room.score
      });
    }

    broadcastRoomList();
  }

  // 4. 위치 및 시점 동기화
  socket.on('move', (data) => {
    const roomCode = socket.roomCode;
    if (roomCode && rooms[roomCode] && rooms[roomCode].players[socket.id]) {
      const p = rooms[roomCode].players[socket.id];
      p.x = data.x;
      p.y = data.y;
      p.z = data.z;
      p.yaw = data.yaw;
      p.pitch = data.pitch;

      socket.to(roomCode).emit('playerMoved', {
        id: socket.id,
        x: p.x, y: p.y, z: p.z,
        yaw: p.yaw, pitch: p.pitch
      });
    }
  });

  // 5. 사격 및 데미지 판정 브로드캐스트
  socket.on('shoot', (shootData) => {
    const roomCode = socket.roomCode;
    if (roomCode) {
      socket.to(roomCode).emit('playerFired', {
        id: socket.id,
        origin: shootData.origin,
        direction: shootData.direction
      });
    }
  });

  // 6. 히트(사격 맞춤) 서버 검증 및 HP 차감
  socket.on('hitPlayer', (hitData) => {
    const roomCode = socket.roomCode;
    if (!roomCode || !rooms[roomCode]) return;

    const room = rooms[roomCode];
    const targetId = hitData.targetId;
    const target = room.players[targetId];

    if (target && target.hp > 0) {
      target.hp -= hitData.damage;

      // 피격자 및 타격자에게 피격 상태 전달
      io.to(roomCode).emit('playerDamaged', {
        targetId: targetId,
        attackerId: socket.id,
        damage: hitData.damage,
        bodyPart: hitData.bodyPart,
        currentHp: Math.max(0, target.hp),
        hitPosition: hitData.hitPosition
      });

      // 체력이 0 이하가 되면 라운드 종료 처리
      if (target.hp <= 0) {
        room.score[socket.id] += 1; // 승자 점수 추가
        
        const isMatchEnd = room.score[socket.id] >= 5; // 5라운드 선승

        io.to(roomCode).emit('roundEnded', {
          winnerId: socket.id,
          loserId: targetId,
          score: room.score,
          isMatchEnd: isMatchEnd
        });

        if (!isMatchEnd) {
          // 라운드 리셋 (3초 후 재시작)
          setTimeout(() => {
            if (rooms[roomCode]) {
              Object.keys(rooms[roomCode].players).forEach((pId, idx) => {
                rooms[roomCode].players[pId].hp = 100;
                rooms[roomCode].players[pId].x = (idx === 0) ? -10 : 10;
                rooms[roomCode].players[pId].z = 0;
              });
              io.to(roomCode).emit('roundStart', {
                players: rooms[roomCode].players
              });
            }
          }, 3000);
        }
      }
    }
  });

  // 7. 연결 해제
  socket.on('disconnect', () => {
    const roomCode = socket.roomCode;
    if (roomCode && rooms[roomCode]) {
      delete rooms[roomCode].players[socket.id];
      delete rooms[roomCode].score[socket.id];

      io.to(roomCode).emit('playerLeft', socket.id);

      if (Object.keys(rooms[roomCode].players).length === 0) {
        delete rooms[roomCode];
      } else {
        rooms[roomCode].status = 'waiting';
      }
      broadcastRoomList();
    }
  });
});

function broadcastRoomList() {
  const roomList = Object.keys(rooms).map(code => ({
    code: code,
    playerCount: Object.keys(rooms[code].players).length,
    status: rooms[code].status
  }));
  io.emit('roomListUpdate', roomList);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`발로란트 1v1 FPS 게임 서버가 포트 ${PORT}에서 작동 중입니다.`);
});
