const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// 생성된 방 목록 관리 객체
let rooms = {};

function getPublicRoomList() {
    return Object.keys(rooms).map(code => ({
        code: code,
        name: rooms[code].name,
        count: rooms[code].players.length
    }));
}

io.on('connection', (socket) => {
    // 연결 시 현재 방 목록 전송
    socket.emit('updateRoomList', getPublicRoomList());

    // 1. 방 만들기
    socket.on('createRoom', (data) => {
        const roomCode = data.code;
        // 방 이름이 비어있으면 대체명 생성
        const roomName = (data.name && data.name.trim() !== '') 
            ? data.name.trim() 
            : `매치 - ${roomCode}`;

        rooms[roomCode] = {
            name: roomName,
            players: [socket.id]
        };

        socket.join(roomCode);
        socket.currentRoom = roomCode;
        io.emit('updateRoomList', getPublicRoomList());
    });

    // 2. 방 참가하기
    socket.on('joinRoom', (data) => {
        const roomCode = data.code;
        const room = rooms[roomCode];

        if (!room) {
            socket.emit('errorMsg', '존재하지 않는 방입니다.');
            return;
        }

        if (room.players.length >= 2) {
            socket.emit('errorMsg', '방이 이미 가득 찼습니다.');
            return;
        }

        room.players.push(socket.id);
        socket.join(roomCode);
        socket.currentRoom = roomCode;

        io.emit('updateRoomList', getPublicRoomList());
        io.to(roomCode).emit('gameStart');
    });

    // 3. 방 나가기 및 취소 (유령 방 문제 완벽 해결)
    socket.on('leaveRoom', (data) => {
        const roomCode = data.code || socket.currentRoom;
        if (roomCode && rooms[roomCode]) {
            socket.leave(roomCode);
            
            // 플레이어 목록에서 제거
            rooms[roomCode].players = rooms[roomCode].players.filter(id => id !== socket.id);
            
            // 방에 남은 인원이 없으면 방 삭제
            if (rooms[roomCode].players.length === 0) {
                delete rooms[roomCode];
            } else {
                io.to(roomCode).emit('playerLeft');
            }

            socket.currentRoom = null;
            io.emit('updateRoomList', getPublicRoomList());
        }
    });

    // 동기화 이벤트
    socket.on('move', (data) => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).emit('playerMoved', data);
        }
    });

    socket.on('weaponChange', (data) => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).emit('enemyWeaponChanged', data);
        }
    });

    socket.on('enemyShoot', (data) => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).emit('enemyFired', data);
        }
    });

    socket.on('hit', (data) => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).emit('takeDamage', data);
        }
    });

    socket.on('iDied', () => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).emit('enemyKilled');
        }
    });

    // 접속 해제 처리
    socket.on('disconnect', () => {
        const roomCode = socket.currentRoom;
        if (roomCode && rooms[roomCode]) {
            rooms[roomCode].players = rooms[roomCode].players.filter(id => id !== socket.id);
            if (rooms[roomCode].players.length === 0) {
                delete rooms[roomCode];
            } else {
                io.to(roomCode).emit('playerLeft');
            }
            io.emit('updateRoomList', getPublicRoomList());
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
