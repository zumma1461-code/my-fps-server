const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// 생성된 방 목록 관리
let rooms = {}; 

function getRoomList() {
    return Object.keys(rooms).map(code => ({
        code: code,
        name: rooms[code].name,
        count: rooms[code].players.length
    }));
}

io.on('connection', (socket) => {
    // 로비에 접속 시 현재 방 목록 전송
    socket.emit('updateRoomList', getRoomList());

    // 방 만들기
    socket.on('createRoom', ({ code, name }) => {
        rooms[code] = {
            name: name || `방-${code}`,
            players: [socket.id]
        };
        socket.join(code);
        socket.roomCode = code;

        // 전체 플레이어에게 방 목록 갱신 전송
        io.emit('updateRoomList', getRoomList());
    });

    // 방 참가하기
    socket.on('joinRoom', ({ code }) => {
        const room = rooms[code];
        if (!room) {
            return socket.emit('errorMsg', '존재하지 않는 방입니다.');
        }
        if (room.players.length >= 2) {
            return socket.emit('errorMsg', '방이 가득 찼습니다.');
        }

        room.players.push(socket.id);
        socket.join(code);
        socket.roomCode = code;

        io.emit('updateRoomList', getRoomList());

        // 2명이 모이면 게임 시작
        if (room.players.length === 2) {
            io.to(code).emit('gameStart');
        }
    });

    // 이동 동기화
    socket.on('move', (data) => {
        if (socket.roomCode) {
            socket.to(socket.roomCode).emit('playerMoved', data);
        }
    });

    // 피격 처리
    socket.on('hit', (data) => {
        if (socket.roomCode) {
            socket.to(socket.roomCode).emit('takeDamage', data);
        }
    });

    // 방장 대기 취소 또는 연결 종료
    const leaveRoom = () => {
        const code = socket.roomCode;
        if (code && rooms[code]) {
            rooms[code].players = rooms[code].players.filter(id => id !== socket.id);
            if (rooms[code].players.length === 0) {
                delete rooms[code];
            }
            io.emit('updateRoomList', getRoomList());
            socket.leave(code);
            socket.roomCode = null;
        }
    };

    socket.on('cancelHost', leaveRoom);
    socket.on('disconnect', leaveRoom);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
