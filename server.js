const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// 방 정보 관리 { roomCode: { name, count, players: [] } }
const rooms = {};

function getPublicRoomList() {
    return Object.keys(rooms).map(code => ({
        code: code,
        name: rooms[code].name,
        count: rooms[code].players.length
    }));
}

io.on('connection', (socket) => {
    // 1. 방 생성 (방 이름 중복 검사 포함)
    socket.on('createRoom', (data) => {
        const { code, name } = data;

        // 같은 이름의 방이 존재하는지 확인
        const nameExists = Object.values(rooms).some(r => r.name === name);
        if (nameExists) {
            socket.emit('errorMsg', '이미 존재하는 방 이름입니다. 다른 이름을 사용해주세요.');
            return;
        }

        rooms[code] = {
            name: name,
            players: [socket.id]
        };
        socket.roomCode = code;
        socket.join(code);

        io.emit('updateRoomList', getPublicRoomList());
    });

    // 2. 방 참가
    socket.on('joinRoom', (data) => {
        const { code } = data;
        const room = rooms[code];

        if (!room) {
            socket.emit('errorMsg', '존재하지 않는 방 코드입니다.');
            return;
        }

        if (room.players.length >= 2) {
            socket.emit('errorMsg', '방이 이미 가득 찼증니다.');
            return;
        }

        room.players.push(socket.id);
        socket.roomCode = code;
        socket.join(code);

        io.emit('updateRoomList', getPublicRoomList());
        io.to(code).emit('gameStart');
    });

    // 3. 호스트 참가 취소
    socket.on('cancelHost', () => {
        if (socket.roomCode && rooms[socket.roomCode]) {
            delete rooms[socket.roomCode];
            socket.leave(socket.roomCode);
            socket.roomCode = null;
            io.emit('updateRoomList', getPublicRoomList());
        }
    });

    // 4. 플레이어 위치 및 시선 정보 전달
    socket.on('move', (data) => {
        if (socket.roomCode) {
            socket.to(socket.roomCode).emit('playerMoved', data);
        }
    });

    // 5. 피격 판정
    socket.on('hit', (data) => {
        if (socket.roomCode) {
            socket.to(socket.roomCode).emit('takeDamage', data);
        }
    });

    // 6. 사망 알림
    socket.on('iDied', () => {
        if (socket.roomCode) {
            socket.to(socket.roomCode).emit('enemyKilled');
        }
    });

    // 7. 연결 해제
    socket.disconnecting = () => {
        if (socket.roomCode && rooms[socket.roomCode]) {
            const room = rooms[socket.roomCode];
            room.players = room.players.filter(id => id !== socket.id);

            if (room.players.length === 0) {
                delete rooms[socket.roomCode];
            } else {
                io.to(socket.roomCode).emit('errorMsg', '상대방이 게임을 나갔습니다.');
            }
            io.emit('updateRoomList', getPublicRoomList());
        }
    };
});

// 초기 방 목록 전송
io.on('connection', (socket) => {
    socket.emit('updateRoomList', getPublicRoomList());
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
