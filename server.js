const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const rooms = {};

io.on('connection', (socket) => {
    console.log('유저 접속:', socket.id);

    // 방 만들기 (호스트)
    socket.on('createRoom', ({ code }) => {
        if (rooms[code]) {
            socket.emit('errorMsg', '이미 존재하는 방 코드입니다.');
            return;
        }

        rooms[code] = {
            host: socket.id,
            guest: null,
            code: code,
            scores: { host: 0, guest: 0 }
        };

        socket.join(code);
        socket.roomId = code;
        socket.isHost = true;
        console.log(`방 생성 완료 [코드: ${code}]`);
    });

    // 방 참가하기 (게스트)
    socket.on('joinRoom', ({ code }) => {
        const room = rooms[code];
        if (!room) {
            socket.emit('errorMsg', '존재하지 않는 방 코드입니다.');
            return;
        }
        if (room.guest) {
            socket.emit('errorMsg', '이미 가득 찬 방입니다.');
            return;
        }

        room.guest = socket.id;
        socket.join(code);
        socket.roomId = code;
        socket.isHost = false;

        console.log(`방 참가 완료 [코드: ${code}]`);

        // 게임 시작 알림 (두 플레이어 모두에게)
        io.to(room.host).emit('gameStart', { role: 'host' });
        io.to(room.guest).emit('gameStart', { role: 'guest' });
    });

    // 호스트의 방 삭제/취소
    socket.on('cancelHost', ({ code }) => {
        if (rooms[code] && rooms[code].host === socket.id) {
            delete rooms[code];
            socket.leave(code);
            console.log(`방 취소됨 [코드: ${code}]`);
        }
    });

    // 위치 동기화
    socket.on('move', (data) => {
        if (!socket.roomId) return;
        socket.to(socket.roomId).emit('playerMoved', {
            id: socket.id,
            x: data.x,
            y: data.y,
            z: data.z,
            yaw: data.yaw
        });
    });

    // 사격 히트 데미지 처리
    socket.on('hit', (data) => {
        if (!socket.roomId) return;
        socket.to(socket.roomId).emit('takeDamage', {
            damage: data.damage,
            part: data.part
        });
    });

    // 연결 종료 처리
    socket.on('disconnect', () => {
        const code = socket.roomId;
        if (code && rooms[code]) {
            io.to(code).emit('errorMsg', '상대방과의 연결이 끊어졌습니다.');
            delete rooms[code];
        }
        console.log('유저 접속 해제:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`서버가 포트 ${PORT}에서 실행 중입니다.`);
});
