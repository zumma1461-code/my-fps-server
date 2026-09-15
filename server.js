const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// ===== [추가] 계정 API / 버전 체크 설정 =====
// 구글 앱스크립트(Code.gs)를 웹앱으로 배포한 뒤 나오는 URL로 반드시 교체하세요.
const ACCOUNT_API_URL = 'https://script.google.com/macros/s/여기에_배포된_ID를_넣으세요/exec';
// 클라이언트가 이 버전이 아니면 "업데이트가 필요합니다" 안내를 보냄
const REQUIRED_VERSION = 'Beta 1.0';

// 앱스크립트 계정 API 호출 헬퍼 - payload 객체를 그대로 JSON으로 전달
async function callAccountApi(payload) {
    const res = await fetch(ACCOUNT_API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    return res.json();
}

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

    // [추가] 클라이언트 버전 확인 - 클라이언트가 접속 직후 자신의 버전을 보내면 비교
    socket.on('checkVersion', (clientVersion) => {
        if (clientVersion !== REQUIRED_VERSION) {
            socket.emit('needUpdate', { required: REQUIRED_VERSION, current: clientVersion });
        }
    });

    // [추가] 회원가입 요청 처리 - 앱스크립트 API에 위임, 결과를 그대로 클라이언트에 전달
    socket.on('register', async (data) => {
        try {
            const result = await callAccountApi({ action: 'register', username: data.username, password: data.password });
            socket.emit('registerResult', result);
        } catch (err) {
            socket.emit('registerResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // [추가] 로그인 요청 처리
    socket.on('login', async (data) => {
        try {
            const result = await callAccountApi({ action: 'login', username: data.username, password: data.password });
            if (result.success) {
                socket.username = data.username; // 이후 방/게임 로직 및 전적 기록에 사용
            }
            socket.emit('loginResult', result);
        } catch (err) {
            socket.emit('loginResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // [추가] 매치 종료 시 승/패 기록 - 로그인한 유저만 기록됨
    socket.on('matchResult', async (data) => {
        if (!socket.username) return;
        try {
            const result = await callAccountApi({
                action: 'recordResult',
                username: socket.username,
                result: data.win ? 'win' : 'loss'
            });
            socket.emit('profileUpdate', result);
        } catch (err) {
            // 전적 기록 실패는 게임 진행에 영향 주지 않도록 조용히 무시
        }
    });

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
