
Server · JS
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
const ACCOUNT_API_URL = 'https://script.google.com/macros/s/AKfycby7gNM97v9keZ-Y7MUnrtvtA2SZD7fOeBzH1wsx-dd3F08rPM-_WZm44zt_ayDTUFfAkA/exec';
// 클라이언트가 이 버전이 아니면 "업데이트가 필요합니다" 안내를 보냄
const REQUIRED_VERSION = 'Beta 1.1';
 
// 앱스크립트 계정 API 호출 헬퍼 - payload 객체를 그대로 JSON으로 전달
async function callAccountApi(payload) {
    const res = await fetch(ACCOUNT_API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    return res.json();
}
 
// ===== [추가] 관리자 비밀번호 무차별 대입 방지 =====
// 같은 IP에서 관리자 비밀번호를 5번 틀리면 1분간 시도를 막음 (Render 프록시 뒤라 x-forwarded-for 사용)
const ADMIN_MAX_FAILS = 5;
const ADMIN_LOCK_MS = 60 * 1000;
const MAX_QUIZZES = 200;
const adminFailMap = new Map(); // ip -> { count, until }
 
function getClientIp(socket) {
    const xff = socket.handshake.headers['x-forwarded-for'];
    if (xff) return String(xff).split(',')[0].trim();
    return socket.handshake.address;
}
function isAdminLocked(ip) {
    const e = adminFailMap.get(ip);
    if (!e || !e.until) return false;
    if (Date.now() < e.until) return true;
    adminFailMap.delete(ip);
    return false;
}
function recordAdminFail(ip) {
    const e = adminFailMap.get(ip) || { count: 0, until: 0 };
    e.count += 1;
    if (e.count >= ADMIN_MAX_FAILS) {
        e.until = Date.now() + ADMIN_LOCK_MS;
        e.count = 0;
    }
    adminFailMap.set(ip, e);
}
function clearAdminFail(ip) {
    adminFailMap.delete(ip);
}
const LOCKED_RESULT = { success: false, message: '시도 횟수가 너무 많습니다. 1분 후 다시 시도하세요.' };
 
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
 
    // [추가] 비밀번호 변경 요청 처리 - 로그인한 사용자만 가능
    socket.on('changePassword', async (data) => {
        if (!socket.username) {
            socket.emit('changePasswordResult', { success: false, message: '로그인이 필요합니다.' });
            return;
        }
        try {
            const result = await callAccountApi({
                action: 'changePassword',
                username: socket.username,
                oldPassword: data.oldPassword,
                newPassword: data.newPassword
            });
            socket.emit('changePasswordResult', result);
        } catch (err) {
            socket.emit('changePasswordResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });
 
    // [수정] 퀴즈 문제 조회 - 문제 개수 제한 없음 (실패 시 빈 목록)
    socket.on('getQuizzes', async () => {
        try {
            const result = await callAccountApi({ action: 'getQuizzes' });
            socket.emit('quizzesData', result);
        } catch (err) {
            socket.emit('quizzesData', { success: false, quizzes: [] });
        }
    });
 
    // [추가] 관리자 비밀번호 확인 전용 - 저장 없이 맞는지만 확인 (퀴즈 관리 화면 입장용)
    socket.on('verifyQuizAdmin', async (data) => {
        const ip = getClientIp(socket);
        if (isAdminLocked(ip)) {
            socket.emit('verifyQuizAdminResult', LOCKED_RESULT);
            return;
        }
        try {
            const result = await callAccountApi({
                action: 'verifyAdmin',
                adminPassword: data && data.adminPassword
            });
            if (result && result.success) clearAdminFail(ip);
            else recordAdminFail(ip);
            socket.emit('verifyQuizAdminResult', result);
        } catch (err) {
            socket.emit('verifyQuizAdminResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });
 
    // [수정] 퀴즈 저장 - 관리자 비밀번호는 Code.gs에서 최종 검증
    socket.on('saveQuizzes', async (data) => {
        const ip = getClientIp(socket);
        if (isAdminLocked(ip)) {
            socket.emit('saveQuizzesResult', LOCKED_RESULT);
            return;
        }
        if (!data || !Array.isArray(data.quizzes) || data.quizzes.length > MAX_QUIZZES) {
            socket.emit('saveQuizzesResult', { success: false, message: `문제 목록이 올바르지 않습니다. (최대 ${MAX_QUIZZES}개)` });
            return;
        }
        try {
            const result = await callAccountApi({
                action: 'saveQuizzes',
                adminPassword: data.adminPassword,
                quizzes: data.quizzes
            });
            if (result && result.success) clearAdminFail(ip);
            else recordAdminFail(ip);
            socket.emit('saveQuizzesResult', result);
        } catch (err) {
            socket.emit('saveQuizzesResult', { success: false, message: '계정 서버 연결 실패' });
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
    // [수정] 위치 패킷은 volatile로 전달 - 받는 쪽 연결이 밀려 있으면 오래된 위치는 쌓지 않고 버림
    // (쌓아뒀다가 한꺼번에 도착하면 상대가 순간이동/되감기처럼 보이는 원인이 됨)
    socket.on('move', (data) => {
        if (socket.currentRoom) {
            socket.to(socket.currentRoom).volatile.emit('playerMoved', data);
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
 





Claude가 응답을 완료했습니다
