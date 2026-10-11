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
const REQUIRED_VERSION = 'Beta 1.2';

// 앱스크립트 계정 API 호출 헬퍼 - payload 객체를 그대로 JSON으로 전달
async function callAccountApi(payload) {
    let res;
    try {
        res = await fetch(ACCOUNT_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (e) {
        console.error('[계정 API] 요청 실패 (action: ' + payload.action + '):', e.message);
        throw e;
    }
    const text = await res.text();
    try {
        return JSON.parse(text);
    } catch (e) {
        // 앱스크립트가 JSON이 아닌 HTML(권한 필요/오류 페이지 등)을 돌려준 경우 - 원인 파악용 로그
        console.error('[계정 API] JSON이 아닌 응답 (action: ' + payload.action + ', status: ' + res.status + '):', text.slice(0, 300));
        throw new Error('invalid response');
    }
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

// ===== [추가] 친구 기능: 온라인 유저 / 대결 신청 관리 =====
const onlineUsers = new Map();       // 아이디 -> 소켓 (로그인한 사람만, 같은 아이디가 다시 접속하면 최신 소켓으로 교체)
const pendingChallenges = new Map(); // "신청자>대상" -> { from, to, timer }
const CHALLENGE_TIMEOUT_MS = 30 * 1000;

// 특정 유저와 관련된 대기 중인 대결 신청을 모두 취소하고 상대에게 알림
function cancelChallengesFor(username, reasonForOther) {
    for (const [key, ch] of Array.from(pendingChallenges.entries())) {
        if (ch.from !== username && ch.to !== username) continue;
        clearTimeout(ch.timer);
        pendingChallenges.delete(key);
        const otherName = ch.from === username ? ch.to : ch.from;
        const otherSocket = onlineUsers.get(otherName);
        if (!otherSocket) continue;
        if (ch.from === username) otherSocket.emit('challengeCancelled', { from: username }); // 받은 쪽 팝업 닫기
        else otherSocket.emit('challengeResult', { success: false, message: reasonForOther });  // 보낸 쪽에 안내
    }
}

// 로그아웃/접속 종료 시 온라인 목록과 대기 중인 신청 정리
function clearUserSession(socket) {
    const name = socket.username;
    if (!name) return;
    if (onlineUsers.get(name) === socket) {
        onlineUsers.delete(name);
        cancelChallengesFor(name, `${name}님이 오프라인이 되었습니다.`);
    }
    socket.username = null;
}

// 친구 목록 + 각 친구의 온라인/게임 중 상태
async function buildFriendList(username) {
    const res = await callAccountApi({ action: 'getFriends', user: username });
    if (!res || !res.success) return res || { success: false, message: '친구 목록을 불러오지 못했습니다.' };
    const friends = (res.friends || []).map(name => {
        const s = onlineUsers.get(name);
        return { name, online: !!s, busy: !!(s && s.currentRoom) };
    });
    return { success: true, friends, incoming: res.incoming || [], outgoing: res.outgoing || [] };
}

function notifyFriendsChanged(...names) {
    names.forEach(n => {
        const s = onlineUsers.get(n);
        if (s) s.emit('friendsChanged');
    });
}

function cleanName(v) {
    return String(v == null ? '' : v).trim().slice(0, 40);
}

function getPublicRoomList() {
    return Object.keys(rooms).filter(code => !rooms[code].private).map(code => ({
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
                clearUserSession(socket); // 같은 소켓으로 다른 계정에 다시 로그인하는 경우 이전 계정 정리
                socket.username = String(data.username).trim(); // 이후 방/게임 로직 및 친구 기능에 사용
                onlineUsers.set(socket.username, socket);
            }
            socket.emit('loginResult', result);
        } catch (err) {
            socket.emit('loginResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // 로그아웃 - 온라인 목록에서 빼고 대기 중인 대결 신청을 정리
    socket.on('logout', () => {
        clearUserSession(socket);
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

    // [추가] 계정 설정 저장/불러오기 - 로그인한 사용자만 가능 (형식 검사는 Code.gs에서도 한 번 더 함)
    socket.on('saveSettings', async (data) => {
        if (!socket.username) {
            socket.emit('saveSettingsResult', { success: false, message: '로그인이 필요합니다.' });
            return;
        }
        const settings = data && typeof data.settings === 'string' ? data.settings.trim() : '';
        if (!/^[0-9a-zA-Z.\/]{1,100}$/.test(settings)) {
            socket.emit('saveSettingsResult', { success: false, message: '설정 형식이 올바르지 않습니다.' });
            return;
        }
        try {
            const result = await callAccountApi({ action: 'saveSettings', username: socket.username, settings });
            socket.emit('saveSettingsResult', result);
        } catch (err) {
            socket.emit('saveSettingsResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    socket.on('getSettings', async () => {
        if (!socket.username) {
            socket.emit('settingsData', { success: false, message: '로그인이 필요합니다.' });
            return;
        }
        try {
            const result = await callAccountApi({ action: 'getSettings', username: socket.username });
            socket.emit('settingsData', result);
        } catch (err) {
            socket.emit('settingsData', { success: false, message: '계정 서버 연결 실패' });
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

    // ===== [추가] 친구 기능 (로그인한 사용자만) =====
    socket.on('getFriends', async () => {
        if (!socket.username) { socket.emit('friendsData', { success: false, message: '로그인이 필요합니다.' }); return; }
        try {
            socket.emit('friendsData', await buildFriendList(socket.username));
        } catch (err) {
            socket.emit('friendsData', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    socket.on('friendRequest', async (data) => {
        if (!socket.username) { socket.emit('friendActionResult', { success: false, message: '로그인이 필요합니다.' }); return; }
        const to = cleanName(data && data.to);
        try {
            const result = await callAccountApi({ action: 'friendRequest', from: socket.username, to });
            socket.emit('friendActionResult', result);
            if (result && result.success) notifyFriendsChanged(socket.username, to);
        } catch (err) {
            socket.emit('friendActionResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    socket.on('friendRespond', async (data) => {
        if (!socket.username) { socket.emit('friendActionResult', { success: false, message: '로그인이 필요합니다.' }); return; }
        const from = cleanName(data && data.from);
        try {
            const result = await callAccountApi({ action: 'friendRespond', user: socket.username, from, accept: !!(data && data.accept) });
            socket.emit('friendActionResult', result);
            if (result && result.success) notifyFriendsChanged(socket.username, from);
        } catch (err) {
            socket.emit('friendActionResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    socket.on('friendRemove', async (data) => {
        if (!socket.username) { socket.emit('friendActionResult', { success: false, message: '로그인이 필요합니다.' }); return; }
        const other = cleanName(data && data.name);
        try {
            const result = await callAccountApi({ action: 'friendRemove', user: socket.username, other });
            socket.emit('friendActionResult', result);
            if (result && result.success) notifyFriendsChanged(socket.username, other);
        } catch (err) {
            socket.emit('friendActionResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // 친구에게 1대1 대결 신청
    socket.on('challengeFriend', async (data) => {
        const fail = (message) => socket.emit('challengeResult', { success: false, message });
        if (!socket.username) return fail('로그인이 필요합니다.');
        const from = socket.username;
        const to = cleanName(data && data.to);
        if (!to || to === from) return fail('대결 상대가 올바르지 않습니다.');
        if (socket.currentRoom) return fail('이미 방에 있습니다. 방에서 나간 뒤 신청하세요.');

        const key = `${from}>${to}`;
        if (pendingChallenges.has(key)) return fail('이미 신청했습니다. 응답을 기다려주세요.');
        if (pendingChallenges.has(`${to}>${from}`)) return fail('상대가 먼저 신청했습니다. 받은 신청을 수락해주세요.');

        let list;
        try {
            list = await callAccountApi({ action: 'getFriends', user: from });
        } catch (err) {
            return fail('계정 서버 연결 실패');
        }
        if (!list || !list.success || !(list.friends || []).includes(to)) return fail('친구에게만 신청할 수 있습니다.');

        const target = onlineUsers.get(to);
        if (!target) return fail(`${to}님은 오프라인입니다.`);
        if (target.currentRoom) return fail(`${to}님은 게임 중이거나 방에서 대기 중입니다.`);
        if (socket.currentRoom) return fail('이미 방에 있습니다. 방에서 나간 뒤 신청하세요.'); // 서버 응답을 기다리는 사이 방에 들어간 경우

        const timer = setTimeout(() => {
            pendingChallenges.delete(key);
            socket.emit('challengeResult', { success: false, message: `${to}님이 응답하지 않았습니다.` });
            target.emit('challengeCancelled', { from });
        }, CHALLENGE_TIMEOUT_MS);
        pendingChallenges.set(key, { from, to, timer });

        target.emit('challengeReceived', { from, timeoutMs: CHALLENGE_TIMEOUT_MS });
        socket.emit('challengeSent', { to });
    });

    // 받은 대결 신청에 수락/거절
    socket.on('challengeRespond', (data) => {
        if (!socket.username) return;
        const me = socket.username;
        const from = cleanName(data && data.from);
        const key = `${from}>${me}`;
        const ch = pendingChallenges.get(key);
        if (!ch) {
            socket.emit('challengeResult', { success: false, message: '만료되었거나 취소된 신청입니다.' });
            return;
        }
        clearTimeout(ch.timer);
        pendingChallenges.delete(key);

        const challenger = onlineUsers.get(from);
        if (!data || !data.accept) {
            if (challenger) challenger.emit('challengeResult', { success: false, message: `${me}님이 거절했습니다.` });
            return;
        }
        if (!challenger) {
            socket.emit('challengeResult', { success: false, message: `${from}님이 오프라인입니다.` });
            return;
        }
        if (challenger.currentRoom || socket.currentRoom) {
            const msg = '방에 있는 중이라 대결을 시작할 수 없습니다.';
            socket.emit('challengeResult', { success: false, message: msg });
            challenger.emit('challengeResult', { success: false, message: msg });
            return;
        }

        // 친구 전용 비공개 방을 만들어 두 사람을 바로 입장시킴 (방 목록에는 보이지 않음)
        let code;
        do { code = 'f' + Math.random().toString(36).slice(2, 8); } while (rooms[code]);
        rooms[code] = { name: `${from} vs ${me}`, players: [challenger.id, socket.id], private: true };
        challenger.join(code);
        socket.join(code);
        challenger.currentRoom = code;
        socket.currentRoom = code;

        challenger.emit('challengeMatchStart', { code, isHost: true });
        socket.emit('challengeMatchStart', { code, isHost: false });
        io.to(code).emit('gameStart');

        // 두 사람이 받았거나 보낸 다른 신청은 모두 취소
        cancelChallengesFor(from, `${from}님이 다른 대결을 시작했습니다.`);
        cancelChallengesFor(me, `${me}님이 다른 대결을 시작했습니다.`);
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
        clearUserSession(socket);
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
