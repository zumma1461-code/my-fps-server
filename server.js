const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// [추가] 상태 확인용 주소 - Render 헬스체크나 외부 핑(UptimeRobot 등)으로 서버를 깨워둘 때 사용
app.get('/', (req, res) => res.send('ok'));
app.get('/health', (req, res) => res.json({ ok: true, uptime: Math.round(process.uptime()), online: onlineUsers.size, rooms: Object.keys(rooms).length }));

// [추가] 처리하지 못한 오류 하나 때문에 서버 전체가 꺼지지 않도록 기록만 남김
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));

// ===== 계정 API / 버전 체크 설정 =====
// 구글 앱스크립트(Code.gs)를 웹앱으로 배포한 뒤 나오는 URL로 반드시 교체하세요.
const ACCOUNT_API_URL = 'https://script.google.com/macros/s/AKfycby7gNM97v9keZ-Y7MUnrtvtA2SZD7fOeBzH1wsx-dd3F08rPM-_WZm44zt_ayDTUFfAkA/exec';
// 클라이언트가 이 버전이 아니면 "업데이트가 필요합니다" 안내를 보냄
const REQUIRED_VERSION = 'Beta 1.4';

const API_TIMEOUT_MS = 20 * 1000;                                  // 앱스크립트가 처음 깰 때 느릴 수 있어 넉넉하게
const READ_ACTIONS = new Set(['getFriends', 'getQuizzes', 'getSettings']); // 실패 시 한 번 더 시도해도 안전한 읽기 요청

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// 앱스크립트 계정 API 호출 1회 - payload 객체를 그대로 JSON으로 전달
async function callAccountApiOnce(payload) {
    let res;
    try {
        res = await fetch(ACCOUNT_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: AbortSignal.timeout(API_TIMEOUT_MS)
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

// [수정] 읽기 요청은 실패하면 한 번 더 시도 (쓰기 요청은 중복 실행을 막기 위해 재시도하지 않음)
async function callAccountApi(payload) {
    const retries = READ_ACTIONS.has(payload.action) ? 1 : 0;
    for (let attempt = 0; ; attempt++) {
        try {
            return await callAccountApiOnce(payload);
        } catch (e) {
            if (attempt >= retries) throw e;
            await sleep(500);
        }
    }
}

// ===== 관리자 비밀번호 무차별 대입 방지 =====
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

// ===== [추가] 퀴즈 캐시 =====
// 모든 접속자가 퀴즈를 요청할 때마다 앱스크립트를 부르지 않고, 서버 메모리에 보관해 바로 돌려줌.
// 오래되면(10분) 일단 보관본을 보내면서 뒤에서 새로 받아옴. 저장(saveQuizzes) 성공 시 바로 무효화.
const QUIZ_TTL_MS = 10 * 60 * 1000;
let quizCache = null;      // { data, at }
let quizInflight = null;   // 동시에 여러 명이 요청해도 앱스크립트 호출은 1번만
let quizVersion = 0;       // 저장으로 무효화된 뒤 늦게 도착한 옛 응답이 캐시를 덮어쓰지 못하게 함

function fetchQuizzesFromGas() {
    if (quizInflight) return quizInflight;
    const v = quizVersion;
    quizInflight = callAccountApi({ action: 'getQuizzes' })
        .then((r) => {
            if (r && r.success && Array.isArray(r.quizzes) && v === quizVersion) {
                quizCache = { data: r, at: Date.now() };
            }
            return r;
        })
        .finally(() => { quizInflight = null; });
    return quizInflight;
}

async function getQuizzesCached() {
    if (quizCache) {
        if (Date.now() - quizCache.at > QUIZ_TTL_MS) fetchQuizzesFromGas().catch(() => {});
        return quizCache.data;
    }
    return fetchQuizzesFromGas();
}

function invalidateQuizCache() {
    quizVersion++;
    quizCache = null;
    quizInflight = null;
}

// 생성된 방 목록 관리 객체
let rooms = {};

// ===== 친구 기능: 온라인 유저 / 대결 신청 관리 =====
const onlineUsers = new Map();       // 아이디 -> 소켓 (로그인한 사람만, 같은 아이디가 다시 접속하면 최신 소켓으로 교체)
const pendingChallenges = new Map(); // "신청자>대상" -> { from, to, timer }
const CHALLENGE_TIMEOUT_MS = 30 * 1000;

// ===== [추가] 친구 데이터 캐시 =====
// 친구 목록/받은 요청/보낸 요청을 서버 메모리에 보관. 친구 창이 10초마다 새로고침해도 앱스크립트를 부르지 않음.
// 친구 요청/수락/삭제는 전부 이 서버를 거치므로, 그때마다 관련된 두 사람의 캐시를 무효화해 최신으로 유지함.
// (시트를 직접 고친 경우를 대비해 5분이 지나면 자동으로 다시 읽음)
const FRIEND_TTL_MS = 5 * 60 * 1000;
const friendCache = new Map();    // 아이디 -> { data: { friends, incoming, outgoing }, at }
const friendInflight = new Map(); // 아이디 -> Promise (같은 사람 요청이 겹치면 1번만 호출)
const friendVer = new Map();      // 아이디 -> 무효화 횟수

function invalidateFriends(name) {
    if (!name) return;
    friendCache.delete(name);
    friendInflight.delete(name);
    friendVer.set(name, (friendVer.get(name) || 0) + 1);
}

async function getFriendData(username) {
    const c = friendCache.get(username);
    if (c && Date.now() - c.at < FRIEND_TTL_MS) return c.data;
    if (friendInflight.has(username)) return friendInflight.get(username);

    const ver = friendVer.get(username) || 0;
    const p = (async () => {
        const res = await callAccountApi({ action: 'getFriends', user: username });
        if (!res || !res.success) throw new Error((res && res.message) || 'getFriends failed');
        const data = {
            friends: res.friends || [],
            incoming: res.incoming || [],
            outgoing: res.outgoing || []
        };
        if ((friendVer.get(username) || 0) === ver) friendCache.set(username, { data, at: Date.now() });
        return data;
    })();
    friendInflight.set(username, p);
    try {
        return await p;
    } finally {
        if (friendInflight.get(username) === p) friendInflight.delete(username);
    }
}

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
        friendCache.delete(name); // 접속 중인 사람만 캐시를 유지 (메모리 정리)
        cancelChallengesFor(name, `${name}님이 오프라인이 되었습니다.`);
        cancelInvitesFor(name, `${name}님이 오프라인이 되었습니다.`);
        broadcastPresence(name); // 친구들에게 오프라인 알림 (온라인 목록에서 뺀 뒤에 보내야 offline으로 전달됨)
    }
    socket.username = null;
    socket.friendSet = null;
}

// 각 소켓이 자기 친구 이름 집합(friendSet)을 들고 있어서, 상태가 바뀔 때 계정 서버를 부르지 않고도 즉시 알릴 수 있음
async function loadFriendSet(sock) {
    if (!sock || !sock.username) return;
    try {
        const data = await getFriendData(sock.username);
        sock.friendSet = new Set(data.friends);
    } catch (err) {
        // 실패하면 이전 집합 유지 (다음 친구 변경/로그인 때 다시 시도)
    }
}

// 내 친구들에게 내 상태(온라인/게임 중)가 바뀌었음을 바로 알림
function broadcastPresence(username) {
    if (!username) return;
    const self = onlineUsers.get(username);
    const payload = { name: username, online: !!self, busy: !!(self && self.currentRoom) };
    for (const s of onlineUsers.values()) {
        if (s !== self && s.friendSet && s.friendSet.has(username)) s.emit('presence', payload);
    }
}

// 친구 관계가 바뀐 뒤: 두 사람의 캐시를 비우고 새로 읽은 뒤, 목록 새로고침 + 서로의 현재 상태를 알림
async function afterFriendMutation(a, b) {
    invalidateFriends(a);
    invalidateFriends(b);
    const sa = onlineUsers.get(a);
    const sb = onlineUsers.get(b);
    await Promise.all([loadFriendSet(sa), loadFriendSet(sb)]);
    if (sa) sa.emit('friendsChanged');
    if (sb) sb.emit('friendsChanged');
    broadcastPresence(a);
    broadcastPresence(b);
}

// 친구 목록 + 각 친구의 온라인/게임 중 상태 (친구 관계는 캐시, 온라인 상태는 항상 실시간)
async function buildFriendList(username) {
    const data = await getFriendData(username);
    const friends = data.friends.map(name => {
        const s = onlineUsers.get(name);
        return { name, online: !!s, busy: !!(s && s.currentRoom) };
    });
    return { success: true, friends, incoming: data.incoming, outgoing: data.outgoing };
}

function cleanName(v) {
    return String(v == null ? '' : v).trim().slice(0, 40);
}

// ===== 방 / 팀전 설정 =====
const TEAM_MAX = 5;     // 팀전은 한 팀당 최대 5명
const WIN_SCORE = 5;    // 먼저 5라운드를 이기면 승리

const sockById = (id) => io.sockets.sockets.get(id);
function nameOf(sock) { return (sock && (sock.username || sock.guestName)) || '손님'; }
function nameOfId(id) { return nameOf(sockById(id)); }

// 친구 대결 방(private)도 목록에 포함 - 이미 2명이 있어서 참가는 못 하지만 관전할 수 있음. 비공개(hidden) 방은 목록에서 숨김
function getPublicRoomList() {
    return Object.keys(rooms).filter(code => !rooms[code].hidden).map(code => ({
        code: code,
        name: rooms[code].name,
        count: rooms[code].players.length,
        max: rooms[code].mode === 'team' ? TEAM_MAX * 2 : 2,
        mode: rooms[code].mode,
        started: !!rooms[code].started,
        friend: !!rooms[code].private,
        spectators: rooms[code].spectators.size
    }));
}

// 방 객체 생성 - 관전자/관전용 상태(마지막 위치, 무기, 체력, 점수)를 함께 보관
// opts.mode: '1v1' | 'team',  opts.hidden: 비공개 방(목록에 안 보임)
function makeRoom(name, players, isPrivate, opts) {
    opts = opts || {};
    return {
        name,
        players,
        host: players[0] || null,
        mode: opts.mode === 'team' ? 'team' : '1v1',
        hidden: !!opts.hidden,
        private: !!isPrivate,      // 친구 대결 신청으로 만들어진 방
        started: !!isPrivate,      // 친구 대결 방은 만들어지는 즉시 시작됨
        teamOf: {},                // (팀전) 소켓 id -> 0 | 1
        slotOf: {}, idOfSlot: [], teamOfSlot: [], nameOfSlot: [], idxOfSlot: [], connected: [], alive: [], n: 2,
        roundEnded: false, matchOver: false, roundTimer: null,
        spectators: new Set(),     // 관전 중인 소켓 id
        last: [null, null],        // 플레이어(슬롯)별 마지막 위치 (늦게 들어온 관전자에게 바로 보여주기 위함)
        weapon: [1, 1],
        hp: [100, 100],
        score: [0, 0],
        resetTimer: null
    };
}

// 방에 더 들어올 자리가 있는지 (시작 전이고 정원 미만)
function roomHasSpace(room) {
    if (!room || room.started) return false;
    return room.mode === 'team' ? room.players.length < TEAM_MAX * 2 : room.players.length < 2;
}

// 소켓이 방 안에서 몇 번째 플레이어(슬롯)인지 - 팀전은 시작할 때 정해진 번호, 1대1은 입장 순서
function slotOfSocket(room, socket) {
    const s = room.slotOf[socket.id];
    return s !== undefined ? s : room.players.indexOf(socket.id);
}

// (팀전) 방 대기실 상태를 방 안 모두에게 보냄
function emitRoomState(code) {
    const room = rooms[code];
    if (!room || room.mode !== 'team') return;
    io.to(code).emit('roomState', {
        code,
        name: room.name,
        mode: room.mode,
        hidden: room.hidden,
        started: room.started,
        host: room.host,
        max: TEAM_MAX,
        players: room.players.map(id => ({ id, name: nameOfId(id), team: room.teamOf[id] === 1 ? 1 : 0, host: id === room.host }))
    });
}

function broadcastRoomList() {
    io.emit('updateRoomList', getPublicRoomList());
}

// ===== [추가] 관전 =====
const MAX_SPECTATORS = 10;
const specChannel = (code) => code + '#s';

// 관전자에게만 보내는 이벤트 (관전자가 없으면 아무것도 안 함)
function specEmit(code, room, event, payload, volatile) {
    if (!room || room.spectators.size === 0) return;
    const target = io.to(specChannel(code));
    (volatile ? target.volatile : target).emit(event, payload);
}

function playerNameAt(room, i) {
    if (room.mode === 'team') return room.nameOfSlot[i] || `플레이어 ${i + 1}`;
    const s = io.sockets.sockets.get(room.players[i]);
    return (s && s.username) ? s.username : `플레이어 ${i + 1}`;
}

// 소켓 하나를 관전에서 빼기
function stopSpectating(socket) {
    const code = socket.spectating;
    if (!code) return false;
    socket.spectating = null;
    socket.leave(specChannel(code));
    const room = rooms[code];
    if (room) {
        room.spectators.delete(socket.id);
        broadcastRoomList();
    }
    return true;
}

// 경기가 끝나거나 플레이어가 나가면 관전자를 모두 내보냄
function endRoomSpectators(code, room, reason) {
    if (!room || room.spectators.size === 0) return;
    for (const id of Array.from(room.spectators)) {
        const s = io.sockets.sockets.get(id);
        if (s) {
            s.leave(specChannel(code));
            s.spectating = null;
            s.emit('spectateEnded', { reason });
        }
    }
    room.spectators.clear();
}

// ===== [추가] 친구 초대 (방 만들고 대기 중일 때 친구를 부름) =====
const pendingInvites = new Map(); // "초대한사람>받는사람" -> { from, to, code, timer }
const INVITE_TIMEOUT_MS = 30 * 1000;

// 초대 하나를 취소하고 받는 사람 팝업을 닫음 (inviterMsg가 있으면 초대한 사람에게도 알림)
function dropInvite(key, inviterMsg) {
    const inv = pendingInvites.get(key);
    if (!inv) return;
    clearTimeout(inv.timer);
    pendingInvites.delete(key);
    const target = onlineUsers.get(inv.to);
    if (target) target.emit('inviteCancelled', { from: inv.from });
    if (inviterMsg) {
        const inviter = onlineUsers.get(inv.from);
        if (inviter) inviter.emit('inviteResult', { success: false, message: inviterMsg, to: inv.to });
    }
}

// 접속 종료/로그아웃한 사람이 보냈거나 받은 초대를 모두 정리
function cancelInvitesFor(username, reasonForInviter) {
    for (const [key, inv] of Array.from(pendingInvites.entries())) {
        if (inv.from === username) dropInvite(key);
        else if (inv.to === username) dropInvite(key, reasonForInviter);
    }
}

// 방이 가득 찼거나 사라지면 그 방으로 보낸 초대를 모두 취소
function cancelInvitesForRoom(code) {
    for (const [key, inv] of Array.from(pendingInvites.entries())) {
        if (inv.code === code) dropInvite(key);
    }
}

// ===== (팀전) 라운드 / 경기 진행 - 서버가 판정함 =====
// 한 팀이 전멸하면 상대 팀이 라운드 승리. 먼저 WIN_SCORE 라운드를 이기면 경기 종료
function checkTeamRound(code, room) {
    if (!room.started || room.roundEnded || room.matchOver) return;
    const aliveCnt = [0, 0];
    for (let sl = 0; sl < room.n; sl++) if (room.alive[sl]) aliveCnt[room.teamOfSlot[sl]]++;
    if (aliveCnt[0] > 0 && aliveCnt[1] > 0) return;

    room.roundEnded = true;
    const winner = aliveCnt[0] > 0 ? 0 : (aliveCnt[1] > 0 ? 1 : -1); // 둘 다 0명이면 무승부(점수 없음)
    if (winner >= 0) room.score[winner] += 1;
    io.to(code).emit('teamRoundEnd', { winner, scores: room.score });
    specEmit(code, room, 'specTeamRound', { winner, score: room.score });

    clearTimeout(room.roundTimer);
    if (winner >= 0 && room.score[winner] >= WIN_SCORE) {
        endTeamMatch(code, room, winner, false);
    } else {
        room.roundTimer = setTimeout(() => startTeamRound(code, room), 1600);
    }
}

function startTeamRound(code, room) {
    if (rooms[code] !== room || !room.started || room.matchOver) return;
    room.roundEnded = false;
    for (let sl = 0; sl < room.n; sl++) {
        room.alive[sl] = !!room.connected[sl];
        room.hp[sl] = room.connected[sl] ? 100 : 0;
    }
    io.to(code).emit('teamRoundStart', { scores: room.score });
    specEmit(code, room, 'specRound', { hp: room.hp });
}

function endTeamMatch(code, room, winner, forfeit) {
    if (room.matchOver) return;
    room.matchOver = true;
    room.roundEnded = true;
    io.to(code).emit('teamMatchEnd', { winner, scores: room.score, forfeit: !!forfeit });
    specEmit(code, room, 'specMatchEnd', { winner, score: room.score });
    clearTimeout(room.roundTimer);
    room.roundTimer = setTimeout(() => teamBackToLobby(code, room), 5000);
}

// 경기가 끝나면 같은 방 대기실로 돌아가 팀을 유지한 채 다시 할 수 있음
function teamBackToLobby(code, room) {
    if (rooms[code] !== room) return;
    clearTimeout(room.roundTimer);
    room.started = false;
    room.matchOver = false;
    room.roundEnded = false;
    room.score = [0, 0];
    room.slotOf = {}; room.idOfSlot = []; room.teamOfSlot = []; room.nameOfSlot = []; room.idxOfSlot = [];
    room.connected = []; room.alive = []; room.n = 2;
    room.last = [null, null]; room.weapon = [1, 1]; room.hp = [100, 100];
    endRoomSpectators(code, room, '경기가 끝났습니다.');
    io.to(code).emit('teamBackToLobby');
    emitRoomState(code);
    broadcastRoomList();
}

// 소켓을 현재 방에서 빼는 공통 처리 (방 나가기 / 접속 종료 / 다른 방으로 이동할 때 공용)
// 1대1: 남은 인원이 없으면 방 삭제, 있으면 상대에게 playerLeft 알림. 관전자는 경기가 끝났으므로 내보냄
// 팀전: 방장이 나가면 다음 사람이 방장, 진행 중이면 그 사람은 탈락 처리(한 팀이 전부 나가면 상대 팀 승리)
function removeFromRoom(socket) {
    const code = socket.currentRoom;
    if (!code) return false;
    socket.currentRoom = null;
    const room = rooms[code];
    if (!room) return true;

    socket.leave(code);
    const slot = room.slotOf[socket.id];
    room.players = room.players.filter(id => id !== socket.id);
    delete room.teamOf[socket.id];
    clearTimeout(room.resetTimer);

    if (room.mode === 'team') {
        if (room.players.length === 0) {
            clearTimeout(room.roundTimer);
            cancelInvitesForRoom(code);
            endRoomSpectators(code, room, '방이 사라졌습니다.');
            delete rooms[code];
        } else {
            const hostChanged = room.host === socket.id;
            if (hostChanged) room.host = room.players[0];
            if (room.started && slot !== undefined) {
                room.connected[slot] = false;
                room.alive[slot] = false;
                room.hp[slot] = 0;
                io.to(code).emit('teamPlayerLeft', { slot });
                specEmit(code, room, 'specLeft', { i: slot });
                const conn = [0, 0];
                for (let sl = 0; sl < room.n; sl++) if (room.connected[sl]) conn[room.teamOfSlot[sl]]++;
                if (!room.matchOver) {
                    if (conn[0] === 0 || conn[1] === 0) endTeamMatch(code, room, conn[0] === 0 ? 1 : 0, true);
                    else checkTeamRound(code, room);
                }
            } else if (hostChanged) {
                cancelInvitesForRoom(code); // 대기실에서 방장이 바뀌면 이전 방장이 보낸 초대는 취소
            }
            emitRoomState(code);
        }
        broadcastRoomList();
        return true;
    }

    cancelInvitesForRoom(code);
    endRoomSpectators(code, room, '플레이어가 나가서 경기가 끝났습니다.');
    if (room.players.length === 0) delete rooms[code];
    else io.to(code).emit('playerLeft');

    broadcastRoomList(); // 친구 대결 방도 목록에 보이므로 항상 알림
    return true;
}

io.on('connection', (socket) => {
    socket.guestName = '손님' + Math.floor(100 + Math.random() * 900); // 로그인하지 않은 사람의 방 안 표시 이름
    // 연결 시 현재 방 목록 전송
    socket.emit('updateRoomList', getPublicRoomList());

    // [수정] 버전이 맞지 않는 클라이언트는 로그인/회원가입/비회원 멀티플레이/관전 등 모든 요청을 서버가 거부함
    // (버전 확인 요청, 접속 직후 자동으로 가는 퀴즈 조회/핑 확인만 확인 전에 허용)
    socket.use((packet, next) => {
        const ev = packet[0];
        if (socket.versionOk === true || ev === 'checkVersion') return next();
        if (socket.versionOk === undefined && (ev === 'getQuizzes' || ev === 'pingCheck')) return next();
        socket.emit('needUpdate', { required: REQUIRED_VERSION, current: socket.clientVersion || '알 수 없음' });
        // next()를 부르지 않으면 이 요청은 버려짐
    });

    // 클라이언트 버전 확인 - 클라이언트가 접속 직후 자신의 버전을 보내면 비교
    socket.on('checkVersion', (clientVersion) => {
        socket.clientVersion = String(clientVersion == null ? '' : clientVersion).slice(0, 40);
        socket.versionOk = socket.clientVersion === REQUIRED_VERSION;
        if (!socket.versionOk) {
            socket.emit('needUpdate', { required: REQUIRED_VERSION, current: socket.clientVersion });
        }
    });

    // 회원가입 요청 처리 - 앱스크립트 API에 위임, 결과를 그대로 클라이언트에 전달
    socket.on('register', async (data) => {
        try {
            const result = await callAccountApi({ action: 'register', username: data.username, password: data.password });
            socket.emit('registerResult', result);
        } catch (err) {
            socket.emit('registerResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // 로그인 요청 처리
    socket.on('login', async (data) => {
        try {
            const result = await callAccountApi({ action: 'login', username: data.username, password: data.password });
            if (result.success) {
                clearUserSession(socket); // 같은 소켓으로 다른 계정에 다시 로그인하는 경우 이전 계정 정리
                socket.username = String(data.username).trim(); // 이후 방/게임 로직 및 친구 기능에 사용
                onlineUsers.set(socket.username, socket);
                broadcastPresence(socket.username); // 친구들에게 온라인 알림
                loadFriendSet(socket);              // 내 친구 집합은 비동기로 불러옴 (이어서 오는 getFriends 요청과 호출이 합쳐짐)
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

    // 비밀번호 변경 요청 처리 - 로그인한 사용자만 가능
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

    // 계정 설정 저장/불러오기 - 로그인한 사용자만 가능 (형식 검사는 Code.gs에서도 한 번 더 함)
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

    // [수정] 퀴즈 문제 조회 - 서버 캐시에서 바로 응답 (없을 때만 앱스크립트 호출). 실패 시 빈 목록
    socket.on('getQuizzes', async () => {
        try {
            const result = await getQuizzesCached();
            socket.emit('quizzesData', result);
        } catch (err) {
            socket.emit('quizzesData', { success: false, quizzes: [] });
        }
    });

    // 관리자 비밀번호 확인 전용 - 저장 없이 맞는지만 확인 (퀴즈 관리 화면 입장용)
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

    // 퀴즈 저장 - 관리자 비밀번호는 Code.gs에서 최종 검증
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
            if (result && result.success) {
                clearAdminFail(ip);
                invalidateQuizCache(); // 저장된 새 문제 목록을 다음 요청부터 바로 반영
            } else {
                recordAdminFail(ip);
            }
            socket.emit('saveQuizzesResult', result);
        } catch (err) {
            socket.emit('saveQuizzesResult', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    // ===== 친구 기능 (로그인한 사용자만) =====
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
            if (result && result.success) afterFriendMutation(socket.username, to);
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
            if (result && result.success) afterFriendMutation(socket.username, from);
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
            if (result && result.success) afterFriendMutation(socket.username, other);
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

        // [수정] 매번 앱스크립트를 부르지 않고 서버에 보관된 친구 목록으로 확인 (신청 응답이 훨씬 빨라짐)
        let friendData;
        try {
            friendData = await getFriendData(from);
        } catch (err) {
            return fail('계정 서버 연결 실패');
        }
        if (!friendData.friends.includes(to)) return fail('친구에게만 신청할 수 있습니다.');

        const target = onlineUsers.get(to);
        if (!target) return fail(`${to}님은 오프라인입니다.`);
        if (target.currentRoom || target.spectating) return fail(`${to}님은 게임 중이거나 방에서 대기 중입니다.`);
        if (socket.currentRoom) return fail('이미 방에 있습니다. 방에서 나간 뒤 신청하세요.'); // 응답을 기다리는 사이 방에 들어간 경우
        if (pendingChallenges.has(key)) return fail('이미 신청했습니다. 응답을 기다려주세요.'); // 대기 중에 같은 신청이 겹친 경우

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
        rooms[code] = makeRoom(`${from} vs ${me}`, [challenger.id, socket.id], true, { mode: '1v1' });
        challenger.join(code);
        socket.join(code);
        challenger.currentRoom = code;
        socket.currentRoom = code;

        challenger.emit('challengeMatchStart', { code, isHost: true });
        socket.emit('challengeMatchStart', { code, isHost: false });
        io.to(code).emit('gameStart');

        broadcastRoomList(); // 친구 대결 방도 멀티플레이 목록에 표시
        broadcastPresence(from);
        broadcastPresence(me);

        // 두 사람이 받았거나 보낸 다른 신청은 모두 취소
        cancelChallengesFor(from, `${from}님이 다른 대결을 시작했습니다.`);
        cancelChallengesFor(me, `${me}님이 다른 대결을 시작했습니다.`);
    });

    // ===== [추가] 핑 측정 - 클라이언트가 응답 콜백을 보내면 바로 호출해 왕복 시간을 재게 함 =====
    socket.on('pingCheck', (cb) => {
        if (typeof cb === 'function') cb();
    });

    // ===== [추가] 친구 초대 =====
    socket.on('inviteFriend', async (data) => {
        const to = cleanName(data && data.to);
        const fail = (message) => socket.emit('inviteResult', { success: false, message, to });
        if (!socket.username) return fail('로그인이 필요합니다.');

        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.private) return fail('방을 만든 뒤에만 초대할 수 있습니다.');
        if (room.host !== socket.id) return fail('방장만 초대할 수 있습니다.');
        if (!roomHasSpace(room)) return fail(room.started ? '이미 게임이 시작되었습니다.' : '방이 가득 찼습니다.');

        const from = socket.username;
        if (!to || to === from) return fail('초대 대상이 올바르지 않습니다.');
        const key = `${from}>${to}`;
        if (pendingInvites.has(key)) return fail('이미 초대했습니다. 응답을 기다려주세요.');

        let friendData;
        try {
            friendData = await getFriendData(from);
        } catch (err) {
            return fail('계정 서버 연결 실패');
        }
        if (!friendData.friends.includes(to)) return fail('친구에게만 초대할 수 있습니다.');

        const target = onlineUsers.get(to);
        if (!target) return fail(`${to}님은 오프라인입니다.`);
        if (target.currentRoom || target.spectating) return fail(`${to}님은 게임 중이거나 방에서 대기 중입니다.`);

        // 서버 응답을 기다리는 사이 방이 바뀌었는지 다시 확인
        if (socket.currentRoom !== code || !rooms[code] || !roomHasSpace(rooms[code])) return fail('방 상태가 바뀌어 초대를 보내지 못했습니다.');
        if (pendingInvites.has(key)) return fail('이미 초대했습니다. 응답을 기다려주세요.');

        const timer = setTimeout(() => {
            pendingInvites.delete(key);
            socket.emit('inviteResult', { success: false, message: `${to}님이 응답하지 않았습니다.`, to });
            target.emit('inviteCancelled', { from });
        }, INVITE_TIMEOUT_MS);
        pendingInvites.set(key, { from, to, code, timer });

        target.emit('inviteReceived', { from, code, roomName: room.name, timeoutMs: INVITE_TIMEOUT_MS });
        socket.emit('inviteSent', { to });
    });

    socket.on('inviteRespond', (data) => {
        if (!socket.username) return;
        const me = socket.username;
        const from = cleanName(data && data.from);
        const key = `${from}>${me}`;
        const inv = pendingInvites.get(key);
        if (!inv) {
            socket.emit('inviteResult', { success: false, message: '만료되었거나 취소된 초대입니다.' });
            return;
        }
        clearTimeout(inv.timer);
        pendingInvites.delete(key);

        const inviter = onlineUsers.get(from);
        if (!data || !data.accept) {
            if (inviter) inviter.emit('inviteResult', { success: false, message: `${me}님이 초대를 거절했습니다.`, to: me });
            return;
        }
        const room = rooms[inv.code];
        if (!inviter || inviter.currentRoom !== inv.code || !room || !roomHasSpace(room) || room.host !== inviter.id) {
            socket.emit('inviteResult', { success: false, message: '초대받은 방이 더 이상 없습니다.' });
            return;
        }
        if (socket.currentRoom || socket.spectating) {
            socket.emit('inviteResult', { success: false, message: '이미 방에 있습니다.' });
            if (inviter) inviter.emit('inviteResult', { success: false, message: `${me}님이 이미 다른 방에 있습니다.`, to: me });
            return;
        }
        // 수락 - 받는 쪽 클라이언트가 이어서 joinRoom을 보내 입장함
        socket.emit('inviteAccepted', { code: inv.code });
        inviter.emit('inviteResult', { success: true, message: `${me}님이 초대를 수락했습니다.`, to: me });
    });

    // ===== [추가] 관전 =====
    socket.on('spectateRoom', (data) => {
        const code = cleanName(data && data.code);
        const room = rooms[code];
        if (!room) { socket.emit('errorMsg', '존재하지 않는 방입니다.'); return; }
        if (!room.started || (room.mode !== 'team' && room.players.length < 2)) { socket.emit('errorMsg', '아직 경기가 시작되지 않은 방입니다.'); return; }
        if (socket.currentRoom) { socket.emit('errorMsg', '방에 있는 동안에는 관전할 수 없습니다.'); return; }
        if (socket.spectating === code) return;
        if (room.spectators.size >= MAX_SPECTATORS) { socket.emit('errorMsg', '관전 인원이 가득 찼습니다.'); return; }

        stopSpectating(socket); // 다른 방을 보고 있었다면 먼저 나감
        room.spectators.add(socket.id);
        socket.join(specChannel(code));
        socket.spectating = code;

        socket.emit('spectateStart', {
            code,
            name: room.name,
            mode: room.mode,
            players: room.mode === 'team' ? room.nameOfSlot.slice() : [playerNameAt(room, 0), playerNameAt(room, 1)],
            teams: room.mode === 'team' ? room.teamOfSlot.slice() : [0, 1],
            positions: room.last,
            weapons: room.weapon,
            hp: room.hp,
            score: room.score
        });
        broadcastRoomList();
    });

    socket.on('leaveSpectate', () => {
        stopSpectating(socket);
    });

    // 1. 방 만들기 (mode: '1v1' | 'team', hidden: 비공개 방)
    socket.on('createRoom', (data) => {
        const roomCode = cleanName(data && data.code);
        if (!roomCode) {
            socket.emit('errorMsg', '방 코드가 올바르지 않습니다.');
            return;
        }
        // 같은 코드의 방이 이미 있으면 덮어쓰지 않음 (같은 요청이 두 번 들어온 경우는 무시)
        if (rooms[roomCode]) {
            if (rooms[roomCode].players.includes(socket.id)) return;
            socket.emit('errorMsg', '이미 존재하는 방 코드입니다. 다시 시도해주세요.');
            return;
        }

        stopSpectating(socket);
        removeFromRoom(socket); // 이전 방에 남아 있었다면 정리

        // 방 이름이 비어있으면 대체명 생성
        const rawName = data && typeof data.name === 'string' ? data.name.trim().slice(0, 40) : '';
        const roomName = rawName !== '' ? rawName : `매치 - ${roomCode}`;
        const mode = data && data.mode === 'team' ? 'team' : '1v1';

        rooms[roomCode] = makeRoom(roomName, [socket.id], false, { mode, hidden: !!(data && data.hidden) });
        if (mode === 'team') rooms[roomCode].teamOf[socket.id] = 0; // 방장은 A팀에서 시작

        socket.join(roomCode);
        socket.currentRoom = roomCode;
        emitRoomState(roomCode);
        broadcastRoomList();
        broadcastPresence(socket.username);
    });

    // 2. 방 참가하기
    socket.on('joinRoom', (data) => {
        const roomCode = cleanName(data && data.code);
        const room = rooms[roomCode];

        if (!room) {
            socket.emit('errorMsg', '존재하지 않는 방입니다.');
            return;
        }

        // 자기 방에 다시 들어가 혼자 게임이 시작되는 문제 방지
        if (room.players.includes(socket.id)) return;

        if (room.started) {
            socket.emit('errorMsg', '이미 게임이 시작된 방입니다. 목록에서 관전할 수 있습니다.');
            return;
        }
        if (!roomHasSpace(room)) {
            socket.emit('errorMsg', '방이 이미 가득 찼습니다.');
            return;
        }

        stopSpectating(socket);
        removeFromRoom(socket); // 다른 방에 있었다면 먼저 나감

        room.players.push(socket.id);
        socket.join(roomCode);
        socket.currentRoom = roomCode;

        if (room.mode === 'team') {
            // 사람이 적은 팀으로 자동 배정 (방장이 나중에 옮길 수 있음)
            const cnt = [0, 0];
            room.players.forEach(id => { if (id !== socket.id) cnt[room.teamOf[id] === 1 ? 1 : 0]++; });
            room.teamOf[socket.id] = cnt[0] <= cnt[1] ? 0 : 1;
            if (!roomHasSpace(room)) cancelInvitesForRoom(roomCode);
            emitRoomState(roomCode);
            broadcastRoomList();
            broadcastPresence(socket.username);
            return;
        }

        room.started = true;
        cancelInvitesForRoom(roomCode); // 방이 찼으니 남은 초대는 취소
        broadcastRoomList();
        io.to(roomCode).emit('gameStart');
        broadcastPresence(socket.username);
    });

    // ===== [추가] 방 옵션 (방장): 공개 / 비공개 =====
    socket.on('roomOption', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.private || room.started || room.host !== socket.id) return;
        room.hidden = !!(data && data.hidden);
        emitRoomState(code);
        broadcastRoomList();
    });

    // ===== [추가] 팀전 대기실: 방장이 플레이어를 다른 팀으로 옮김 =====
    socket.on('teamMove', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.mode !== 'team' || room.started) return;
        if (room.host !== socket.id) { socket.emit('errorMsg', '방장만 팀을 옮길 수 있습니다.'); return; }
        const id = String(data && data.id);
        if (!room.players.includes(id)) return;
        const team = data && data.team === 1 ? 1 : 0;
        const inTeam = room.players.filter(p => p !== id && room.teamOf[p] === team).length;
        if (inTeam >= TEAM_MAX) { socket.emit('errorMsg', `한 팀은 최대 ${TEAM_MAX}명입니다.`); return; }
        room.teamOf[id] = team;
        emitRoomState(code);
    });

    // ===== [추가] 팀전 시작 (방장만) =====
    socket.on('startTeamGame', () => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.mode !== 'team' || room.started) return;
        if (room.host !== socket.id) { socket.emit('errorMsg', '방장만 게임을 시작할 수 있습니다.'); return; }

        const t0 = room.players.filter(id => room.teamOf[id] !== 1);
        const t1 = room.players.filter(id => room.teamOf[id] === 1);
        if (t0.length === 0 || t1.length === 0) { socket.emit('errorMsg', '양 팀에 최소 1명씩 있어야 시작할 수 있습니다.'); return; }

        // 시작할 때 모든 플레이어에게 고정 번호(슬롯)를 줌 - 이후 위치/발사/피격 이벤트가 이 번호로 오감
        room.slotOf = {}; room.idOfSlot = []; room.teamOfSlot = []; room.nameOfSlot = []; room.idxOfSlot = [];
        [[t0, 0], [t1, 1]].forEach(([list, team]) => list.forEach((id, idx) => {
            const slot = room.idOfSlot.length;
            room.slotOf[id] = slot;
            room.idOfSlot.push(id);
            room.teamOfSlot.push(team);
            room.nameOfSlot.push(nameOfId(id));
            room.idxOfSlot.push(idx);
        }));
        room.n = room.idOfSlot.length;
        room.alive = Array(room.n).fill(true);
        room.connected = Array(room.n).fill(true);
        room.hp = Array(room.n).fill(100);
        room.weapon = Array(room.n).fill(1);
        room.last = Array(room.n).fill(null);
        room.score = [0, 0];
        room.started = true;
        room.matchOver = false;
        room.roundEnded = false;
        clearTimeout(room.roundTimer);
        cancelInvitesForRoom(code);

        const roster = room.idOfSlot.map((id, slot) => ({ slot, name: room.nameOfSlot[slot], team: room.teamOfSlot[slot], idx: room.idxOfSlot[slot] }));
        room.idOfSlot.forEach((id, slot) => {
            io.to(id).emit('teamGameStart', { players: roster, you: { slot, team: room.teamOfSlot[slot] } });
        });
        emitRoomState(code);
        broadcastRoomList();
    });

    // 3. 방 나가기 및 취소 (유령 방 방지)
    socket.on('leaveRoom', () => {
        if (removeFromRoom(socket)) {
            broadcastPresence(socket.username);
        }
    });

    // 동기화 이벤트
    // 위치 패킷은 volatile로 전달 - 받는 쪽 연결이 밀려 있으면 오래된 위치는 쌓지 않고 버림
    // (쌓아뒀다가 한꺼번에 도착하면 상대가 순간이동/되감기처럼 보이는 원인이 됨)
    socket.on('move', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        if (room.mode === 'team' && (!room.started || !room.alive[slot])) return; // 대기실이거나 이미 죽은 사람의 위치는 무시
        socket.to(code).volatile.emit('playerMoved', Object.assign({}, data, { pid: slot }));

        if (slot >= 0 && data) {
            // 관전자가 1인칭으로 볼 수 있도록 시선 위아래(pitch)도 함께 전달 (숫자로 보정)
            const num = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);
            const pos = { x: num(data.x), y: num(data.y), z: num(data.z), yaw: num(data.yaw), pitch: Math.max(-1.6, Math.min(1.6, num(data.pitch))) };
            room.last[slot] = pos;
            specEmit(code, room, 'specMove', { i: slot, x: pos.x, y: pos.y, z: pos.z, yaw: pos.yaw, pitch: pos.pitch }, true);
        }
    });

    socket.on('weaponChange', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        if (room.mode === 'team' && !room.started) return;
        socket.to(code).emit('enemyWeaponChanged', Object.assign({}, data, { pid: slot }));
        if (slot >= 0 && data) {
            room.weapon[slot] = data.weapon;
            specEmit(code, room, 'specWeapon', { i: slot, weapon: data.weapon });
        }
    });

    socket.on('enemyShoot', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        if (room.mode === 'team' && (!room.started || !room.alive[slot])) return;
        socket.to(code).emit('enemyFired', Object.assign({}, data, { pid: slot }));
        if (slot >= 0) specEmit(code, room, 'specFire', { i: slot, weapon: data && data.weapon, mode: data && data.mode });
    });

    socket.on('hit', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        const dmg = Math.max(0, Math.min(100, Number(data && data.damage) || 0));

        if (room.mode === 'team') {
            // 팀전: 맞은 사람에게만 데미지를 전달. 같은 팀/이미 죽은 사람은 무시
            if (!room.started || room.matchOver || !room.alive[slot]) return;
            const tSlot = Number(data && data.target);
            if (!Number.isInteger(tSlot) || tSlot < 0 || tSlot >= room.n || !room.alive[tSlot]) return;
            if (room.teamOfSlot[tSlot] === room.teamOfSlot[slot]) return;
            io.to(room.idOfSlot[tSlot]).emit('takeDamage', { damage: dmg, from: slot });
            room.hp[tSlot] = Math.max(0, room.hp[tSlot] - dmg);
            specEmit(code, room, 'specHp', { i: tSlot, hp: room.hp[tSlot] });
            return;
        }

        socket.to(code).emit('takeDamage', data);
        // 관전자용 체력 추적 (실제 판정은 기존처럼 각 클라이언트가 함)
        if (slot >= 0) {
            const victim = 1 - slot;
            room.hp[victim] = Math.max(0, room.hp[victim] - dmg);
            specEmit(code, room, 'specHp', { i: victim, hp: room.hp[victim] });
        }
    });

    socket.on('iDied', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);

        if (room.mode === 'team') {
            if (!room.started || room.matchOver || !room.alive[slot]) return;
            room.alive[slot] = false;
            room.hp[slot] = 0;
            let killer = Number(data && data.killer);
            if (!Number.isInteger(killer) || killer < 0 || killer >= room.n || room.teamOfSlot[killer] === room.teamOfSlot[slot]) killer = -1;
            io.to(code).emit('playerDied', { slot, killer });
            specEmit(code, room, 'specDeath', { i: slot, killer, score: room.score });
            checkTeamRound(code, room);
            return;
        }

        socket.to(code).emit('enemyKilled');
        if (slot >= 0) {
            const winner = 1 - slot;
            room.hp[slot] = 0;
            room.score[winner] += 1;
            specEmit(code, room, 'specDeath', { i: slot, score: room.score });
            clearTimeout(room.resetTimer);
            if (room.score[winner] >= WIN_SCORE) {
                specEmit(code, room, 'specMatchEnd', { winner, score: room.score });
            } else {
                // 라운드가 다시 시작되는 시점(클라이언트가 약 1.5초 뒤 시작)에 체력을 되돌림
                room.resetTimer = setTimeout(() => {
                    if (rooms[code] !== room) return;
                    room.hp = [100, 100];
                    specEmit(code, room, 'specRound', { hp: room.hp });
                }, 1600);
            }
        }
    });

    // 접속 해제 처리
    socket.on('disconnect', () => {
        stopSpectating(socket);
        clearUserSession(socket);
        removeFromRoom(socket);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    // [추가] 서버가 켜질 때 퀴즈를 미리 받아 두고 앱스크립트도 깨워둠 (첫 접속자가 느리게 기다리지 않도록)
    fetchQuizzesFromGas().catch(() => {});
});
