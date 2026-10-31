const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// 상태 확인용 주소 - Render 헬스체크나 외부 핑(UptimeRobot 등)으로 서버를 깨워둘 때 사용
app.get('/', (req, res) => res.send('ok'));
app.get('/health', (req, res) => res.json({ ok: true, uptime: Math.round(process.uptime()), online: onlineUsers.size, rooms: Object.keys(rooms).length }));

// 처리하지 못한 오류 하나 때문에 서버 전체가 꺼지지 않도록 기록만 남김
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e));
process.on('uncaughtException', (e) => console.error('[uncaughtException]', e));

// ===== 계정 API / 버전 체크 설정 =====
// 구글 앱스크립트(Code.gs)를 웹앱으로 배포한 뒤 나오는 URL로 반드시 교체하세요.
const ACCOUNT_API_URL = 'https://script.google.com/macros/s/AKfycby7gNM97v9keZ-Y7MUnrtvtA2SZD7fOeBzH1wsx-dd3F08rPM-_WZm44zt_ayDTUFfAkA/exec';
// 클라이언트가 이 버전이 아니면 "업데이트가 필요합니다" 안내를 보냄
const REQUIRED_VERSION = 'Beta 1.6';

const API_TIMEOUT_MS = 20 * 1000;                                              // 앱스크립트가 처음 깰 때 느릴 수 있어 넉넉하게
const READ_ACTIONS = new Set(['getFriends', 'getQuizzes', 'getSettings', 'getRanked']); // 실패 시 한 번 더 시도해도 안전한 읽기 요청

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

// 읽기 요청은 실패하면 한 번 더 시도 (쓰기 요청은 중복 실행을 막기 위해 재시도하지 않음)
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

// ===== 퀴즈 캐시 =====
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

// ===== 경쟁전 대기열 =====
const rankedQueue = []; // 매칭을 기다리는 소켓들 (로그인한 사람만)
function leaveRankedQueue(s) {
    const i = rankedQueue.indexOf(s);
    if (i >= 0) { rankedQueue.splice(i, 1); return true; }
    return false;
}

// ===== 친구 데이터 캐시 =====
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
    leaveRankedQueue(socket);
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

const nf = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);

// ===== 방 / 팀전 설정 =====
const TEAM_MAX = 5;     // 팀전은 한 팀당 최대 5명
const WIN_SCORE = 5;    // 먼저 5라운드를 이기면 승리

// ===== 데스매치 (서버가 켜져 있는 동안 항상 열려 있는 방) =====
const DM_CODE = 'DM1';
const DM_MAX = 10;                 // 동시에 들어올 수 있는 최대 인원
const DM_MIN_RESPAWN_MS = 2000;    // 죽은 뒤 이 시간이 지나야 부활 요청이 받아들여짐
const DM_PROTECT_MS = 2500;        // 부활 직후 무적 시간
const isSlotMode = (room) => !!room && (room.mode === 'team' || room.mode === 'dm'); // 플레이어마다 고정 번호(슬롯)를 쓰는 방

const sockById = (id) => io.sockets.sockets.get(id);
function nameOf(sock) { return (sock && (sock.username || sock.guestName)) || '손님'; }
function nameOfId(id) { return nameOf(sockById(id)); }

// 친구 대결 방(private)도 목록에 포함 - 이미 2명이 있어서 참가는 못 하지만 관전할 수 있음. 비공개(hidden) 방은 목록에서 숨김
function getPublicRoomList() {
    return Object.keys(rooms).filter(code => !rooms[code].hidden).map(code => ({
        code: code,
        name: rooms[code].name,
        count: rooms[code].players.length,
        max: rooms[code].mode === 'team' ? TEAM_MAX * 2 : (rooms[code].mode === 'dm' ? DM_MAX : 2),
        mode: rooms[code].mode,
        started: rooms[code].mode === 'dm' ? false : !!rooms[code].started, // 데스매치는 언제든 들어갈 수 있으므로 "시작 전" 방처럼 보여줌
        friend: !!rooms[code].private && !rooms[code].ranked,
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
        mode: opts.mode === 'team' ? 'team' : (opts.mode === 'dm' ? 'dm' : '1v1'),
        hidden: !!opts.hidden,
        private: !!isPrivate,      // 친구 대결/경쟁전으로 만들어진 방
        started: !!isPrivate,      // 친구 대결/경쟁전 방은 만들어지는 즉시 시작됨
        ranked: false,             // 경쟁전 방 여부
        rankedNames: {},           // (경쟁전) 소켓 id -> 계정 아이디 (나간 뒤에도 결과를 기록하기 위해 보관)
        rankedDone: false,         // (경쟁전) 결과가 이미 기록됐는지
        skillPhase: false,         // 첫 라운드 전 스킬(캐릭터) 선택 단계
        skillOf: {},               // 소켓 id -> 고른 스킬
        skillTimer: null,
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
    if (room && room.mode === 'dm') return room.players.length < DM_MAX;
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

// ===== 관전 =====
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

// ===== 친구 초대 (방 만들고 대기 중일 때 친구를 부름) =====
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

// ===== 스킬(캐릭터) 선택 단계 =====
// 경기가 시작되면 모든 플레이어가 스킬을 하나씩 골라야 첫 라운드가 시작됨. 30초 안에 못 고르면 대시로 자동 선택.
const SKILL_KEYS = new Set(['dash', 'para', 'grenade']);
const SKILL_PICK_MS = 30 * 1000;

function beginSkillPhase(code, room) {
    clearTimeout(room.skillTimer);
    room.skillPhase = true;
    room.skillOf = {};
    room.skillTimer = setTimeout(() => {
        if (rooms[code] !== room || !room.skillPhase) return;
        room.players.forEach(id => {
            if (!room.skillOf[id]) {
                room.skillOf[id] = 'dash';
                const s = sockById(id);
                if (s) s.emit('skillAutoPick', { skill: 'dash' });
            }
        });
        finishSkillPhase(code, room);
    }, SKILL_PICK_MS);
}

function finishSkillPhase(code, room) {
    clearTimeout(room.skillTimer);
    if (!room.skillPhase) return;
    room.skillPhase = false;
    io.to(code).emit('skillsReady');
}

function checkSkillsReady(code, room) {
    if (!room || !room.skillPhase) return;
    const total = room.players.length;
    const count = room.players.filter(id => room.skillOf[id]).length;
    io.to(code).emit('skillProgress', { count, total });
    if (total > 0 && count >= total) finishSkillPhase(code, room);
}

// ===== 킬로그 =====
// 죽은 사람이 보고한 무기/부위와 함께 방 안 모두에게 보냄. 팀전에서는 상대 팀 이름이 "적 N"으로 가려짐
const KILL_WEAPONS = new Set([1, 2, 3, 5]);            // 1 권총, 2 칼, 3 돌격소총, 5 수류탄
const KILL_PARTS = new Set(['head', 'body', 'legs', 'blast']);

function emitKillFeed(code, room, killerSlot, victimSlot, weapon, part) {
    weapon = KILL_WEAPONS.has(Number(weapon)) ? Number(weapon) : 0;
    part = KILL_PARTS.has(part) ? part : '';
    const slotMode = isSlotMode(room);
    const nameAt = (slot) => slotMode ? (room.nameOfSlot[slot] || '플레이어') : playerNameAt(room, slot);
    const recipients = slotMode
        ? room.idOfSlot.map((id, sl) => ({ id, sl })).filter(r => r.id && room.connected[r.sl])
        : room.players.map((id, sl) => ({ id, sl }));
    recipients.forEach(({ id, sl }) => {
        const view = (slot) => {
            if (slot < 0) return { name: '', me: false, ally: false };
            let shown = nameAt(slot);
            const sameTeam = room.mode === 'team' ? room.teamOfSlot[slot] === room.teamOfSlot[sl] : slot === sl;
            if (room.mode === 'team' && !sameTeam) shown = `적 ${room.idxOfSlot[slot] + 1}`;
            return { name: shown, me: slot === sl, ally: sameTeam };
        };
        io.to(id).emit('killFeed', { killer: view(killerSlot), victim: view(victimSlot), weapon, part });
    });
    specEmit(code, room, 'specKill', {
        killer: killerSlot >= 0 ? nameAt(killerSlot) : '', victim: nameAt(victimSlot), weapon, part
    });
}

// 데스매치 점수판 (킬/데스)을 방 안 모두에게 보냄
function emitDmScores(code, room) {
    io.to(code).emit('dmScores', { kills: room.kills, deaths: room.deaths });
}

// 슬롯 번호로 소켓 id 찾기 (1대1은 입장 순서)
function idOfSlotAny(room, slot) {
    return isSlotMode(room) ? room.idOfSlot[slot] : room.players[slot];
}

// ===== 경쟁전 결과 기록 =====
// 이긴 사람/진 사람의 소켓 id를 받아 계정 서버(Code.gs)에 점수를 반영하고, 두 사람에게 결과를 보냄.
// 경기 도중에 나간 사람은 패배와 똑같이 처리됨 (leftId로 표시)
async function settleRanked(room, winnerId, loserId, leftId) {
    if (!room || !room.ranked || room.rankedDone) return;
    room.rankedDone = true;
    const wName = room.rankedNames[winnerId];
    const lName = room.rankedNames[loserId];
    if (!wName || !lName) return;

    let res = null;
    try {
        res = await callAccountApi({ action: 'rankedResult', winner: wName, loser: lName });
    } catch (e) {
        res = null;
    }
    const wSock = sockById(winnerId);
    const lSock = sockById(loserId);
    if (!res || !res.success) {
        const fail = { success: false, message: '경쟁전 기록을 저장하지 못했습니다.' };
        if (wSock) wSock.emit('rankedResult', fail);
        if (lSock) lSock.emit('rankedResult', fail);
        return;
    }
    const left = !!leftId;
    if (wSock) wSock.emit('rankedResult', { success: true, win: true, left, delta: res.winner.delta, points: res.winner.points, wins: res.winner.wins, losses: res.winner.losses });
    if (lSock) lSock.emit('rankedResult', { success: true, win: false, left, delta: res.loser.delta, points: res.loser.points, wins: res.loser.wins, losses: res.loser.losses });
}

// ===== 1대1 대결 방 만들기 (친구 대결 / 경쟁전 공용) =====
function startDuel(a, b, opts) {
    opts = opts || {};
    leaveRankedQueue(a);
    leaveRankedQueue(b);
    if (opts.ranked && Math.random() < 0.5) { const t = a; a = b; b = t; } // 경쟁전은 시작 위치를 랜덤으로

    let code;
    do { code = (opts.ranked ? 'r' : 'f') + Math.random().toString(36).slice(2, 8); } while (rooms[code]);
    const name = opts.ranked ? '경쟁전 매치' : `${a.username} vs ${b.username}`;
    const room = makeRoom(name, [a.id, b.id], true, { mode: '1v1' });
    if (opts.ranked) {
        room.ranked = true;
        room.rankedNames = { [a.id]: a.username, [b.id]: b.username };
    }
    rooms[code] = room;
    a.join(code);
    b.join(code);
    a.currentRoom = code;
    b.currentRoom = code;

    console.log(`[대결 시작] ${opts.ranked ? '경쟁전' : '친구 대결'} ${a.username} vs ${b.username} (방 ${code})`);
    a.emit('challengeMatchStart', { code, isHost: true, ranked: !!opts.ranked });
    b.emit('challengeMatchStart', { code, isHost: false, ranked: !!opts.ranked });
    a.emit('opponentName', { name: nameOf(b) });
    b.emit('opponentName', { name: nameOf(a) });
    beginSkillPhase(code, room);
    io.to(code).emit('gameStart');

    broadcastRoomList(); // 대결 방도 멀티플레이 목록에 표시
    broadcastPresence(a.username);
    broadcastPresence(b.username);

    // 두 사람이 받았거나 보낸 다른 신청은 모두 취소
    cancelChallengesFor(a.username, `${a.username}님이 다른 대결을 시작했습니다.`);
    cancelChallengesFor(b.username, `${b.username}님이 다른 대결을 시작했습니다.`);
}

// 대기열에서 두 명씩 짝지어 경기 시작 (티어 차이는 제한 없음)
function tryRankedMatch() {
    // 접속이 끊겼거나 방에 들어간 사람, 같은 아이디로 중복 접속한 사람은 걸러냄
    const seen = new Map();
    rankedQueue.forEach(s => {
        if (!(s.connected && s.username && !s.currentRoom && !s.spectating)) return;
        const prev = seen.get(s.username);
        if (prev && prev !== s) prev.emit('rankedQueueState', { searching: false, message: '같은 계정으로 다른 곳에서 대기해서 이쪽 대기는 취소되었습니다.' });
        seen.set(s.username, s);
    });
    rankedQueue.length = 0;
    seen.forEach(s => rankedQueue.push(s));

    while (rankedQueue.length >= 2) {
        const a = rankedQueue.shift();
        const b = rankedQueue.shift();
        startDuel(a, b, { ranked: true });
    }
    rankedQueue.forEach(s => s.emit('rankedQueueState', { searching: true, waiting: rankedQueue.length }));
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
    clearTimeout(room.skillTimer);
    room.skillPhase = false;
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
//       경쟁전 도중에 나가면 그 사람은 패배 처리됨
// 팀전: 방장이 나가면 다음 사람이 방장, 진행 중이면 그 사람은 탈락 처리(한 팀이 전부 나가면 상대 팀 승리)
function removeFromRoom(socket) {
    const code = socket.currentRoom;
    if (!code) return false;
    socket.currentRoom = null;
    const room = rooms[code];
    if (!room) return true;

    socket.leave(code);
    const slot = room.slotOf[socket.id];

    // 경쟁전 경기 도중 이탈 = 패배 (상대는 승리)
    if (room.ranked && room.started && !room.rankedDone) {
        const other = room.players.find(id => id !== socket.id);
        if (other) settleRanked(room, other, socket.id, socket.id);
    }

    room.players = room.players.filter(id => id !== socket.id);
    delete room.teamOf[socket.id];
    clearTimeout(room.resetTimer);

    if (room.mode === 'dm') {
        // 데스매치는 사람이 모두 나가도 방이 사라지지 않음
        if (slot !== undefined) {
            room.connected[slot] = false;
            room.alive[slot] = false;
            room.idOfSlot[slot] = null;
            room.nameOfSlot[slot] = '';
            room.last[slot] = null;
            delete room.slotOf[socket.id];
            io.to(code).emit('teamPlayerLeft', { slot });
            emitDmScores(code, room);
        }
        broadcastRoomList();
        return true;
    }

    if (room.mode === 'team') {
        if (room.players.length === 0) {
            clearTimeout(room.roundTimer);
            clearTimeout(room.skillTimer);
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
                if (room.skillPhase) checkSkillsReady(code, room); // 스킬을 고르던 중 나갔다면 남은 사람 기준으로 다시 확인
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
    if (room.players.length === 0) {
        clearTimeout(room.skillTimer);
        delete rooms[code];
    } else {
        io.to(code).emit('playerLeft');
    }

    broadcastRoomList(); // 친구 대결 방도 목록에 보이므로 항상 알림
    return true;
}

function createDeathmatchRoom() {
    const r = makeRoom('[데스매치] 24시간 상시', [], false, { mode: 'dm' });
    r.started = true;
    r.permanent = true;
    r.n = DM_MAX;
    r.slotOf = {};
    r.idOfSlot = Array(DM_MAX).fill(null);
    r.teamOfSlot = Array.from({ length: DM_MAX }, (_, i) => i); // 모두가 서로 적 (팀이 따로 없음)
    r.nameOfSlot = Array(DM_MAX).fill('');
    r.idxOfSlot = Array.from({ length: DM_MAX }, (_, i) => i);
    r.connected = Array(DM_MAX).fill(false);
    r.alive = Array(DM_MAX).fill(false);
    r.hp = Array(DM_MAX).fill(100);
    r.weapon = Array(DM_MAX).fill(3);
    r.last = Array(DM_MAX).fill(null);
    r.kills = Array(DM_MAX).fill(0);
    r.deaths = Array(DM_MAX).fill(0);
    r.inv = Array(DM_MAX).fill(0);       // 이 시각까지 무적
    r.diedAt = Array(DM_MAX).fill(0);
    rooms[DM_CODE] = r;
}
createDeathmatchRoom();

// 데스매치에 들어가기 - 빈 슬롯 하나를 받고, 첫 스폰(dmRespawn)을 하기 전까지는 보이지도 맞지도 않음
function joinDeathmatch(socket, room, code) {
    if (room.players.length >= DM_MAX) { socket.emit('errorMsg', '방이 이미 가득 찼습니다.'); return; }
    leaveRankedQueue(socket);
    stopSpectating(socket);
    removeFromRoom(socket); // 다른 방에 있었다면 먼저 나감
    const slot = room.connected.findIndex(c => !c);
    if (slot < 0) { socket.emit('errorMsg', '방이 이미 가득 찼습니다.'); return; }

    room.players.push(socket.id);
    socket.join(code);
    socket.currentRoom = code;
    room.slotOf[socket.id] = slot;
    room.idOfSlot[slot] = socket.id;
    room.nameOfSlot[slot] = nameOf(socket);
    room.connected[slot] = true;
    room.alive[slot] = false;
    room.hp[slot] = 100;
    room.weapon[slot] = 3;
    room.kills[slot] = 0;
    room.deaths[slot] = 0;
    room.inv[slot] = 0;
    room.last[slot] = null;

    const players = [];
    for (let sl = 0; sl < DM_MAX; sl++) {
        if (room.connected[sl]) players.push({ slot: sl, name: room.nameOfSlot[sl], k: room.kills[sl], d: room.deaths[sl] });
    }
    socket.emit('dmStart', { code, name: room.name, you: { slot }, players });
    socket.to(code).emit('dmPlayerJoined', { slot, name: room.nameOfSlot[slot] });
    emitDmScores(code, room);
    broadcastRoomList();
    broadcastPresence(socket.username);
}

io.on('connection', (socket) => {
    socket.guestName = '손님' + Math.floor(100 + Math.random() * 900); // 로그인하지 않은 사람의 방 안 표시 이름
    // 연결 시 현재 방 목록 전송
    socket.emit('updateRoomList', getPublicRoomList());

    // [1.6] 버전은 접속(handshake) 때 함께 받음 - 접속 직후 다른 요청이 먼저 도착해도 "업데이트 필요"가 잘못 뜨지 않음
    const authVer = socket.handshake && socket.handshake.auth && socket.handshake.auth.version;
    if (authVer !== undefined && authVer !== null) {
        socket.clientVersion = String(authVer).slice(0, 40);
        socket.versionOk = socket.clientVersion === REQUIRED_VERSION;
        if (!socket.versionOk) socket.emit('needUpdate', { required: REQUIRED_VERSION, current: socket.clientVersion });
    }

    // 버전이 맞지 않는 클라이언트는 로그인/회원가입/비회원 멀티플레이/관전 등 모든 요청을 서버가 거부함
    // 버전 확인 전(옛 클라이언트)의 요청은 안내 없이 조용히 버림 (checkVersion, 퀴즈 조회, 핑 확인만 허용)
    socket.use((packet, next) => {
        const ev = packet[0];
        if (socket.versionOk === true || ev === 'checkVersion') return next();
        if (socket.versionOk === undefined) {
            if (ev === 'getQuizzes' || ev === 'pingCheck') return next();
            return; // 버림
        }
        socket.emit('needUpdate', { required: REQUIRED_VERSION, current: socket.clientVersion || '알 수 없음' });
        // next()를 부르지 않으면 이 요청은 버려짐
    });

    // 옛 방식(접속 후 버전 전송)도 그대로 지원
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

    // 퀴즈 문제 조회 - 서버 캐시에서 바로 응답 (없을 때만 앱스크립트 호출). 실패 시 빈 목록
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
    // [1.6] 상대가 이미 나에게 신청해 둔 상태라면, 내가 신청하는 순간 수락한 것으로 보고 바로 대결을 시작함
    socket.on('challengeFriend', async (data) => {
        const fail = (message) => socket.emit('challengeResult', { success: false, message });
        if (!socket.username) return fail('로그인이 필요합니다.');
        const from = socket.username;
        const to = cleanName(data && data.to);
        if (!to || to === from) return fail('대결 상대가 올바르지 않습니다.');
        if (socket.currentRoom) return fail('이미 방에 있습니다. 방에서 나간 뒤 신청하세요.');

        const key = `${from}>${to}`;
        const reverseKey = `${to}>${from}`;
        if (pendingChallenges.has(key)) return fail('이미 신청했습니다. 응답을 기다려주세요.');

        // 매번 앱스크립트를 부르지 않고 서버에 보관된 친구 목록으로 확인 (신청 응답이 훨씬 빨라짐)
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

        // 상대가 먼저 보낸 신청이 있으면 바로 대결 시작 (먼저 신청한 사람이 방장)
        const rev = pendingChallenges.get(reverseKey);
        if (rev) {
            clearTimeout(rev.timer);
            pendingChallenges.delete(reverseKey);
            startDuel(target, socket);
            return;
        }

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

        // 친구 전용 방을 만들어 두 사람을 바로 입장시킴
        startDuel(challenger, socket);
    });

    // ===== 핑 측정 - 클라이언트가 응답 콜백을 보내면 바로 호출해 왕복 시간을 재게 함 =====
    socket.on('pingCheck', (cb) => {
        if (typeof cb === 'function') cb();
    });

    // ===== [1.6] 경쟁전 (랜덤 매칭, 로그인한 사람만, 1대1) =====
    socket.on('getRanked', async () => {
        if (!socket.username) { socket.emit('rankedData', { success: false, message: '로그인이 필요합니다.' }); return; }
        try {
            const r = await callAccountApi({ action: 'getRanked', username: socket.username });
            socket.emit('rankedData', r);
        } catch (err) {
            socket.emit('rankedData', { success: false, message: '계정 서버 연결 실패' });
        }
    });

    socket.on('rankedQueue', () => {
        const stop = (message) => socket.emit('rankedQueueState', { searching: false, message });
        if (!socket.username) return stop('로그인한 계정만 경쟁전을 할 수 있습니다.');
        if (socket.currentRoom || socket.spectating) return stop('방에서 나간 뒤 이용하세요.');
        // 같은 계정이 이미 다른 곳에서 대기 중이면 자기 자신과는 매칭될 수 없으므로 안내
        if (rankedQueue.some(s => s !== socket && s.username === socket.username)) {
            return stop('같은 계정이 이미 대기 중입니다. 서로 다른 계정 두 개로 접속해야 매칭됩니다.');
        }
        if (!rankedQueue.includes(socket)) rankedQueue.push(socket);
        console.log(`[경쟁전] ${socket.username} 대기열 입장 (대기 ${rankedQueue.length}명)`);
        socket.emit('rankedQueueState', { searching: true, waiting: rankedQueue.length });
        tryRankedMatch();
    });

    socket.on('rankedCancel', () => {
        if (leaveRankedQueue(socket)) socket.emit('rankedQueueState', { searching: false });
    });

    // ===== 친구 초대 =====
    socket.on('inviteFriend', async (data) => {
        const to = cleanName(data && data.to);
        const fail = (message) => socket.emit('inviteResult', { success: false, message, to });
        if (!socket.username) return fail('로그인이 필요합니다.');

        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.private || room.permanent) return fail('방을 만든 뒤에만 초대할 수 있습니다.');
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

    // ===== 관전 =====
    socket.on('spectateRoom', (data) => {
        const code = cleanName(data && data.code);
        const room = rooms[code];
        if (!room) { socket.emit('errorMsg', '존재하지 않는 방입니다.'); return; }
        if (room.mode === 'dm') { socket.emit('errorMsg', '데스매치는 관전할 수 없습니다. 직접 들어와서 참여하세요.'); return; }
        if (!room.started || (room.mode !== 'team' && room.players.length < 2)) { socket.emit('errorMsg', '아직 경기가 시작되지 않은 방입니다.'); return; }
        if (socket.currentRoom) { socket.emit('errorMsg', '방에 있는 동안에는 관전할 수 없습니다.'); return; }
        if (socket.spectating === code) return;
        if (room.spectators.size >= MAX_SPECTATORS) { socket.emit('errorMsg', '관전 인원이 가득 찼습니다.'); return; }

        leaveRankedQueue(socket);
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

        leaveRankedQueue(socket);
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

        if (room.mode === 'dm') { joinDeathmatch(socket, room, roomCode); return; }

        if (room.started) {
            socket.emit('errorMsg', '이미 게임이 시작된 방입니다. 목록에서 관전할 수 있습니다.');
            return;
        }
        if (!roomHasSpace(room)) {
            socket.emit('errorMsg', '방이 이미 가득 찼습니다.');
            return;
        }

        leaveRankedQueue(socket);
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
        beginSkillPhase(roomCode, room);
        if (room.players.length === 2) {
            const s0 = sockById(room.players[0]);
            if (s0) s0.emit('opponentName', { name: nameOf(socket) });
            socket.emit('opponentName', { name: nameOf(s0) });
        }
        io.to(roomCode).emit('gameStart');
        broadcastPresence(socket.username);
    });

    // ===== 방 옵션 (방장): 공개 / 비공개 =====
    socket.on('roomOption', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.private || room.started || room.host !== socket.id) return;
        room.hidden = !!(data && data.hidden);
        emitRoomState(code);
        broadcastRoomList();
    });

    // ===== 팀전 대기실: 방장이 플레이어를 다른 팀으로 옮김 =====
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

    // ===== 팀전 시작 (방장만) =====
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
        beginSkillPhase(code, room); // 모두 스킬을 골라야 첫 라운드가 시작됨

        // 게임 중에는 상대 팀 닉네임이 보이지 않도록, 받는 사람마다 상대 팀 이름을 "적 N"으로 바꿔서 보냄
        room.idOfSlot.forEach((id, slot) => {
            const myTeam = room.teamOfSlot[slot];
            const roster = room.idOfSlot.map((_, sl) => ({
                slot: sl,
                name: room.teamOfSlot[sl] === myTeam ? room.nameOfSlot[sl] : `적 ${room.idxOfSlot[sl] + 1}`,
                team: room.teamOfSlot[sl],
                idx: room.idxOfSlot[sl]
            }));
            io.to(id).emit('teamGameStart', { players: roster, you: { slot, team: myTeam } });
        });
        emitRoomState(code);
        broadcastRoomList();
    });

    // ===== [1.6] 스킬(캐릭터) 선택 =====
    socket.on('pickSkill', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || !room.skillPhase || !room.players.includes(socket.id)) return;
        const skill = data && data.skill;
        if (!SKILL_KEYS.has(skill) || room.skillOf[socket.id]) return;
        room.skillOf[socket.id] = skill;
        checkSkillsReady(code, room);
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
        if (isSlotMode(room) && (!room.started || !room.alive[slot])) return; // 대기실이거나 이미 죽은 사람의 위치는 무시
        socket.to(code).volatile.emit('playerMoved', Object.assign({}, data, { pid: slot }));

        if (slot >= 0 && data) {
            // 관전자가 1인칭으로 볼 수 있도록 시선 위아래(pitch)도 함께 전달 (숫자로 보정)
            const pos = { x: nf(data.x), y: nf(data.y), z: nf(data.z), yaw: nf(data.yaw), pitch: Math.max(-1.6, Math.min(1.6, nf(data.pitch))) };
            room.last[slot] = pos;
            // [1.6] 들고 있는 무기도 위치 패킷에 실려 오므로, 무기 교체 패킷을 놓쳐도 곧바로 바로잡힘
            // (0 = 스킬/장비를 쓰는 동안의 맨손 상태)
            const wv = Number(data.weapon);
            const w = (wv >= 0 && wv <= 4 && Number.isInteger(wv)) ? wv : -1;
            if (w > 0) room.weapon[slot] = w;
            specEmit(code, room, 'specMove', { i: slot, x: pos.x, y: pos.y, z: pos.z, yaw: pos.yaw, pitch: pos.pitch, w: w >= 0 ? w : undefined }, true);
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
            const wv = Number(data.weapon);
            if (wv >= 1 && wv <= 4) room.weapon[slot] = wv;
            if (wv >= 0 && wv <= 4) specEmit(code, room, 'specWeapon', { i: slot, weapon: wv });
        }
    });

    socket.on('enemyShoot', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        if (isSlotMode(room) && (!room.started || !room.alive[slot])) return;
        socket.to(code).emit('enemyFired', Object.assign({}, data, { pid: slot }));
        if (slot >= 0) specEmit(code, room, 'specFire', { i: slot, weapon: data && data.weapon, mode: data && data.mode });
    });

    // ===== [1.6] 섬광탄 / 스킬 투사체 (던짐 → 터짐) =====
    // 서버는 그대로 전달만 함. 섬광/어둠 효과는 맞는 사람의 화면에서 계산하고, 수류탄 피해는 던진 사람이 계산해 hit로 보냄
    const PROJ_KINDS = new Set(['flash', 'para', 'grenade']);
    socket.on('skillThrow', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || !d || !PROJ_KINDS.has(d.kind) || room.skillPhase) return;
        const slot = slotOfSocket(room, socket);
        if (isSlotMode(room) && (!room.started || !room.alive[slot])) return;
        const out = {
            pid: slot, id: nf(d.id), kind: d.kind,
            x: nf(d.x), y: nf(d.y), z: nf(d.z), vx: nf(d.vx), vy: nf(d.vy), vz: nf(d.vz),
            fuse: Math.max(0, Math.min(5000, nf(d.fuse)))
        };
        socket.to(code).emit('skillThrow', out);
        if (slot >= 0) specEmit(code, room, 'specSkillThrow', Object.assign({ i: slot }, out));
    });

    socket.on('skillPop', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || !d || !PROJ_KINDS.has(d.kind)) return;
        if (isSlotMode(room) && !room.started) return;
        const slot = slotOfSocket(room, socket);
        const out = { pid: slot, id: nf(d.id), kind: d.kind, x: nf(d.x), y: nf(d.y), z: nf(d.z) };
        socket.to(code).emit('skillPop', out);
        if (slot >= 0) specEmit(code, room, 'specSkillPop', Object.assign({ i: slot }, out));
    });

    // ===== [1.6] 내 체력 보고 - 관전자 화면의 체력 표시는 맞은 본인이 알려주는 값을 그대로 사용 =====
    socket.on('myHp', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        if (slot < 0 || slot >= room.hp.length) return;
        if (isSlotMode(room) && !room.started) return;
        const hp = Math.max(0, Math.min(100, Math.round(nf(d && d.hp))));
        room.hp[slot] = hp;
        specEmit(code, room, 'specHp', { i: slot, hp });
    });

    // ===== 데스매치 부활 (죽은 뒤 일정 시간이 지나야 받아들여짐. 처음 스폰할 때도 이 이벤트를 보냄) =====
    socket.on('dmRespawn', () => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.mode !== 'dm') return;
        const slot = slotOfSocket(room, socket);
        if (slot < 0 || !room.connected[slot]) return;
        if (!room.alive[slot] && room.diedAt[slot] && Date.now() - room.diedAt[slot] < DM_MIN_RESPAWN_MS) return;
        room.alive[slot] = true;
        room.hp[slot] = 100;
        room.inv[slot] = Date.now() + DM_PROTECT_MS;
        socket.to(code).emit('dmRespawned', { slot });
    });

    // ===== 팀 핑 (팀전에서 같은 팀 사람에게만 전달) =====
    const PING_TYPES = new Set(['danger', 'assist', 'watch']);
    socket.on('ping', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || room.mode !== 'team' || !room.started || !d || !PING_TYPES.has(d.type)) return;
        const slot = slotOfSocket(room, socket);
        if (slot < 0 || !room.alive[slot]) return;
        const now = Date.now();
        if (socket.lastPingAt && now - socket.lastPingAt < 800) return; // 도배 방지
        socket.lastPingAt = now;
        const lim = (v) => Math.max(-300, Math.min(300, nf(v)));
        const out = { pid: slot, type: d.type, x: lim(d.x), y: lim(d.y), z: lim(d.z) };
        room.idOfSlot.forEach((id, sl) => {
            if (id && sl !== slot && room.connected[sl] && room.teamOfSlot[sl] === room.teamOfSlot[slot]) io.to(id).emit('ping', out);
        });
    });

    // ===== 섬광/어둠에 맞은 사람이 "내가 맞았다"고 알림 → 다른 사람 화면에서 그 사람 머리 위에 표시 =====
    socket.on('skillStatus', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || !d || (d.kind !== 'flash' && d.kind !== 'para')) return;
        const slot = slotOfSocket(room, socket);
        if (slot < 0) return;
        const ms = Math.max(200, Math.min(6000, nf(d.ms)));
        const by = Number(d.by);
        const byId = Number.isInteger(by) && by >= 0 ? idOfSlotAny(room, by) : null;
        // 던진 사람에게는 "적중" 표시용(mine: true), 나머지 사람에게는 머리 위 표시용(mine: false). 맞은 본인에게는 보내지 않음
        const memberIds = isSlotMode(room) ? room.idOfSlot.filter(Boolean) : room.players;
        memberIds.forEach(id => {
            if (id === socket.id) return;
            io.to(id).emit('skillStatus', { pid: slot, kind: d.kind, ms, mine: id === byId });
        });
        if (room.spectators.size) specEmit(code, room, 'specSkillStatus', { i: slot, kind: d.kind, ms });
    });

    // ===== 대시 이펙트 (다른 사람 화면에 잔상) =====
    socket.on('skillFx', (d) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room || !d || d.kind !== 'dash') return;
        const slot = slotOfSocket(room, socket);
        if (isSlotMode(room) && (!room.started || !room.alive[slot])) return;
        const out = { pid: slot, kind: 'dash', x: nf(d.x), y: nf(d.y), z: nf(d.z), dx: nf(d.dx), dz: nf(d.dz) };
        socket.to(code).emit('skillFx', out);
    });

    socket.on('hit', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);
        const dmg = Math.max(0, Math.min(100, Number(data && data.damage) || 0));
        // 맞은 부위/무기도 같이 전달 (사망 정보 패널과 킬로그에 사용)
        const part = (data && KILL_PARTS.has(data.part)) ? data.part : '';
        const weapon = (data && KILL_WEAPONS.has(Number(data.weapon))) ? Number(data.weapon) : 0;

        if (isSlotMode(room)) {
            // 팀전/데스매치: 맞은 사람에게만 데미지를 전달. 같은 팀/이미 죽은 사람은 무시
            if (!room.started || room.matchOver || !room.alive[slot]) return;
            const tSlot = Number(data && data.target);
            if (!Number.isInteger(tSlot) || tSlot < 0 || tSlot >= room.n || !room.alive[tSlot]) return;
            if (room.teamOfSlot[tSlot] === room.teamOfSlot[slot]) return;
            if (room.mode === 'dm' && Date.now() < room.inv[tSlot]) return; // 부활 직후 무적
            io.to(room.idOfSlot[tSlot]).emit('takeDamage', { damage: dmg, from: slot, part, weapon });
            return;
        }

        socket.to(code).emit('takeDamage', { damage: Number(data && data.damage) || 0, part, weapon });
    });

    socket.on('iDied', (data) => {
        const code = socket.currentRoom;
        const room = code && rooms[code];
        if (!room) return;
        const slot = slotOfSocket(room, socket);

        if (room.mode === 'dm') {
            if (!room.alive[slot]) return;
            room.alive[slot] = false;
            room.hp[slot] = 0;
            room.diedAt[slot] = Date.now();
            let killer = Number(data && data.killer);
            if (!Number.isInteger(killer) || killer < 0 || killer >= room.n || killer === slot || !room.connected[killer]) killer = -1;
            room.deaths[slot] += 1;
            if (killer >= 0) room.kills[killer] += 1;
            io.to(code).emit('playerDied', { slot, killer });
            emitKillFeed(code, room, killer, slot, data && data.weapon, data && data.part);
            emitDmScores(code, room);
            return;
        }

        if (room.mode === 'team') {
            if (!room.started || room.matchOver || !room.alive[slot]) return;
            room.alive[slot] = false;
            room.hp[slot] = 0;
            let killer = Number(data && data.killer);
            if (!Number.isInteger(killer) || killer < 0 || killer >= room.n || room.teamOfSlot[killer] === room.teamOfSlot[slot]) killer = -1;
            io.to(code).emit('playerDied', { slot, killer });
            emitKillFeed(code, room, killer, slot, data && data.weapon, data && data.part);
            specEmit(code, room, 'specDeath', { i: slot, killer, score: room.score });
            checkTeamRound(code, room);
            return;
        }

        socket.to(code).emit('enemyKilled');
        if (slot >= 0) emitKillFeed(code, room, 1 - slot, slot, data && data.weapon, data && data.part);
        if (slot >= 0) {
            const winner = 1 - slot;
            room.hp[slot] = 0;
            room.score[winner] += 1;
            specEmit(code, room, 'specDeath', { i: slot, score: room.score });
            clearTimeout(room.resetTimer);
            if (room.score[winner] >= WIN_SCORE) {
                specEmit(code, room, 'specMatchEnd', { winner, score: room.score });
                // [1.6] 경쟁전: 마지막 라운드가 끝나면 바로 결과를 기록
                if (room.ranked && room.players[winner] && room.players[slot]) {
                    settleRanked(room, room.players[winner], room.players[slot], null);
                }
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
    console.log(`Server running on port ${PORT} (데스매치 방 ${DM_CODE} 열림)`);
    // 서버가 켜질 때 퀴즈를 미리 받아 두고 앱스크립트도 깨워둠 (첫 접속자가 느리게 기다리지 않도록)
    fetchQuizzesFromGas().catch(() => {});
});
