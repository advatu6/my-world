const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static('public'));

const W = 800, H = 500;
const START_COINS = 100, INCOME = 2, INCOME_EVERY_MS = 30000;
const SPEED_PER_TICK = 9;
const MAX_ONLINE = 150;
const ROOM_CAP = 40;
const ROOMS = { square: 'כיכר', forest: 'יער', beach: 'חוף' };
const ARR = { 'square>forest': [120, 300], 'forest>square': [680, 300], 'square>beach': [680, 300], 'beach>square': [120, 300] };
const kindOf = (room) => String(room || '').split(':')[0];
const COIN_VALUE = 3, COIN_COUNT = 6, COIN_RESPAWN_MS = 4000;
const CRAB_COUNT = 4, CRAB_REWARD = 4;
const EMOTES = ['😀', '😂', '😍', '😢', '👍', '💃'];

const CATALOG = [
  { id: 'top_red',   slot: 'top', name: 'חולצה אדומה',  price: 20, color: '#d62828' },
  { id: 'top_blue',  slot: 'top', name: 'חולצה כחולה',  price: 20, color: '#277da1' },
  { id: 'top_green', slot: 'top', name: 'חולצה ירוקה',  price: 20, color: '#43aa8b' },
  { id: 'top_black', slot: 'top', name: 'חולצה שחורה',  price: 30, color: '#222222' },
  { id: 'pants_jeans', slot: 'pants', name: 'ג׳ינס',           price: 25, color: '#3a5a8c' },
  { id: 'pants_black', slot: 'pants', name: 'מכנסיים שחורים', price: 25, color: '#1f1f1f' },
  { id: 'pants_beige', slot: 'pants', name: 'מכנסיים בז׳',    price: 25, color: '#c8ad7f' },
  { id: 'shoes_white', slot: 'shoes', name: 'נעליים לבנות',   price: 20, color: '#f5f5f5' },
  { id: 'shoes_red',   slot: 'shoes', name: 'נעליים אדומות',  price: 20, color: '#c1121f' },
  { id: 'glasses_sun',   slot: 'glasses', name: 'משקפי שמש',     price: 35, icon: '🕶️' },
  { id: 'glasses_round', slot: 'glasses', name: 'משקפיים עגולים', price: 25, icon: '👓' },
  { id: 'hat_cap',   slot: 'hat', name: 'כובע מצחייה',  price: 30, icon: '🧢' },
  { id: 'hat_party', slot: 'hat', name: 'כובע מסיבה',   price: 40, icon: '🥳' },
  { id: 'hat_crown', slot: 'hat', name: 'כתר',          price: 60, icon: '👑' },
];
const BYID = Object.fromEntries(CATALOG.map((i) => [i.id, i]));

// ---- שמירת משתמשות בקובץ ----
const USERS_FILE = path.join(__dirname, 'users.json');
const REPORTS_FILE = path.join(__dirname, 'reports.json');
const DMS_FILE = path.join(__dirname, 'dms.json');
const ADMINS_FILE = path.join(__dirname, 'admins.txt');
// אם מוגדר DATABASE_URL שומרים במסד נתונים חיצוני (Postgres); אחרת בקבצים, כמו עד עכשיו
const DB_URL = process.env.DATABASE_URL;
const pool = DB_URL ? new (require('pg').Pool)({ connectionString: DB_URL, max: 3 }) : null;
// חנות נתונים: כל רשומה נשמרת בנפרד, ורק מה ששונה נכתב
function makeStore(table, file) {
  const data = Object.create(null), dirty = new Set();
  let timer = null;
  async function persist() {
    if (!dirty.size) return;
    const keys = [...dirty]; dirty.clear();
    try {
      if (pool) {
        const obj = {}; for (const k of keys) if (data[k]) obj[k] = data[k];
        await pool.query('INSERT INTO ' + table + '(key, data) SELECT key, value FROM jsonb_each($1::jsonb) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data', [JSON.stringify(obj)]);
      } else await fs.promises.writeFile(file, JSON.stringify(data));
    } catch (e) { keys.forEach((k) => dirty.add(k)); throw e; }
  }
  const save = (...keys) => {
    if (!keys.length) Object.keys(data).forEach((k) => dirty.add(k)); else keys.forEach((k) => k && dirty.add(k));
    if (timer) return;
    timer = setTimeout(() => { timer = null; persist().catch((e) => console.error('שמירה נכשלה:', e.message)); }, 1500);
  };
  async function load() {
    let fromFile = {}; try { fromFile = JSON.parse(fs.readFileSync(file, 'utf8')); } catch {}
    if (!pool) return Object.assign(data, fromFile);
    await pool.query('CREATE TABLE IF NOT EXISTS ' + table + ' (key text PRIMARY KEY, data jsonb NOT NULL)');
    const r = await pool.query('SELECT key, data FROM ' + table);
    for (const row of r.rows) data[row.key] = row.data;
    if (!r.rows.length && Object.keys(fromFile).length) {
      Object.assign(data, fromFile); Object.keys(fromFile).forEach((k) => dirty.add(k)); await persist();
      console.log('הועברו נתונים מקובץ למסד הנתונים:', table);
    }
  }
  return { data, persist, save, load };
}
const U = makeStore('users', USERS_FILE), D = makeStore('dms', DMS_FILE);
const users = U.data, dms = D.data, saveUsers = U.save, saveDms = D.save;
async function loadUsers() {
  if (pool) await pool.query('CREATE TABLE IF NOT EXISTS reports (id serial PRIMARY KEY, created timestamptz DEFAULT now(), data jsonb NOT NULL)');
  await U.load(); await D.load();
}

// ---- דיווחים ----
const readJson = (f, d) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return d; } };
const withIds = (list) => list.map((r, i) => (r.id ? r : { ...r, id: i + 1 }));
const reportsAPI = {
  async add(rec) {
    if (pool) return (await pool.query('INSERT INTO reports(data) VALUES ($1::jsonb) RETURNING id', [JSON.stringify(rec)])).rows[0].id;
    const list = withIds(readJson(REPORTS_FILE, []));
    const id = list.reduce((m, r) => Math.max(m, r.id), 0) + 1;
    list.push({ ...rec, id }); fs.writeFileSync(REPORTS_FILE, JSON.stringify(list, null, 2)); return id;
  },
  async open() {
    if (pool) {
      const r = await pool.query("SELECT id, data FROM reports WHERE COALESCE((data->>'handled')::boolean, false) = false ORDER BY id DESC LIMIT 50");
      return r.rows.map((x) => ({ ...x.data, id: x.id }));
    }
    return withIds(readJson(REPORTS_FILE, [])).filter((r) => !r.handled).reverse().slice(0, 50);
  },
  async handle(id) {
    if (pool) return pool.query("UPDATE reports SET data = data || '{\"handled\": true}'::jsonb WHERE id = $1", [id]);
    const list = withIds(readJson(REPORTS_FILE, [])), r = list.find((x) => x.id === id);
    if (r) { r.handled = true; fs.writeFileSync(REPORTS_FILE, JSON.stringify(list, null, 2)); }
  },
  async all() {
    if (pool) return (await pool.query('SELECT id, created, data FROM reports ORDER BY id')).rows;
    return withIds(readJson(REPORTS_FILE, []));
  },
};
// מנהלות: משתנה סביבה ADMINS (שמות מופרדים בפסיק) או קובץ admins.txt (שם בכל שורה)
const loadAdmins = () => new Set(String(process.env.ADMINS || '').split(',')
  .concat(String(readTextSafe(ADMINS_FILE)).split(/\r?\n/)).map((x) => x.trim().toLowerCase()).filter(Boolean));
function readTextSafe(f) { try { return fs.readFileSync(f, 'utf8'); } catch { return ''; } }

const shutdown = async () => { try { await Promise.all([U.persist(), D.persist()]); } catch {} process.exit(); };
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
const sha = (t) => crypto.createHash('sha256').update(t).digest('hex');
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');

// ---- סינון הודעות ----
// רשימה בסיסית. אפשר להוסיף או להסיר מילים כאן. מילים בעייתיות מוסתרות בכוכביות
const BAD_WORDS_HE = ['זונה', 'שרמוטה', 'בן זונה', 'בת זונה', 'כוס אמק', 'כוסאמק', 'חרא', 'מניאק', 'מזדיין', 'מזדיינת', 'תזדיין', 'לך תזדיין', 'זין', 'בן כלב', 'מפגר', 'מפגרת'];
const BAD_WORDS_EN = ['fuck', 'fucking', 'shit', 'bitch', 'cunt', 'asshole', 'nigger', 'nigga', 'faggot', 'whore', 'slut', 'dick', 'pussy'];
const escRe = (x) => x.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const wordBody = (w) => [...w].map((ch) => (ch === ' ' ? '[\\s._\\-*]+' : escRe(ch))).join('[\\s._\\-*]{0,2}');
const BAD_RES = BAD_WORDS_HE.slice().sort((a, b) => b.length - a.length).map((w) => new RegExp('(?<![א-ת])[והבכלמש]?(?:' + wordBody(w) + ')(?![א-ת])', 'gu'))
  .concat(BAD_WORDS_EN.map((w) => new RegExp('(?<![a-z])(?:' + wordBody(w) + ')(?![a-z])', 'gi')));
const maskBad = (t) => BAD_RES.reduce((acc, re) => acc.replace(re, (m) => '*'.repeat(m.length)), t);
const LINK_RE = /(https?:\/\/|www\.|[a-z0-9-]+\.(com|net|org|co|il|io|me|ly|xyz|info|biz|ru|tk|gg|app|dev)\b)/i;
const PHONE_RE = /\d[\d\s\-.]{6,}\d/;
// מחזירה { text } או { error } (error ריק = להתעלם בשקט)
function filterText(raw, max, st) {
  let t = String(raw || '').replace(/[\u0591-\u05C7\u200B-\u200F\u202A-\u202E\u2060\uFEFF\u0000-\u001F]/g, '').replace(/\s+/g, ' ').trim().slice(0, max);
  if (!t) return { error: '' };
  t = t.replace(/(.)\1{4,}/gu, '$1$1$1');
  if (LINK_RE.test(t) || PHONE_RE.test(t)) return { error: 'לא ניתן לשלוח קישורים או מספרי טלפון' };
  t = maskBad(t);
  if (st) {
    if (st.lastText === t && Date.now() - st.lastAt < 15000) return { error: 'כבר כתבת את זה' };
    st.lastText = t; st.lastAt = Date.now();
  }
  return { text: t };
}

// ---- שמות שמורים ----
const RESERVED = ['admin', 'administrator', 'moderator', 'staff', 'support', 'system', 'owner', 'manager', 'olam', 'מנהל', 'מנהלת', 'מנהלים', 'אדמין', 'מודרטור', 'תמיכה', 'מערכת', 'בעלים', 'בעלת', 'העולם', 'עולם', 'צוות'];
const LEET = { 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', 7: 't', '@': 'a', $: 's' };
const reservedName = (n) => {
  const low = [...String(n).toLowerCase()];
  const flats = [low.map((c) => LEET[c] || c).join(''), low.join('')].map((x) => x.replace(/[^a-z\u05d0-\u05ea]/g, ''));
  return flats.some((flat) => RESERVED.some((r) => flat === r || (r.length >= 5 && flat.includes(r))));
};

// ---- קוד שחזור סיסמה ----
const newCode = () => {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let c = '';
  for (let i = 0; i < 12; i++) c += A[crypto.randomInt(A.length)];
  return c.match(/.{4}/g).join('-');
};
const normCode = (c) => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const setRecovery = (u) => {
  const code = newCode();
  u.recSalt = crypto.randomBytes(16).toString('hex'); u.recHash = hashPw(normCode(code), u.recSalt);
  return code;
};

// ---- הודעות פרטיות ----
const pairKey = (a, b) => (a < b ? a + '|' + b : b + '|' + a);
const dmUnread = (u, myKey, fk) => {
  const read = (u.dmRead || {})[fk] || 0;
  return (dms[pairKey(myKey, fk)] || []).filter((m) => m.f === fk && m.ts > read).length;
};

// ---- הגבלת קצב לפי כתובת רשת ----
const buckets = new Map();
const hits = (key, windowMs) => (buckets.get(key) || []).filter((t) => Date.now() - t < windowMs).length;
const record = (key, windowMs) => { const a = (buckets.get(key) || []).filter((t) => Date.now() - t < windowMs); a.push(Date.now()); buckets.set(key, a); };
setInterval(() => { for (const [k, v] of buckets) if (!v.length || Date.now() - v[v.length - 1] > 3600000) buckets.delete(k); }, 600000);
const ipOf = (s) => String(s.handshake.headers['x-forwarded-for'] || '').split(',')[0].trim() || s.handshake.address;

const players = {};  // מה שכולן רואות
const wallets = {};  // socket.id -> רשומת המשתמשת (פרטי)
const online = new Set();
const sockByKey = {};
const recentChat = [];
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const sendMe = (id) => {
  const w = wallets[id];
  if (w) io.to(id).emit('me', { coins: w.coins, owned: w.owned, blocked: w.blocked });
};
const pub = (p) => ({ ...p, room: kindOf(p.room) });
const roomPlayers = (room) =>
  Object.fromEntries(Object.entries(players).filter(([, p]) => p.room === room).map(([id, p]) => [id, pub(p)]));
const instCount = (inst) => {
  let n = 0;
  for (const id in players) if (players[id].room === inst) n++;
  return n;
};
const countsOfKind = (kind) => {
  const counts = {};
  for (const id in players) {
    const r = players[id].room;
    if (kindOf(r) === kind) counts[r] = (counts[r] || 0) + 1;
  }
  return counts;
};
// רמת מילוי יחסית 0 עד 4 (ריק, רבע, חצי, שלושת רבעים, מלא). לא חושפים כמה מקומות נשארו
const fillLevel = (n, cap) => (n <= 0 ? 0 : n >= cap ? 4 : Math.min(3, Math.max(1, Math.round((n / cap) * 4))));
let countsTimer = null;
const emitCounts = () => { // מרוכז: לכל היותר פעם בשנייה
  if (countsTimer) return;
  countsTimer = setTimeout(() => {
    countsTimer = null;
    const rooms = {};
    for (const r in ROOMS) {
      const counts = countsOfKind(r);
      rooms[r] = Object.keys(counts).sort((a, b) => Number(a.split(':')[1]) - Number(b.split(':')[1]))
        .map((id) => ({ id, no: Number(id.split(':')[1]) + 1, level: fillLevel(counts[id], ROOM_CAP), full: counts[id] >= ROOM_CAP }));
    }
    io.emit('counts', { world: fillLevel(online.size, MAX_ONLINE), rooms });
  }, 1000);
};
const pickInstance = (kind) => {
  const counts = countsOfKind(kind);
  let best = null, bestN = Infinity;
  for (const [inst, n] of Object.entries(counts)) {
    if (n < ROOM_CAP && n < bestN) { best = inst; bestN = n; }
  }
  if (best) return best;
  let i = 0;
  while (counts[kind + ':' + i]) i++;
  return kind + ':' + i;
};
const kindIsFull = (kind) => {
  const counts = countsOfKind(kind);
  const ids = Object.keys(counts);
  return ids.length > 0 && ids.every((id) => counts[id] >= ROOM_CAP);
};
// שליחה לחדר, בלי מי שחסמה את השולחת
const sayTo = (room, sender, event, payload) => {
  for (const sid of io.sockets.adapter.rooms.get(room) || []) {
    const w = wallets[sid];
    if (w && !w.blocked.includes(sender.name.toLowerCase())) io.to(sid).emit(event, payload);
  }
};

// ---- חברות ----
const nameOf = (k) => (users[k] ? users[k].name : k);
const friendsPayload = (u, myKey) => ({
  requests: u.requests.map((k) => ({ key: k, name: nameOf(k) })),
  friends: u.friends.map((k) => {
    const sid = sockByKey[k], p = sid && players[sid];
    return { key: k, name: nameOf(k), online: !!p, room: p ? kindOf(p.room) : null,
      server: p ? Number(String(p.room).split(':')[1] || 0) + 1 : null,
      unread: dmUnread(u, myKey, k) };
  }),
});
const pushFriendsFor = (key) => {
  const seen = new Set();
  const send = (k) => {
    if (!k || seen.has(k)) return;
    seen.add(k);
    const sid = sockByKey[k], u = users[k];
    if (sid && u) io.to(sid).emit('friends', friendsPayload(u, k));
  };
  send(key);
  const u = users[key];
  if (u) {
    for (const k of u.friends) send(k);
    for (const k of u.requests) send(k);
  }
};
const without = (arr, k) => arr.filter((x) => x !== k);
const link = (a, ak, b, bk) => {
  a.requests = without(a.requests, bk); b.requests = without(b.requests, ak);
  if (!a.friends.includes(bk)) a.friends.push(bk);
  if (!b.friends.includes(ak)) b.friends.push(ak);
  saveUsers(ak, bk); pushFriendsFor(ak); pushFriendsFor(bk);
};

let coinSeq = 0;
const newCoin = () => ({ id: ++coinSeq, x: Math.round(100 + Math.random() * 640), y: Math.round(170 + Math.random() * 300) });
const forestCoinsByInst = Object.create(null);
const coinsOf = (inst) => {
  if (!forestCoinsByInst[inst]) forestCoinsByInst[inst] = Array.from({ length: COIN_COUNT }, newCoin);
  return forestCoinsByInst[inst];
};

let crabSeq = 0;
const newCrab = (awayFromX) => {
  const left = awayFromX !== undefined ? awayFromX > 400 : Math.random() < 0.5;
  return { id: ++crabSeq, a: Math.random() * 6.28,
    x: Math.round(left ? 80 + Math.random() * 140 : 580 + Math.random() * 140),
    y: Math.round(180 + Math.random() * 280) };
};
const crabsByInst = Object.create(null);
const crabsOf = (inst) => {
  if (!crabsByInst[inst]) crabsByInst[inst] = Array.from({ length: CRAB_COUNT }, () => newCrab());
  return crabsByInst[inst];
};

const dropIfEmpty = (inst) => {
  if (inst && instCount(inst) === 0) { delete forestCoinsByInst[inst]; delete crabsByInst[inst]; }
};

io.on('connection', (socket) => {
  let fails = 0, started = false;
  const reported = new Set();
  const ip = ipOf(socket);

  socket.on('auth', ({ mode, username, password, color, remember, token, agreed }) => {
    if (started) return;
    if (hits('fail:' + ip, 600000) >= 20) return socket.emit('authError', 'יותר מדי ניסיונות. נסי שוב בעוד כמה דקות');
    username = String(username || '').trim();
    password = String(password || '');
    const key = username.toLowerCase();
    const fail = (m) => { record('fail:' + ip, 600000); socket.emit('authError', m); if (++fails >= 8) socket.disconnect(); };
    if (username.length < 2 || username.length > 16) return fail('השם חייב להיות באורך 2 עד 16 תווים');
    if (mode !== 'token' && (password.length < 4 || password.length > 100)) return fail('הסיסמה חייבת להכיל לפחות 4 תווים');
    let user, newRecovery = null;
    if (mode === 'register') {
      if (users[key]) return fail('השם כבר תפוס');
      if (hits('reg:' + ip, 3600000) >= 5) return fail('נרשמו יותר מדי חשבונות מהכתובת הזו. נסי שוב מאוחר יותר');
      record('reg:' + ip, 3600000);
      if (agreed !== true) return fail('צריך לאשר שאת בת 18 ומעלה ומסכימה לתנאי השימוש');
      if (!loadAdmins().has(key) && reservedName(username)) return fail('השם הזה שמור, נא לבחור שם אחר');
      if (maskBad(username) !== username) return fail('השם לא מתאים, נא לבחור שם אחר');
      const salt = crypto.randomBytes(16).toString('hex');
      user = users[key] = {
        name: username, salt, hash: hashPw(password, salt),
        color: /^#[0-9a-f]{6}$/i.test(color) ? color : '#e07a5f',
        coins: START_COINS, owned: [], blocked: [], friends: [], requests: [], dmRead: {}, terms: { at: Date.now(), v: 1 },
        wear: { top: null, hat: null, pants: null, shoes: null, glasses: null },
      };
      newRecovery = setRecovery(user);
      saveUsers(key);
    } else if (mode === 'token') {
      user = users[key];
      const ok = user && (user.tokens || []).includes(sha(String(token || '')));
      if (!ok) { socket.emit('tokenBad'); return fail('הכניסה האוטומטית פגה, נא להיכנס עם סיסמה'); }
    } else {
      user = users[key];
      const ok = user && crypto.timingSafeEqual(Buffer.from(hashPw(password, user.salt), 'hex'), Buffer.from(user.hash, 'hex'));
      if (!ok) return fail('שם או סיסמה לא נכונים');
    }
    for (const f of ['blocked', 'friends', 'requests', 'tokens']) if (!user[f]) user[f] = [];
    if (user.banned) return fail('החשבון הושעה');
    if (online.has(key)) return fail('המשתמשת הזו כבר מחוברת');
    if (online.size >= MAX_ONLINE) return fail('העולם מלא כרגע. נסי בעוד כמה דקות');
    started = true; online.add(key); socket.data.key = key; sockByKey[key] = socket.id;
    if (remember && mode !== 'token') { // "זה המחשב שלי": שומרים מפתח כניסה, לא את הסיסמה
      const tok = crypto.randomBytes(24).toString('hex');
      user.tokens = user.tokens.slice(-4).concat(sha(tok)); saveUsers(key);
      socket.emit('token', { username: user.name, token: tok, color: user.color });
    }
    wallets[socket.id] = user;
    const inst = pickInstance('square');
    const p = players[socket.id] = {
      id: socket.id, name: user.name, color: user.color, room: inst,
      x: 400, y: 300, tx: 400, ty: 300, wear: user.wear,
    };
    socket.join(inst);
    socket.emit('init', { me: socket.id, room: 'square', inst, players: roomPlayers(inst), catalog: CATALOG, emotes: EMOTES,
      needTerms: !user.terms, hasRecovery: !!user.recHash, admin: loadAdmins().has(key) });
    if (newRecovery) socket.emit('recoveryCode', { code: newRecovery, first: true });
    if (loadAdmins().has(key)) reportsAPI.open().then((l) => socket.emit('adminAlert', { count: l.length })).catch(() => {});
    sendMe(socket.id);
    socket.to(inst).emit('joined', pub(p));
    emitCounts();
    pushFriendsFor(key);
  });

  const enterInst = (inst) => {
    const p = players[socket.id];
    if (!p || p.room === inst) return;
    const from = kindOf(p.room), to = kindOf(inst);
    const [ax, ay] = ARR[from + '>' + to] || [400, 300];
    const oldRoom = p.room;
    socket.leave(p.room);
    io.to(p.room).emit('left', p.id);
    p.room = inst; p.x = p.tx = ax; p.y = p.ty = ay;
    socket.join(inst);
    socket.emit('room', { room: to, inst, players: roomPlayers(inst), coins: to === 'forest' ? coinsOf(inst) : [] });
    socket.to(inst).emit('joined', pub(p));
    dropIfEmpty(oldRoom);
    emitCounts();
    pushFriendsFor(socket.data.key);
  };
  const go = (kind) => {
    const p = players[socket.id];
    if (!p || !ROOMS[kind] || kindOf(p.room) === kind) return;
    enterInst(pickInstance(kind));
  };
  socket.on('goto', go);

  socket.on('joinServer', (id) => {
    const p = players[socket.id];
    const inst = String(id || '');
    const kind = kindOf(inst);
    if (!p || !ROOMS[kind] || !/^(square|forest|beach):\d+$/.test(inst)) return;
    const n = instCount(inst);
    if (n <= 0) return socket.emit('notice', 'השרת הזה סגור');
    if (n >= ROOM_CAP && p.room !== inst) return socket.emit('notice', 'השרת מלא');
    enterInst(inst);
  });
  socket.on('openServer', (kind) => {
    const p = players[socket.id];
    if (!p || !ROOMS[kind]) return;
    if (!kindIsFull(kind) && Object.keys(countsOfKind(kind)).length) {
      return socket.emit('notice', 'יש עוד מקום בשרת פתוח');
    }
    enterInst(pickInstance(kind));
  });

  socket.on('move', ({ x, y }) => {
    const p = players[socket.id];
    if (!p) return;
    p.tx = clamp(Number(x) || 0, 20, W - 20);
    p.ty = clamp(Number(y) || 0, 140, H - 20);
    io.to(p.room).emit('moved', { id: p.id, x: p.tx, y: p.ty });
  });

  let lastChat = 0, lastEmote = 0;
  socket.on('chat', (text) => {
    const p = players[socket.id];
    const now = Date.now();
    if (!p || now - lastChat < 700) return;
    lastChat = now;
    const r = filterText(text, 100, socket.data);
    if (r.error !== undefined) { if (r.error) socket.emit('notice', r.error); return; }
    text = r.text;
    recentChat.push({ user: p.name, text }); if (recentChat.length > 200) recentChat.shift();
    sayTo(p.room, p, 'chat', { id: p.id, text });
  });

  socket.on('emote', (i) => {
    const p = players[socket.id];
    const now = Date.now();
    if (!p || !EMOTES[i] || now - lastEmote < 500) return;
    lastEmote = now;
    sayTo(p.room, p, 'emote', { id: p.id, e: i });
  });

  // חסימה (מתחלפת: חסימה / ביטול)
  socket.on('block', (tid) => {
    const w = wallets[socket.id], t = players[tid];
    if (!w || !t || tid === socket.id) return;
    const k = t.name.toLowerCase(), i = w.blocked.indexOf(k);
    if (i >= 0) w.blocked.splice(i, 1); else w.blocked.push(k);
    sendMe(socket.id); saveUsers(socket.data.key);
  });

  // דיווח: נשמר בקובץ reports.json יחד עם ההודעות האחרונות שלה
  socket.on('report', (tid) => {
    const p = players[socket.id], t = players[tid];
    if (!p || !t || tid === socket.id || reported.has(tid)) return;
    reported.add(tid);
    const rec = { time: new Date().toISOString(), reporter: p.name, reported: t.name,
      recent: recentChat.filter((m) => m.user === t.name).slice(-5) };
    reportsAPI.add(rec).then(() => reportsAPI.open()).then((open) => {
      for (const k of loadAdmins()) if (sockByKey[k]) io.to(sockByKey[k]).emit('adminAlert', { count: open.length, text: p.name + ' דיווחה על ' + t.name });
    }).catch((e) => console.error('דיווח נכשל:', e.message));
  });

  // ---- ניהול (רק למנהלות) ----
  const isAdmin = () => loadAdmins().has(socket.data.key);
  const sendReports = async () => {
    const list = await reportsAPI.open();
    socket.emit('adminReports', list.map((r) => ({ ...r, banned: !!(users[String(r.reported).toLowerCase()] || {}).banned })));
    socket.emit('adminAlert', { count: list.length });
  };
  socket.on('adminReports', () => { if (isAdmin()) sendReports().catch(() => {}); });
  socket.on('adminHandle', (id) => { if (isAdmin()) reportsAPI.handle(Number(id)).then(sendReports).catch(() => {}); });
  socket.on('adminBan', ({ name, on } = {}) => {
    if (!isAdmin()) return;
    const k = String(name || '').toLowerCase(), u = users[k];
    if (!u || loadAdmins().has(k)) return;
    u.banned = !!on; saveUsers(k);
    if (on && sockByKey[k]) { const so = io.sockets.sockets.get(sockByKey[k]); if (so) so.disconnect(true); }
    sendReports().catch(() => {});
  });
  socket.on('adminBackup', async () => {
    if (!isAdmin()) return;
    try { socket.emit('backup', JSON.stringify({ exported: new Date().toISOString(), users, dms, reports: await reportsAPI.all() })); }
    catch (e) { socket.emit('notice', 'הגיבוי נכשל: ' + e.message); }
  });

  // ---- תנאי שימוש, קוד שחזור, איפוס סיסמה ----
  socket.on('acceptTerms', () => {
    const w = wallets[socket.id]; if (!w) return;
    w.terms = { at: Date.now(), v: 1 }; saveUsers(socket.data.key);
  });
  socket.on('newRecovery', () => {
    const w = wallets[socket.id]; if (!w) return;
    const code = setRecovery(w); saveUsers(socket.data.key);
    socket.emit('recoveryCode', { code, first: false });
  });
  socket.on('resetPw', ({ username, code, password } = {}) => {
    if (started) return;
    const bad = (m) => { record('fail:' + ip, 600000); socket.emit('authError', m); };
    if (hits('fail:' + ip, 600000) >= 20) return socket.emit('authError', 'יותר מדי ניסיונות. נסי שוב בעוד כמה דקות');
    const k = String(username || '').trim().toLowerCase(), u = users[k], pw = String(password || '');
    if (pw.length < 4 || pw.length > 100) return bad('הסיסמה החדשה חייבת להכיל לפחות 4 תווים');
    const ok = u && u.recHash && crypto.timingSafeEqual(Buffer.from(hashPw(normCode(code), u.recSalt), 'hex'), Buffer.from(u.recHash, 'hex'));
    if (!ok) return bad('השם או קוד השחזור לא נכונים');
    u.salt = crypto.randomBytes(16).toString('hex'); u.hash = hashPw(pw, u.salt); u.tokens = [];
    const fresh = setRecovery(u); saveUsers(k);
    socket.emit('resetOk', { code: fresh });
  });

  // ---- הודעות פרטיות (נשמרות, עד 100 אחרונות בכל שיחה) ----
  socket.on('dmOpen', (fk) => {
    const me = wallets[socket.id], k = String(fk || '').toLowerCase(), mk = socket.data.key;
    if (!me || !me.friends.includes(k)) return;
    socket.data.dmView = k;
    me.dmRead = me.dmRead || {}; me.dmRead[k] = Date.now(); saveUsers(mk);
    socket.emit('dmHistory', { key: k, msgs: (dms[pairKey(mk, k)] || []).map((m) => ({ me: m.f === mk, text: m.t, ts: m.ts })) });
    pushFriendsFor(mk);
  });
  socket.on('dmClose', () => { socket.data.dmView = null; });
  let lastDm = 0;
  socket.on('dmSend', ({ to, text } = {}) => {
    const me = wallets[socket.id], mk = socket.data.key, k = String(to || '').toLowerCase(), other = users[k];
    if (!me || !other || !me.friends.includes(k)) return;
    const now = Date.now(); if (now - lastDm < 800) return; lastDm = now;
    if (other.blocked.includes(mk)) return socket.emit('notice', 'לא ניתן לשלוח הודעה לחברה הזו');
    const r = filterText(text, 300, socket.data);
    if (r.error !== undefined) { if (r.error) socket.emit('notice', r.error); return; }
    const pk = pairKey(mk, k), list = dms[pk] || (dms[pk] = []);
    list.push({ f: mk, t: r.text, ts: now }); if (list.length > 100) list.shift();
    saveDms(pk);
    socket.emit('dm', { key: k, me: true, text: r.text, ts: now });
    const sid = sockByKey[k];
    if (sid) {
      const so = io.sockets.sockets.get(sid), viewing = so && so.data.dmView === mk;
      if (viewing) { other.dmRead = other.dmRead || {}; other.dmRead[mk] = now; saveUsers(k); }
      io.to(sid).emit('dm', { key: mk, me: false, text: r.text, ts: now });
      if (!viewing) pushFriendsFor(k);
    }
  });

  // "הסרה ממחשב זה": מוחקת את מפתח הכניסה (אפשר גם בלי להיות מחוברת)
  socket.on('forget', ({ username, token } = {}) => {
    const u = users[String(username || '').toLowerCase()];
    if (!u) return;
    const h = sha(String(token || ''));
    u.tokens = (u.tokens || []).filter((t) => t !== h); saveUsers(String(username || '').toLowerCase());
  });

  socket.on('logout', ({ token } = {}) => {
    const w = wallets[socket.id]; if (!w) return;
    const h = sha(String(token || ''));
    w.tokens = (w.tokens || []).filter((t) => t !== h); saveUsers(socket.data.key);
  });

  // בקשות חברות
  socket.on('friendReq', (tid) => {
    const me = wallets[socket.id], p = players[socket.id], t = players[tid], tw = wallets[tid];
    if (!me || !p || !t || !tw || tid === socket.id) return;
    const mk = socket.data.key, tk = t.name.toLowerCase();
    const say = (m) => socket.emit('notice', m);
    if (me.friends.includes(tk)) return say('אתן כבר חברות');
    if (tw.blocked.includes(mk) || me.blocked.includes(tk)) return say('הבקשה נשלחה ✓');
    if (me.requests.includes(tk)) { link(me, mk, tw, tk); io.to(tid).emit('notice', p.name + ' אישרה את הבקשה שלך 🎉'); return say('אתן עכשיו חברות! 🎉'); }
    if (tw.requests.includes(mk)) return say('כבר שלחת לה בקשה');
    if (tw.requests.length >= 50) return say('הבקשה נשלחה ✓');
    tw.requests.push(mk); saveUsers(tk); pushFriendsFor(tk);
    io.to(tid).emit('notice', p.name + ' שלחה לך בקשת חברות 👥');
    say('הבקשה נשלחה ✓');
  });

  socket.on('friendRespond', ({ key, accept }) => {
    const me = wallets[socket.id], k = String(key || '').toLowerCase(), other = users[k];
    if (!me || !me.requests.includes(k)) return;
    if (accept && other) {
      link(me, socket.data.key, other, k);
      if (sockByKey[k]) io.to(sockByKey[k]).emit('notice', me.name + ' אישרה את הבקשה שלך 🎉');
    } else { me.requests = without(me.requests, k); saveUsers(socket.data.key); pushFriendsFor(socket.data.key); }
  });

  socket.on('unfriend', (key) => {
    const me = wallets[socket.id], k = String(key || '').toLowerCase();
    if (!me || !me.friends.includes(k)) return;
    me.friends = without(me.friends, k);
    if (users[k]) users[k].friends = without(users[k].friends, socket.data.key);
    saveUsers(socket.data.key, k); pushFriendsFor(socket.data.key); pushFriendsFor(k);
  });

  socket.on('joinFriend', (key) => {
    const me = wallets[socket.id], k = String(key || '').toLowerCase();
    if (!me || !me.friends.includes(k)) return;
    const fp = players[sockByKey[k]];
    if (fp) enterInst(fp.room);
  });

  socket.on('collect', (cid) => {
    const p = players[socket.id], w = wallets[socket.id];
    if (!p || !w || kindOf(p.room) !== 'forest') return;
    const list = coinsOf(p.room);
    const i = list.findIndex((c) => c.id === cid);
    if (i < 0) return;
    const c = list[i];
    if (Math.hypot(p.x - c.x, p.y - c.y) > 55) return;
    list.splice(i, 1);
    w.coins += COIN_VALUE; sendMe(socket.id);
    io.to(p.room).emit('coins', list);
    const inst = p.room;
    setTimeout(() => { list.push(newCoin()); io.to(inst).emit('coins', list); }, COIN_RESPAWN_MS);
  });

  socket.on('buy', (itemId) => {
    const w = wallets[socket.id], p = players[socket.id], item = BYID[itemId];
    if (!w || !p || !item || w.owned.includes(itemId) || w.coins < item.price) return;
    w.coins -= item.price; w.owned.push(itemId);
    p.wear[item.slot] = itemId;
    sendMe(socket.id); saveUsers(socket.data.key);
    io.to(p.room).emit('wore', { id: p.id, wear: p.wear });
  });

  socket.on('wear', ({ slot, id }) => {
    const w = wallets[socket.id], p = players[socket.id];
    if (!w || !p || !(slot in p.wear)) return;
    if (id === null) p.wear[slot] = null;
    else if (BYID[id] && BYID[id].slot === slot && w.owned.includes(id)) p.wear[slot] = id;
    else return;
    saveUsers(socket.data.key);
    io.to(p.room).emit('wore', { id: p.id, wear: p.wear });
  });

  socket.on('disconnect', () => {
    const p = players[socket.id];
    if (p) io.to(p.room).emit('left', p.id);
    const key = socket.data.key;
    const leftRoom = p && p.room;
    if (key) { online.delete(key); delete sockByKey[key]; saveUsers(key); }
    delete players[socket.id]; delete wallets[socket.id];
    dropIfEmpty(leftRoom);
    emitCounts();
    if (key) pushFriendsFor(key);
  });
});

setInterval(() => {
  for (const id in players) {
    const p = players[id];
    const dx = p.tx - p.x, dy = p.ty - p.y, d = Math.hypot(dx, dy);
    if (d > 0) { const s = Math.min(d, SPEED_PER_TICK); p.x += dx / d * s; p.y += dy / d * s; }
  }
  // משחק החוף: סרטנים שבורחים, ומי שתופסת אותם מקבלת מטבעות
  const byBeach = {};
  for (const p of Object.values(players)) {
    if (kindOf(p.room) === 'beach') (byBeach[p.room] || (byBeach[p.room] = [])).push(p);
  }
  for (const inst in byBeach) {
    const beach = byBeach[inst];
    const crabs = crabsOf(inst);
    crabs.forEach((c, i) => {
      let near = null, nd = 110;
      for (const p of beach) { const d = Math.hypot(p.x - c.x, p.y - c.y); if (d < nd) { nd = d; near = p; } }
      if (near) {
        const dx = c.x - near.x, dy = c.y - near.y, d = Math.hypot(dx, dy) || 1;
        c.x += dx / d * 5; c.y += dy / d * 5; // בורחים לאט יותר מהליכה (5 מול 9)
      } else {
        if (Math.random() < 0.05) c.a = Math.random() * 6.28;
        c.x += Math.cos(c.a) * 1.5; c.y += Math.sin(c.a) * 1.5;
      }
      c.x = clamp(c.x, 30, 770); c.y = clamp(c.y, 150, 480);
      const catcher = beach.find((p) => Math.hypot(p.x - c.x, p.y - c.y) < 26);
      if (catcher && wallets[catcher.id]) {
        wallets[catcher.id].coins += CRAB_REWARD; sendMe(catcher.id);
        io.to(inst).emit('pop', { x: Math.round(c.x), y: Math.round(c.y), text: '+' + CRAB_REWARD });
        crabs[i] = newCrab(catcher.x);
      }
    });
    io.to(inst).emit('crabs', crabs.map((c) => ({ id: c.id, x: Math.round(c.x), y: Math.round(c.y) })));
  }
}, 50);

let incomeTicks = 0;
setInterval(() => {
  for (const id in wallets) { wallets[id].coins += INCOME; sendMe(id); }
  if (++incomeTicks % 2 === 0 && Object.keys(sockByKey).length) saveUsers(...Object.keys(sockByKey)); // פעם בדקה
}, INCOME_EVERY_MS);

const PORT = process.env.PORT || 3000;
loadUsers()
  .then(() => server.listen(PORT, () => console.log('http://localhost:' + PORT)))
  .catch((e) => { console.error('שגיאה בהפעלה:', e.message); process.exit(1); });
