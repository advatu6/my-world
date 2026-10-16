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
// אם מוגדר DATABASE_URL שומרים במסד נתונים חיצוני (Postgres); אחרת בקבצים, כמו עד עכשיו
const DB_URL = process.env.DATABASE_URL;
const pool = DB_URL ? new (require('pg').Pool)({ connectionString: DB_URL, max: 3 }) : null;
const users = Object.create(null);
const dirty = new Set(); // רק חשבונות ששונו נשמרים

async function persist() {
  if (!dirty.size) return;
  const keys = [...dirty]; dirty.clear();
  try {
    if (pool) {
      const obj = {}; for (const k of keys) if (users[k]) obj[k] = users[k];
      await pool.query('INSERT INTO users(key, data) SELECT key, value FROM jsonb_each($1::jsonb) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data', [JSON.stringify(obj)]);
    } else await fs.promises.writeFile(USERS_FILE, JSON.stringify(users));
  } catch (e) { keys.forEach((k) => dirty.add(k)); throw e; }
}

async function loadUsers() {
  let fileUsers = {};
  try { fileUsers = JSON.parse(fs.readFileSync(USERS_FILE, 'utf8')); } catch {}
  if (!pool) return Object.assign(users, fileUsers);
  await pool.query('CREATE TABLE IF NOT EXISTS users (key text PRIMARY KEY, data jsonb NOT NULL)');
  await pool.query('CREATE TABLE IF NOT EXISTS reports (id serial PRIMARY KEY, created timestamptz DEFAULT now(), data jsonb NOT NULL)');
  const r = await pool.query('SELECT key, data FROM users');
  for (const row of r.rows) users[row.key] = row.data;
  if (!r.rows.length && Object.keys(fileUsers).length) { // הפעלה ראשונה: מעבירות את החשבונות מהקובץ
    Object.assign(users, fileUsers); Object.keys(fileUsers).forEach((k) => dirty.add(k)); await persist(); console.log('הועברו חשבונות מ-users.json למסד הנתונים');
  }
}

let saveTimer = null;
const saveUsers = (...keys) => {
  if (!keys.length) Object.keys(users).forEach((k) => dirty.add(k)); else keys.forEach((k) => k && dirty.add(k));
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; persist().catch((e) => console.error('שמירה נכשלה:', e.message)); }, 1500);
};
const shutdown = async () => { try { await persist(); } catch {} process.exit(); };
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
const sha = (t) => crypto.createHash('sha256').update(t).digest('hex');
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');

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
const friendsPayload = (u) => ({
  requests: u.requests.map((k) => ({ key: k, name: nameOf(k) })),
  friends: u.friends.map((k) => {
    const sid = sockByKey[k], p = sid && players[sid];
    return { key: k, name: nameOf(k), online: !!p, room: p ? kindOf(p.room) : null,
      server: p ? Number(String(p.room).split(':')[1] || 0) + 1 : null };
  }),
});
const pushFriendsFor = (key) => {
  const seen = new Set();
  const send = (k) => {
    if (!k || seen.has(k)) return;
    seen.add(k);
    const sid = sockByKey[k], u = users[k];
    if (sid && u) io.to(sid).emit('friends', friendsPayload(u));
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

  socket.on('auth', ({ mode, username, password, color, remember, token }) => {
    if (started) return;
    if (hits('fail:' + ip, 600000) >= 20) return socket.emit('authError', 'יותר מדי ניסיונות. נסי שוב בעוד כמה דקות');
    username = String(username || '').trim();
    password = String(password || '');
    const key = username.toLowerCase();
    const fail = (m) => { record('fail:' + ip, 600000); socket.emit('authError', m); if (++fails >= 8) socket.disconnect(); };
    if (username.length < 2 || username.length > 16) return fail('השם חייב להיות באורך 2 עד 16 תווים');
    if (mode !== 'token' && (password.length < 4 || password.length > 100)) return fail('הסיסמה חייבת להכיל לפחות 4 תווים');
    let user;
    if (mode === 'register') {
      if (users[key]) return fail('השם כבר תפוס');
      if (hits('reg:' + ip, 3600000) >= 5) return fail('נרשמו יותר מדי חשבונות מהכתובת הזו. נסי שוב מאוחר יותר');
      record('reg:' + ip, 3600000);
      const salt = crypto.randomBytes(16).toString('hex');
      user = users[key] = {
        name: username, salt, hash: hashPw(password, salt),
        color: /^#[0-9a-f]{6}$/i.test(color) ? color : '#e07a5f',
        coins: START_COINS, owned: [], blocked: [], friends: [], requests: [],
        wear: { top: null, hat: null, pants: null, shoes: null, glasses: null },
      };
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
    socket.emit('init', { me: socket.id, room: 'square', inst, players: roomPlayers(inst), catalog: CATALOG, emotes: EMOTES });
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
    text = String(text || '').trim().slice(0, 100);
    if (!text) return;
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
    if (pool) pool.query('INSERT INTO reports(data) VALUES ($1::jsonb)', [JSON.stringify(rec)]).catch((e) => console.error(e.message));
    else {
      let list = []; try { list = JSON.parse(fs.readFileSync(REPORTS_FILE, 'utf8')); } catch {}
      list.push(rec);
      fs.writeFile(REPORTS_FILE, JSON.stringify(list, null, 2), () => {});
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
