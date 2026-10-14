// Servidor do Allosaurus Online — sem dependências (só Node 16+).
// Local:  node server.js        (porta 8080)
// Nuvem:  Render / Railway / Fly.io -> comando "node server.js" (usa a variável PORT)
const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 8080;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
// ---------- armazenamento das contas ----------
// Com UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN as contas ficam num Redis gratuito (Upstash)
// e NÃO se perdem quando o Render reinicia. Sem essas variáveis, usa um arquivo local (só serve para testes).
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL, REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
async function redis(cmd) {
  const r = await fetch(REDIS_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'content-type': 'application/json' }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
let store;
if (REDIS_URL && REDIS_TOKEN) {
  store = { kind: 'redis',
    get: k => redis(['GET', k]),
    set: (k, v) => redis(['SET', k, v]),
    setnx: async (k, v) => (await redis(['SET', k, v, 'NX'])) === 'OK' };
} else {
  const FILE = process.env.ACCOUNTS_FILE || path.join(__dirname, 'accounts.json');
  let data = {}; try { data = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch (e) {}
  let t = 0; const persist = () => { clearTimeout(t); t = setTimeout(() => { try { fs.writeFileSync(FILE, JSON.stringify(data)); } catch (e) {} }, 500); };
  store = { kind: 'file',
    get: async k => (k in data ? data[k] : null),
    set: async (k, v) => { data[k] = v; persist(); },
    setnx: async (k, v) => { if (k in data) return false; data[k] = v; persist(); return true; } };
  console.warn('AVISO: sem Redis configurado — contas em arquivo local (somem se o Render reiniciar).');
}
const rooms = new Map();          // nome da sala -> Set de clientes
const online = new Map();         // conta (minúscula) -> cliente logado
const fails = new Map();          // ip|conta -> tentativas erradas
const regs = new Map();           // ip -> criações de conta recentes
const clients = new Set();
let nextId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' });
  res.end('Allosaurus server OK · ' + clients.size + ' conectados\n');
});

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  onClient(sock, String(req.headers['x-forwarded-for'] || sock.remoteAddress || '').split(',')[0].trim());
});

function frame(c, op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([128 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
  if (c.sock.destroyed) return;
  if (c.sock.writableLength > 1 << 20) { c.sock.destroy(); return; }   // cliente lento demais
  try { c.sock.write(Buffer.concat([h, payload])); } catch (e) {}
}
const send = (c, o) => frame(c, 1, Buffer.from(JSON.stringify(o)));
function bcast(room, o, except) {
  const set = rooms.get(room); if (!set) return;
  const buf = Buffer.from(JSON.stringify(o));
  set.forEach(c => { if (c !== except) frame(c, 1, buf); });
}
function leaveRoom(c, room) {
  if (!c.rooms.has(room)) return;
  c.rooms.delete(room);
  const set = rooms.get(room);
  if (set) { set.delete(c); if (!set.size) rooms.delete(room); }
  bcast(room, { t: 'pl', room, peer: c.id }, c);
}
function drop(c) {
  if (!clients.delete(c)) return;
  if (c.acc && online.get(c.acc.lower) === c) online.delete(c.acc.lower);
  [...c.rooms.keys()].forEach(r => leaveRoom(c, r));
}

function onClient(sock, ip) {
  const c = { sock, ip, acc: null, id: 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 6),
    rooms: new Map(), buf: Buffer.alloc(0), frag: null, tokens: 150, last: Date.now(), alive: true };
  clients.add(c);
  sock.on('data', d => { c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > 140000) { sock.destroy(); return; } parse(c); });
  sock.on('close', () => drop(c));
  sock.on('error', () => {});
  send(c, { t: 'hello', id: c.id });
}

function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = !!(b[0] & 128), op = b[0] & 15, masked = !!(b[1] & 128);
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    if (!masked || len > 40000) { c.sock.destroy(); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) { c.sock.end(); return; }
    if (op === 9) { frame(c, 10, payload); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 1 || op === 0) {
      if (op === 1) c.frag = [payload]; else if (c.frag && c.frag.length < 4) c.frag.push(payload); else { c.frag = null; continue; }
      if (fin) { const s = Buffer.concat(c.frag).toString('utf8'); c.frag = null; handle(c, s); }
    }
  }
}


// ---------- contas ----------
const sha = x => crypto.createHash('sha256').update(x).digest('hex');
const scrypt = (p, salt) => new Promise((res, rej) => crypto.scrypt(p, salt, 32, (e, k) => (e ? rej(e) : res(k))));
const validName = n => typeof n === 'string' && /^[A-Za-z0-9_.-]{3,16}$/.test(n) && !/^guest/i.test(n);
const validPass = p => typeof p === 'string' && p.length >= 6 && p.length <= 64;
const validSave = v => v && typeof v === 'object' && !Array.isArray(v) && JSON.stringify(v).length <= 20000;
const parse2 = x => { try { return x ? JSON.parse(x) : null; } catch (e) { return null; } };

function attach(c, lower, name) {
  if (c.acc && online.get(c.acc.lower) === c) online.delete(c.acc.lower);
  const old = online.get(lower);
  if (old && old !== c) {          // a conta só pode estar em 1 aparelho: derruba o anterior
    send(old, { t: 'kicked' }); old.acc = null;
    setTimeout(() => { try { old.sock.end(); } catch (e) {} }, 150);
  }
  online.set(lower, c);
  c.acc = { lower, name, lastSave: 0 };
}
function tooMany(c, lower) {
  const f = fails.get(c.ip + '|' + lower);
  return f && Date.now() < f.until;
}
function fail(c, lower) {
  const k = c.ip + '|' + lower, f = fails.get(k) || { n: 0, until: 0 };
  if (++f.n >= 5) { f.until = Date.now() + 5 * 60 * 1000; f.n = 0; }
  fails.set(k, f);
}

async function rpc(c, m) {
  const reply = o => send(c, Object.assign({ t: 'res', id: m.id }, o));
  try {
    const op = m.op;
    const name = typeof m.name === 'string' ? m.name.trim() : '';
    const lower = name.toLowerCase();
    if (op === 'chk') {
      if (!/^[A-Za-z0-9_.-]{3,16}$/.test(name)) return reply({ ok: true, taken: false });
      return reply({ ok: true, taken: !!(await store.get('acc:' + lower)) });
    }
    if (op === 'reg') {
      if (!validName(name)) return reply({ ok: false, err: 'Nome inválido: use 3 a 16 letras, números, _ . ou - (não pode começar com Guest).' });
      if (!validPass(m.pass)) return reply({ ok: false, err: 'A senha precisa ter de 6 a 64 caracteres.' });
      const r = regs.get(c.ip) || [], now = Date.now(), recent = r.filter(t => now - t < 3600e3);
      if (recent.length >= 5) return reply({ ok: false, err: 'Muitas contas criadas deste aparelho. Tente mais tarde.' });
      const salt = crypto.randomBytes(16), hash = await scrypt(m.pass, salt), token = crypto.randomBytes(24).toString('hex');
      const rec = { name, salt: salt.toString('base64'), hash: hash.toString('base64'), tokens: [sha(token)], created: now };
      if (!(await store.setnx('acc:' + lower, JSON.stringify(rec)))) return reply({ ok: false, err: 'Esse nome já está em uso.' });
      recent.push(now); regs.set(c.ip, recent);
      const save = validSave(m.save) ? m.save : null;
      if (save) await store.set('sav:' + lower, JSON.stringify(save));
      attach(c, lower, name);
      return reply({ ok: true, name, token, save });
    }
    if (op === 'login' || op === 'resume') {
      if (!/^[A-Za-z0-9_.-]{3,16}$/.test(name)) return reply({ ok: false, bad: true, err: 'Nome ou senha incorretos.' });
      if (tooMany(c, lower)) return reply({ ok: false, err: 'Muitas tentativas erradas. Aguarde alguns minutos.' });
      const rec = parse2(await store.get('acc:' + lower));
      const bad = () => { fail(c, lower); return reply({ ok: false, bad: true, err: 'Nome ou senha incorretos.' }); };
      if (!rec) return bad();
      let token = null;
      if (op === 'login') {
        if (typeof m.pass !== 'string' || m.pass.length > 64) return bad();
        const hash = await scrypt(m.pass, Buffer.from(rec.salt, 'base64')), good = Buffer.from(rec.hash, 'base64');
        if (hash.length !== good.length || !crypto.timingSafeEqual(hash, good)) return bad();
        token = crypto.randomBytes(24).toString('hex');
        rec.tokens = (rec.tokens || []).slice(-4).concat(sha(token));
        await store.set('acc:' + lower, JSON.stringify(rec));
      } else {
        if (typeof m.token !== 'string' || !(rec.tokens || []).includes(sha(m.token))) return bad();
        token = m.token;
      }
      fails.delete(c.ip + '|' + lower);
      const save = parse2(await store.get('sav:' + lower));
      attach(c, lower, rec.name);
      return reply({ ok: true, name: rec.name, token, save });
    }
    if (op === 'save') {
      if (!c.acc) return reply({ ok: false, err: 'Não está logado.' });
      if (!validSave(m.save)) return reply({ ok: false, err: 'Dados inválidos.' });
      const now = Date.now();
      if (now - c.acc.lastSave < 1500) return reply({ ok: false, err: 'Aguarde.' });
      c.acc.lastSave = now;
      await store.set('sav:' + c.acc.lower, JSON.stringify(m.save));
      return reply({ ok: true });
    }
    if (op === 'logout') {
      if (c.acc && online.get(c.acc.lower) === c) online.delete(c.acc.lower);
      c.acc = null;
      return reply({ ok: true });
    }
    reply({ ok: false, err: 'Operação desconhecida.' });
  } catch (e) {
    console.error('rpc', e && e.message);
    reply({ ok: false, err: 'Erro no servidor. Tente de novo.' });
  }
}

function handle(c, s) {
  if (s.length > 40000) return;
  const now = Date.now();
  c.tokens = Math.min(150, c.tokens + (now - c.last) * 0.08); c.last = now;   // ~80 msgs/s
  if (c.tokens < 1) return; c.tokens--;
  let m; try { m = JSON.parse(s); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'rpc') { rpc(c, m); return; }
  if (s.length > 8192) return;
  const room = typeof m.room === 'string' ? m.room : '';
  if (!/^[a-z0-9_.-]{0,48}$/.test(room)) return;
  if (m.t === 'join') {
    if (c.rooms.has(room) || c.rooms.size >= 16) return;
    c.rooms.set(room, Object.create(null));
    let set = rooms.get(room); if (!set) { set = new Set(); rooms.set(room, set); }
    const peers = []; set.forEach(o => peers.push({ peer: o.id, presence: o.rooms.get(room) }));
    set.add(c);
    send(c, { t: 'peers', room, peers });
  } else if (m.t === 'leave') {
    leaveRoom(c, room);
  } else if (m.t === 'pres') {
    const pr = c.rooms.get(room); if (!pr || !m.p || typeof m.p !== 'object') return;
    for (const k of Object.keys(m.p)) {
      if (!/^[A-Za-z_][A-Za-z0-9_-]{0,31}$/.test(k)) continue;
      if (m.p[k] === null) delete pr[k]; else pr[k] = m.p[k];
    }
    if (JSON.stringify(pr).length > 4096) { for (const k of Object.keys(pr)) delete pr[k]; return; }
    bcast(room, { t: 'pu', room, peer: c.id, presence: pr }, c);
  } else if (m.t === 'emit') {
    if (!c.rooms.has(room) || typeof m.topic !== 'string' || !/^[a-z][a-z0-9_.-]{0,47}$/.test(m.topic)) return;
    if (JSON.stringify(m.data === undefined ? null : m.data).length > 4096) return;
    bcast(room, { t: 'msg', room, topic: m.topic, data: m.data, peer: c.id }, null);
  }
}

setInterval(() => {
  clients.forEach(c => { if (!c.alive) { c.sock.destroy(); return; } c.alive = false; frame(c, 9, Buffer.alloc(0)); });
}, 30000);

server.listen(PORT, () => console.log('Allosaurus server na porta ' + PORT));
