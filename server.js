// Servidor do Allosaurus Online — sem dependências (só Node 16+).
// Local:  node server.js        (porta 8080)
// Nuvem:  Render / Railway / Fly.io -> comando "node server.js" (usa a variável PORT)
const http = require('http'), crypto = require('crypto');
const PORT = process.env.PORT || 8080;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map();          // nome da sala -> Set de clientes
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
  onClient(sock);
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
  [...c.rooms.keys()].forEach(r => leaveRoom(c, r));
}

function onClient(sock) {
  const c = { sock, id: 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 6),
    rooms: new Map(), buf: Buffer.alloc(0), frag: null, tokens: 150, last: Date.now(), alive: true };
  clients.add(c);
  sock.on('data', d => { c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > 70000) { sock.destroy(); return; } parse(c); });
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
    if (!masked || len > 16384) { c.sock.destroy(); return; }
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

function handle(c, s) {
  if (s.length > 8192) return;
  const now = Date.now();
  c.tokens = Math.min(150, c.tokens + (now - c.last) * 0.08); c.last = now;   // ~80 msgs/s
  if (c.tokens < 1) return; c.tokens--;
  let m; try { m = JSON.parse(s); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
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
