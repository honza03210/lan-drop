// LAN Drop — discover devices on your local network and send files/text between them.
// Zero dependencies: run with `node server.js` (Node 18+), then open the printed URL
// on every device connected to the same network.
//
// Files are never stored on this computer. When the receiver downloads, the server asks
// the sender's page to upload exactly those bytes and pipes them straight through:
//
//   receiver  --GET /api/dl/..-->  server  --SSE "pull" {offset}-->  sender page
//   receiver  <---- bytes ------  server  <---- POST /api/push ----  sender page
//
// If either side drops, the transfer continues from the last byte that got through:
// the server re-requests the sender from that offset, and the receiver's browser can
// resume its download with a Range request.

'use strict';
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');

const PORT = Number(process.env.PORT) || 8080;
const INDEX = path.join(__dirname, 'index.html');

const PEER_GRACE_MS = 20 * 1000;          // keep a disconnected device listed this long (rides out Wi-Fi blips)
const SENDER_AWAY_MS = 10 * 60 * 1000;    // cancel a sender's offers if its page stays away this long
const PEER_FORGET_MS = 24 * 60 * 60 * 1000;
const PUSH_RETRY_MS = 2000;               // wait before re-asking the sender after a failed push
const MAX_PUSH_FAILS = 5;                 // consecutive pushes with no progress before giving up
const MAX_TEXT = 1024 * 1024;
const MAX_FILES = 200000;
const MAX_OFFER_JSON = 64 * 1024 * 1024;

// Random (incompressible) payload reused for speed-test downloads.
const SPEED_CHUNK = crypto.randomBytes(1024 * 1024);
const SPEED_MAX = 1024 * 1024 * 1024;

const newId = (bytes = 8) => crypto.randomBytes(bytes).toString('hex');

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/** id -> { id, secret, name, device, res, listed, hideTimer, awayTimer, forgetTimer } */
const peers = new Map();
/** secret -> peer. The id is public (everyone sees it in the device list); the secret proves who you are. */
const bySecret = new Map();
/** transferId -> { id, from, to, files: [{ path, size, type }], total, at, pulls: Set } */
const transfers = new Map();
/** pullId -> pull (see startPull) */
const pulls = new Map();

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function lanAddresses() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      // skip loopback and 169.254.x.x (self-assigned, unreachable from other devices)
      if (a.family === 'IPv4' && !a.internal && !a.address.startsWith('169.254.')) out.push(a.address);
    }
  }
  return out;
}

function sse(res, event, data) {
  if (res && !res.destroyed) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}
function sendTo(peerId, event, data) {
  const p = peers.get(peerId);
  if (p && p.res) sse(p.res, event, data);
  return !!(p && p.res);
}

function peerList() {
  return [...peers.values()].filter((p) => p.listed).map(({ id, name, device }) => ({ id, name, device }));
}
function broadcastPeers() {
  const list = peerList();
  for (const p of peers.values()) if (p.res) sse(p.res, 'peers', list);
}

function json(res, status, body) {
  if (res.headersSent) return;
  res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(body));
}
function text(res, status, msg) {
  if (res.headersSent) return;
  res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(msg);
}

function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('Payload too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function clean(s, max = 60) {
  return String(s ?? '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max);
}

/** Relative path from the sender ("Photos/2024/img.jpg"), made safe for zips and file systems. */
function safePath(p) {
  const parts = String(p ?? '')
    .replace(/\\/g, '/')
    .split('/')
    .map((s) => clean(s, 200).replace(/[:*?"<>|]/g, '_').replace(/[. ]+$/, ''))
    .filter((s) => s && s !== '.' && s !== '..');
  return parts.join('/').slice(0, 1000) || 'file';
}
const baseName = (p) => p.slice(p.lastIndexOf('/') + 1);

function auth(k) {
  return (k && bySecret.get(k)) || null;
}

// ---------------------------------------------------------------------------
// Transfers and pulls
// ---------------------------------------------------------------------------

function offerPayload(t) {
  const from = peers.get(t.from);
  return {
    transferId: t.id,
    from: t.from,
    fromName: from ? from.name : 'device',
    files: t.files.map((f) => ({ path: f.path, size: f.size })),
    total: t.total,
    at: t.at,
  };
}

function cancelTransfer(t, by) {
  if (!transfers.has(t.id)) return;
  transfers.delete(t.id);
  for (const pull of [...t.pulls]) abortPull(pull);
  sendTo(t.from, 'cancel', { transferId: t.id, by });
  sendTo(t.to, 'cancel', { transferId: t.id, by });
}

/**
 * A pull moves bytes [start, size) of one file from the sender's page into `out`
 * (an HTTP response). It survives the sender's upload dropping: the next push
 * continues at start + written.
 */
function startPull(t, index, start, out, hooks = {}) {
  const f = t.files[index];
  const pull = {
    id: newId(),
    t,
    index,
    start,
    written: 0,
    length: f.size - start,
    out,
    onData: hooks.onData,
    onComplete: hooks.onComplete,
    onAbort: hooks.onAbort,
    pushReq: null,
    fails: 0,
    retryTimer: null,
    done: false,
  };
  pulls.set(pull.id, pull);
  t.pulls.add(pull);
  if (pull.length === 0) finishPull(pull);
  else requestPush(pull);
  return pull;
}

function requestPush(pull) {
  if (pull.done) return;
  clearTimeout(pull.retryTimer);
  // If the sender is offline this is a no-op; it is re-sent when the sender reconnects.
  sendTo(pull.t.from, 'pull', {
    pullId: pull.id,
    transferId: pull.t.id,
    index: pull.index,
    offset: pull.start + pull.written,
  });
}

function endPull(pull) {
  pull.done = true;
  clearTimeout(pull.retryTimer);
  pulls.delete(pull.id);
  pull.t.pulls.delete(pull);
}

function finishPull(pull) {
  if (pull.done) return;
  endPull(pull);
  sendTo(pull.t.from, 'file-done', { transferId: pull.t.id, index: pull.index });
  if (pull.onComplete) pull.onComplete();
}

function abortPull(pull) {
  if (pull.done) return;
  endPull(pull);
  if (pull.pushReq) pull.pushReq.destroy();
  sendTo(pull.t.from, 'pull-cancel', { pullId: pull.id });
  if (pull.onAbort) pull.onAbort();
  if (!pull.out.destroyed) pull.out.destroy();
}

function pushFailed(pull) {
  if (pull.done) return;
  if (++pull.fails > MAX_PUSH_FAILS) return abortPull(pull);
  clearTimeout(pull.retryTimer);
  pull.retryTimer = setTimeout(() => requestPush(pull), PUSH_RETRY_MS);
}

/** Pipe the sender's upload into the pull's output, honouring backpressure. */
function attachPush(pull, req, res) {
  if (pull.pushReq) pull.pushReq.destroy(); // a newer push replaces a stale one
  pull.pushReq = req;
  clearTimeout(pull.retryTimer);
  const out = pull.out;

  req.on('data', (chunk) => {
    if (pull.done || pull.pushReq !== req) return;
    const room = pull.length - pull.written;
    if (chunk.length > room) chunk = chunk.subarray(0, room);
    if (!chunk.length) return;
    pull.written += chunk.length;
    pull.fails = 0;
    if (pull.onData) pull.onData(chunk);
    if (!out.write(chunk)) {
      req.pause();
      out.once('drain', () => req.resume());
    }
  });
  req.on('end', () => {
    json(res, 200, { ok: true });
    if (pull.pushReq !== req) return;
    pull.pushReq = null;
    if (pull.written >= pull.length) finishPull(pull);
    else pushFailed(pull); // sender stopped short; ask again from where it got to
  });
  req.on('close', () => {
    if (pull.pushReq !== req) return; // finished normally, or replaced
    pull.pushReq = null;
    pushFailed(pull);
  });
}

// ---------------------------------------------------------------------------
// ZIP64 streaming (store, no compression) for "Download as zip"
// ---------------------------------------------------------------------------
// Sizes are known up front, so the full length is known before any data moves.
// CRCs are only known after each file streams through, so each entry ends with a
// data descriptor. ZIP64 fields are always used, so there is no 4 GB limit.

const CRC_TABLE = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();
function crc32(buf, prev = 0) {
  if (zlib.crc32) return zlib.crc32(buf, prev);
  let c = prev ^ -1;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
}

function dosDateTime(ms) {
  const d = new Date(ms);
  return {
    time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
    date: ((Math.max(d.getFullYear(), 1980) - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  };
}

/** Give duplicate paths a " (2)" suffix so entries don't overwrite each other when extracted. */
function uniquePaths(entries) {
  const seen = new Set();
  for (const e of entries) {
    let p = e.path;
    const slash = p.lastIndexOf('/');
    const dot = p.lastIndexOf('.');
    const cut = dot > slash + 1 ? dot : p.length;
    for (let n = 2; seen.has(p.toLowerCase()); n++) p = `${e.path.slice(0, cut)} (${n})${e.path.slice(cut)}`;
    seen.add(p.toLowerCase());
    e.zipName = Buffer.from(p, 'utf8');
  }
  return entries;
}

const LOCAL_LEN = 30 + 20;   // local header + ZIP64 extra
const DESC_LEN = 24;         // ZIP64 data descriptor
const CENTRAL_LEN = 46 + 28; // central header + ZIP64 extra
const END_LEN = 56 + 20 + 22;

function zipLength(entries) {
  let n = END_LEN;
  for (const e of entries) n += LOCAL_LEN + DESC_LEN + CENTRAL_LEN + 2 * e.zipName.length + e.size;
  return n;
}

/** Local file header, then the name, then the ZIP64 extra (in that order). */
function localHeader(e, time, date) {
  const h = Buffer.alloc(30);
  h.writeUInt32LE(0x04034b50, 0);
  h.writeUInt16LE(45, 4);          // version needed: ZIP64
  h.writeUInt16LE(0x0808, 6);      // flags: data descriptor + UTF-8 names
  h.writeUInt16LE(0, 8);           // method: store
  h.writeUInt16LE(time, 10);
  h.writeUInt16LE(date, 12);
  h.writeUInt32LE(0, 14);          // CRC follows in the data descriptor
  h.writeUInt32LE(0xffffffff, 18); // sizes are in the ZIP64 extra
  h.writeUInt32LE(0xffffffff, 22);
  h.writeUInt16LE(e.zipName.length, 26);
  h.writeUInt16LE(20, 28);
  const x = Buffer.alloc(20);
  x.writeUInt16LE(0x0001, 0);      // ZIP64 extra
  x.writeUInt16LE(16, 2);
  x.writeBigUInt64LE(BigInt(e.size), 4);
  x.writeBigUInt64LE(BigInt(e.size), 12);
  return Buffer.concat([h, e.zipName, x]);
}

function dataDescriptor(e) {
  const d = Buffer.alloc(DESC_LEN);
  d.writeUInt32LE(0x08074b50, 0);
  d.writeUInt32LE(e.crc, 4);
  d.writeBigUInt64LE(BigInt(e.size), 8);
  d.writeBigUInt64LE(BigInt(e.size), 16);
  return d;
}

function centralHeader(e, time, date) {
  const c = Buffer.alloc(46);
  c.writeUInt32LE(0x02014b50, 0);
  c.writeUInt16LE(45, 4);          // version made by
  c.writeUInt16LE(45, 6);          // version needed
  c.writeUInt16LE(0x0808, 8);
  c.writeUInt16LE(0, 10);
  c.writeUInt16LE(time, 12);
  c.writeUInt16LE(date, 14);
  c.writeUInt32LE(e.crc, 16);
  c.writeUInt32LE(0xffffffff, 20);
  c.writeUInt32LE(0xffffffff, 24);
  c.writeUInt16LE(e.zipName.length, 28);
  c.writeUInt16LE(28, 30);         // extra length
  c.writeUInt32LE(0xffffffff, 42); // offset is in the ZIP64 extra
  const x = Buffer.alloc(28);
  x.writeUInt16LE(0x0001, 0);
  x.writeUInt16LE(24, 2);
  x.writeBigUInt64LE(BigInt(e.size), 4);
  x.writeBigUInt64LE(BigInt(e.size), 12);
  x.writeBigUInt64LE(BigInt(e.offset), 20);
  return Buffer.concat([c, e.zipName, x]);
}

function endRecords(count, cdSize, cdOffset) {
  const z = Buffer.alloc(END_LEN);
  // ZIP64 end of central directory record
  z.writeUInt32LE(0x06064b50, 0);
  z.writeBigUInt64LE(44n, 4);
  z.writeUInt16LE(45, 12);
  z.writeUInt16LE(45, 14);
  z.writeBigUInt64LE(BigInt(count), 24);
  z.writeBigUInt64LE(BigInt(count), 32);
  z.writeBigUInt64LE(BigInt(cdSize), 40);
  z.writeBigUInt64LE(BigInt(cdOffset), 48);
  // ZIP64 locator
  z.writeUInt32LE(0x07064b50, 56);
  z.writeBigUInt64LE(BigInt(cdOffset + cdSize), 64);
  z.writeUInt32LE(1, 72);
  // classic end record, every field pointing at the ZIP64 one
  z.writeUInt32LE(0x06054b50, 76);
  z.writeUInt16LE(0xffff, 84);
  z.writeUInt16LE(0xffff, 86);
  z.writeUInt32LE(0xffffffff, 88);
  z.writeUInt32LE(0xffffffff, 92);
  return z;
}

async function streamZip(res, entries, zipName) {
  uniquePaths(entries);
  res.writeHead(200, {
    'Content-Type': 'application/zip',
    'Content-Length': zipLength(entries),
    'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(zipName)}`,
    'Cache-Control': 'no-store',
  });

  let aborted = false;
  let current = null;
  res.on('close', () => { aborted = true; if (current) abortPull(current); });

  let offset = 0;
  const central = [];
  for (const e of entries) {
    if (aborted) return;
    const { time, date } = dosDateTime(e.t.at);
    e.offset = offset;
    e.crc = 0;
    res.write(localHeader(e, time, date));
    const ok = await new Promise((resolve) => {
      current = startPull(e.t, e.index, 0, res, {
        onData: (chunk) => { e.crc = crc32(chunk, e.crc); },
        onComplete: () => resolve(true),
        onAbort: () => resolve(false),
      });
    });
    current = null;
    if (!ok || aborted) return;
    res.write(dataDescriptor(e));
    central.push(centralHeader(e, time, date));
    offset += LOCAL_LEN + e.zipName.length + e.size + DESC_LEN;
  }
  const cd = Buffer.concat(central);
  res.write(cd);
  res.end(endRecords(entries.length, cd.length, offset));
}

// ---------------------------------------------------------------------------
// Peers coming and going
// ---------------------------------------------------------------------------

function handleEvents(req, res, url) {
  const id = clean(url.searchParams.get('id'), 40);
  const secret = clean(url.searchParams.get('secret'), 80);
  if (!/^[0-9a-f]{16,40}$/.test(id) || !/^[0-9a-f]{32,80}$/.test(secret)) return text(res, 400, 'Bad identity');

  let peer = peers.get(id);
  if (peer && peer.secret !== secret) return text(res, 409, 'Identity in use'); // page makes a new one
  if (!peer && bySecret.has(secret)) return text(res, 409, 'Identity in use');
  if (!peer) {
    peer = { id, secret, res: null, listed: false };
    peers.set(id, peer);
    bySecret.set(secret, peer);
  }
  peer.name = clean(url.searchParams.get('name')) || peer.name || 'Device';
  peer.device = clean(url.searchParams.get('device'), 40) || peer.device || 'unknown';
  clearTimeout(peer.hideTimer);
  clearTimeout(peer.awayTimer);
  clearTimeout(peer.forgetTimer);

  // A reconnect (e.g. after a Wi-Fi blip) can arrive before the old connection is noticed dead.
  if (peer.res && peer.res !== res) peer.res.end();
  peer.res = res;
  peer.listed = true;

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-store',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.write('retry: 2000\n\n');

  const outgoing = [];
  const incoming = [];
  for (const t of transfers.values()) {
    if (t.from === id) outgoing.push(t.id);
    if (t.to === id) incoming.push(t.id);
  }
  sse(res, 'hello', {
    id,
    addresses: lanAddresses().map((a) => `http://${a}:${PORT}`),
    outgoing,
    incoming,
  });
  for (const t of transfers.values()) if (t.to === id) sse(res, 'offer', offerPayload(t));
  broadcastPeers();

  // Anything waiting on this device's files can continue now.
  for (const pull of pulls.values()) if (pull.t.from === id) requestPush(pull);

  const ping = setInterval(() => res.write(': ping\n\n'), 15000);
  req.on('close', () => {
    clearInterval(ping);
    if (peer.res !== res) return; // replaced by a newer connection
    peer.res = null;
    peer.hideTimer = setTimeout(() => { peer.listed = false; broadcastPeers(); }, PEER_GRACE_MS);
    peer.awayTimer = setTimeout(() => {
      for (const t of [...transfers.values()]) if (t.from === id) cancelTransfer(t, 'sender-left');
    }, SENDER_AWAY_MS);
    peer.forgetTimer = setTimeout(() => {
      for (const t of [...transfers.values()]) if (t.to === id) cancelTransfer(t, 'receiver-left');
      peers.delete(id);
      bySecret.delete(peer.secret);
    }, PEER_FORGET_MS);
  });
}

// ---------------------------------------------------------------------------
// HTTP routes
// ---------------------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const route = `${req.method} ${url.pathname}`;
  const k = url.searchParams.get('k');

  try {
    if (route === 'GET /' || route === 'GET /index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      fs.createReadStream(INDEX).pipe(res);
      return;
    }

    if (route === 'GET /events') return handleEvents(req, res, url);

    // Lets a page whose event stream was refused find out whether its identity clashes.
    if (route === 'GET /api/check') {
      const p = peers.get(url.searchParams.get('id'));
      const s = url.searchParams.get('secret');
      const clash = (p && p.secret !== s) || (!p && bySecret.has(s));
      return json(res, clash ? 409 : 200, { ok: !clash });
    }

    if (route === 'POST /api/rename') {
      const body = JSON.parse(await readBody(req, 4096));
      const p = auth(body.k);
      if (!p) return json(res, 403, { error: 'Not connected' });
      p.name = clean(body.name) || p.name;
      broadcastPeers();
      return json(res, 200, { ok: true });
    }

    if (route === 'POST /api/text') {
      const body = JSON.parse(await readBody(req, MAX_TEXT + 4096));
      const sender = auth(body.k);
      if (!sender) return json(res, 403, { error: 'Not connected' });
      const target = peers.get(body.to);
      if (!target || !target.res) return json(res, 404, { error: 'That device is not online' });
      sse(target.res, 'text', { from: sender.id, fromName: sender.name, text: String(body.text).slice(0, MAX_TEXT), at: Date.now() });
      return json(res, 200, { ok: true });
    }

    // Sender announces files; nothing is uploaded until the receiver downloads.
    if (route === 'POST /api/offer') {
      const body = JSON.parse(await readBody(req, MAX_OFFER_JSON));
      const sender = auth(body.k);
      if (!sender) return json(res, 403, { error: 'Not connected' });
      const target = peers.get(body.to);
      if (!target || !target.listed) return json(res, 404, { error: 'That device is not online' });
      if (target === sender) return json(res, 400, { error: 'Cannot send to yourself' });
      const list = Array.isArray(body.files) ? body.files : [];
      if (!list.length) return json(res, 400, { error: 'No files' });
      if (list.length > MAX_FILES) return json(res, 400, { error: `Too many files (max ${MAX_FILES})` });
      const files = list.map((f) => ({
        path: safePath(f.path),
        size: Number.isSafeInteger(f.size) && f.size >= 0 ? f.size : NaN,
        type: clean(f.type, 100),
      }));
      if (files.some((f) => Number.isNaN(f.size))) return json(res, 400, { error: 'Bad file size' });
      const t = {
        id: newId(),
        from: sender.id,
        to: target.id,
        files,
        total: files.reduce((s, f) => s + f.size, 0),
        at: Date.now(),
        pulls: new Set(),
      };
      transfers.set(t.id, t);
      sendTo(target.id, 'offer', offerPayload(t));
      return json(res, 200, { ok: true, transferId: t.id });
    }

    // Either side can cancel (sender) or decline (receiver).
    if (route === 'POST /api/cancel') {
      const body = JSON.parse(await readBody(req, 64 * 1024));
      const p = auth(body.k);
      if (!p) return json(res, 403, { error: 'Not connected' });
      for (const tid of [].concat(body.transferId || [])) {
        const t = transfers.get(tid);
        if (t && (t.from === p.id || t.to === p.id)) cancelTransfer(t, t.from === p.id ? 'sender' : 'receiver');
      }
      return json(res, 200, { ok: true });
    }

    // Sender's page uploads bytes for a pull, starting at `offset`.
    if (req.method === 'POST' && url.pathname.startsWith('/api/push/')) {
      const sender = auth(k);
      const pull = pulls.get(url.pathname.slice('/api/push/'.length));
      if (!sender || !pull || pull.t.from !== sender.id) { req.resume(); return json(res, 410, { error: 'No longer needed' }); }
      if (Number(url.searchParams.get('offset')) !== pull.start + pull.written) {
        req.resume();
        return json(res, 409, { error: 'Stale offset' });
      }
      attachPush(pull, req, res);
      return;
    }

    // Receiver downloads one file. Supports Range so interrupted downloads can resume.
    if ((req.method === 'GET' || req.method === 'HEAD') && url.pathname.startsWith('/api/dl/')) {
      const [tid, idx] = url.pathname.slice('/api/dl/'.length).split('/');
      const recv = auth(k);
      const t = transfers.get(tid);
      const index = Number(idx);
      if (!recv || !t || t.to !== recv.id || !t.files[index]) return text(res, 404, 'File not found, or the sender cancelled it');
      const f = t.files[index];
      const etag = `"${t.id}-${index}"`;

      let start = 0;
      const range = req.headers.range;
      const ifRange = req.headers['if-range'];
      if (range && (!ifRange || ifRange === etag)) {
        const m = /^bytes=(\d+)-(\d*)$/.exec(range.trim());
        // Only open-ended ranges ("resume from N") are supported; anything else gets the whole file.
        if (m && (!m[2] || Number(m[2]) >= f.size - 1)) {
          start = Number(m[1]);
          if (start >= f.size && f.size > 0) {
            res.writeHead(416, { 'Content-Range': `bytes */${f.size}` });
            return res.end();
          }
        }
      }
      const headers = {
        'Content-Type': 'application/octet-stream',
        'Content-Length': f.size - start,
        'Accept-Ranges': 'bytes',
        ETag: etag,
        'Last-Modified': new Date(t.at).toUTCString(),
        'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(baseName(f.path))}`,
        'X-Content-Type-Options': 'nosniff',
        'Content-Security-Policy': "default-src 'none'",
      };
      if (start > 0) headers['Content-Range'] = `bytes ${start}-${f.size - 1}/${f.size}`;
      res.writeHead(start > 0 ? 206 : 200, headers);
      if (req.method === 'HEAD' || f.size - start === 0) return res.end();
      const pull = startPull(t, index, start, res, { onComplete: () => res.end() });
      res.on('close', () => abortPull(pull));
      return;
    }

    // Receiver downloads several offers (or one) as a single zip, folders preserved.
    if (route === 'GET /api/zip') {
      const recv = auth(k);
      const list = (url.searchParams.get('t') || '').split(',').map((id) => transfers.get(id))
        .filter((t) => t && recv && t.to === recv.id);
      if (!list.length) return text(res, 404, 'Files not found, or the sender cancelled them');
      const entries = [];
      for (const t of list) t.files.forEach((f, index) => entries.push({ t, index, path: f.path, size: f.size }));

      // Name the zip after a shared top folder if there is one, else by date.
      const tops = new Set(entries.map((e) => (e.path.includes('/') ? e.path.split('/')[0] : null)));
      const d = new Date();
      const p2 = (n) => String(n).padStart(2, '0');
      const name = tops.size === 1 && !tops.has(null)
        ? `${[...tops][0]}.zip`
        : `landrop-${d.getFullYear()}${p2(d.getMonth() + 1)}${p2(d.getDate())}-${p2(d.getHours())}${p2(d.getMinutes())}.zip`;
      await streamZip(res, entries, name);
      return;
    }

    // ---- Speed test (this device <-> host) ---------------------------------
    if (route === 'GET /api/speed/ping') {
      res.writeHead(204, { 'Cache-Control': 'no-store' });
      return res.end();
    }
    if (route === 'GET /api/speed/down') {
      const bytes = Math.min(Math.max(Number(url.searchParams.get('bytes')) || 0, 0), SPEED_MAX);
      res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Content-Length': bytes, 'Cache-Control': 'no-store' });
      let left = bytes;
      const pump = () => {
        while (left > 0 && !res.destroyed) {
          const chunk = left >= SPEED_CHUNK.length ? SPEED_CHUNK : SPEED_CHUNK.subarray(0, left);
          left -= chunk.length;
          if (!res.write(chunk)) return res.once('drain', pump);
        }
        res.end();
      };
      return pump();
    }
    if (route === 'POST /api/speed/up') {
      let bytes = 0;
      req.on('data', (c) => { bytes += c.length; });
      req.on('end', () => json(res, 200, { bytes }));
      return;
    }

    text(res, 404, 'Not found');
  } catch (err) {
    json(res, 400, { error: err.message });
  }
});

// No timeouts: a big transfer can take hours, and a pull may wait for the sender to come back.
server.requestTimeout = 0;
server.headersTimeout = 60 * 1000;

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  LAN Drop is running. Open one of these on each device on your network:\n');
  console.log(`    http://localhost:${PORT}   (this computer)`);
  for (const a of lanAddresses()) console.log(`    http://${a}:${PORT}`);
  console.log('\n  Press Ctrl+C to stop.\n');
});
