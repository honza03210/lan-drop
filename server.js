// LAN Drop — discover devices on your local network and send files/text between them.
// Zero dependencies: run with `node server.js` (Node 18+), then open the printed URL
// on every device connected to the same network.

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const FILE_TTL_MS = 30 * 60 * 1000; // uploaded files expire after 30 minutes
const MAX_TEXT = 1024 * 1024;       // 1 MB of text per message
const TMP_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'landrop-'));
const INDEX = path.join(__dirname, 'index.html');

/** id -> { id, name, device, res (SSE response), ip } */
const peers = new Map();
/** fileId -> { path, name, size, type, to, from, timer } */
const files = new Map();

const newId = () => crypto.randomBytes(8).toString('hex');

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
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function peerList() {
  return [...peers.values()].map(({ id, name, device }) => ({ id, name, device }));
}

function broadcastPeers() {
  const list = peerList();
  for (const p of peers.values()) sse(p.res, 'peers', list);
}

function json(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
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
  return String(s || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, max);
}

function safeFileName(name) {
  return clean(name, 200).replace(/[\\/:*?"<>|]/g, '_') || 'file';
}

function removeFile(id) {
  const f = files.get(id);
  if (!f) return;
  clearTimeout(f.timer);
  files.delete(id);
  fs.rm(f.path, { force: true }, () => {});
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const route = `${req.method} ${url.pathname}`;

  try {
    // ---- Static page ------------------------------------------------------
    if (route === 'GET /' || route === 'GET /index.html') {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      fs.createReadStream(INDEX).pipe(res);
      return;
    }

    // ---- Presence: Server-Sent Events --------------------------------------
    if (route === 'GET /events') {
      const id = newId();
      const peer = {
        id,
        name: clean(url.searchParams.get('name')) || 'Device',
        device: clean(url.searchParams.get('device'), 40) || 'unknown',
        ip: req.socket.remoteAddress,
        res,
      };
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-store',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      });
      res.write('retry: 2000\n\n');
      peers.set(id, peer);
      sse(res, 'hello', { id, addresses: lanAddresses().map((a) => `http://${a}:${PORT}`) });
      broadcastPeers();
      const ping = setInterval(() => res.write(': ping\n\n'), 15000);
      req.on('close', () => {
        clearInterval(ping);
        peers.delete(id);
        broadcastPeers();
        // drop files nobody can collect anymore
        for (const [fid, f] of files) if (f.to === id) removeFile(fid);
      });
      return;
    }

    // ---- Rename ------------------------------------------------------------
    if (route === 'POST /api/rename') {
      const { id, name } = JSON.parse(await readBody(req, 4096));
      const p = peers.get(id);
      if (!p) return json(res, 404, { error: 'Unknown peer' });
      p.name = clean(name) || p.name;
      broadcastPeers();
      return json(res, 200, { ok: true });
    }

    // ---- Send text ---------------------------------------------------------
    if (route === 'POST /api/text') {
      const { from, to, text } = JSON.parse(await readBody(req, MAX_TEXT + 4096));
      const sender = peers.get(from);
      const target = peers.get(to);
      if (!sender) return json(res, 403, { error: 'Unknown sender' });
      if (!target) return json(res, 404, { error: 'That device is no longer online' });
      sse(target.res, 'text', { from: sender.id, fromName: sender.name, text: String(text).slice(0, MAX_TEXT), at: Date.now() });
      return json(res, 200, { ok: true });
    }

    // ---- Upload a file (raw body stream) -----------------------------------
    if (route === 'POST /api/file') {
      const sender = peers.get(url.searchParams.get('from'));
      const target = peers.get(url.searchParams.get('to'));
      if (!sender) return json(res, 403, { error: 'Unknown sender' });
      if (!target) return json(res, 404, { error: 'That device is no longer online' });

      const fileId = newId();
      const filePath = path.join(TMP_DIR, fileId);
      const out = fs.createWriteStream(filePath);
      let size = 0;
      let failed = false;
      req.on('data', (c) => { size += c.length; });
      req.on('aborted', () => { failed = true; out.destroy(); fs.rm(filePath, { force: true }, () => {}); });
      req.pipe(out);
      out.on('error', () => { failed = true; json(res, 500, { error: 'Could not store file' }); });
      out.on('finish', () => {
        if (failed) return;
        const t = peers.get(target.id);
        if (!t) { fs.rm(filePath, { force: true }, () => {}); return json(res, 404, { error: 'Recipient went offline' }); }
        const entry = {
          path: filePath,
          name: safeFileName(url.searchParams.get('name')),
          type: clean(url.searchParams.get('type'), 100) || 'application/octet-stream',
          size,
          to: target.id,
          from: sender.id,
          timer: setTimeout(() => removeFile(fileId), FILE_TTL_MS),
        };
        files.set(fileId, entry);
        sse(t.res, 'file', { fileId, name: entry.name, size, type: entry.type, from: sender.id, fromName: sender.name, at: Date.now() });
        json(res, 200, { ok: true, fileId });
      });
      return;
    }

    // ---- Download a file ---------------------------------------------------
    if (req.method === 'GET' && url.pathname.startsWith('/api/file/')) {
      const fileId = url.pathname.slice('/api/file/'.length);
      const f = files.get(fileId);
      if (!f || url.searchParams.get('peer') !== f.to) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        return res.end('File not found or expired');
      }
      const inline = url.searchParams.has('inline');
      res.writeHead(200, {
        'Content-Type': f.type,
        'Content-Length': f.size,
        'Content-Disposition': `${inline ? 'inline' : 'attachment'}; filename*=UTF-8''${encodeURIComponent(f.name)}`,
        'Cache-Control': 'no-store',
        'X-Content-Type-Options': 'nosniff',
        'Content-Security-Policy': "default-src 'none'; img-src 'self'; media-src 'self'; style-src 'unsafe-inline'",
      });
      fs.createReadStream(f.path).pipe(res);
      return;
    }

    // ---- Dismiss a received file ------------------------------------------
    if (req.method === 'DELETE' && url.pathname.startsWith('/api/file/')) {
      const fileId = url.pathname.slice('/api/file/'.length);
      const f = files.get(fileId);
      if (f && url.searchParams.get('peer') === f.to) removeFile(fileId);
      return json(res, 200, { ok: true });
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
  } catch (err) {
    if (!res.headersSent) json(res, 400, { error: err.message });
  }
});

// No request timeout: big uploads over Wi-Fi can take a while.
server.requestTimeout = 0;

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n  LAN Drop is running. Open one of these on each device on your network:\n');
  console.log(`    http://localhost:${PORT}   (this computer)`);
  for (const a of lanAddresses()) console.log(`    http://${a}:${PORT}`);
  console.log('\n  Press Ctrl+C to stop.\n');
});

function shutdown() {
  fs.rmSync(TMP_DIR, { recursive: true, force: true });
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
