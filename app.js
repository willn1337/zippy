'use strict';

/* =======================================================================
   zippy — hamachi-style P2P file drop (single peer per room)
   -----------------------------------------------------------------------
   - a PeerJS instance has exactly ONE id, so multi-room = multiple peers:
       host   -> new Peer('p2pdrop_' + id)   (is the hub of that room)
       member -> new Peer()                  (temporary client peer)
   - wire protocol per DataConnection (strictly sequential):
       1) { type: 'meta', name, size }     JSON object
       2) raw ArrayBuffer chunks, 64 KB    (file.slice, never full file)
       3) { type: 'done' }                 JSON object
   - backpressure: pause when dataChannel.bufferedAmount > 1 MB, resume
     on the 'bufferedamountlow' event
   ======================================================================= */

// ------------------------------ config ---------------------------------
const NS       = 'p2pdrop_';        // peer-id namespace
const CHUNK    = 64 * 1024;         // 64 KB wire chunks
const BUF_HIGH = 1024 * 1024;       // pause sender above 1 MB buffered
const BUF_LOW  = 256 * 1024;        // drain() resumes below this
const RETRY_MS = 30000;             // member host-retry interval
const LS_KEY   = 'zippy.rooms';
const ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no 0/O/1/I/L
const PEER_OPTS = {
  debug: 1,
  config: { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
};

// ------------------------------ state ----------------------------------
const rooms = new Map();        // roomId -> room record
const roomFolders = new Map();  // roomId -> FileSystemDirectoryHandle

const roomsEl  = document.getElementById('rooms');
const compatEl = document.getElementById('compat');

// --------------------------- tiny helpers ------------------------------
function h(tag, text) {
  const el = document.createElement(tag);
  if (text !== undefined) el.textContent = text;
  return el;
}

function genId() {
  let id;
  do {
    id = '';
    for (let i = 0; i < 5; i++) {
      id += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
    }
  } while (rooms.has(id));
  return id;
}

function shortId(pid) {
  return String(pid || '').replace(/[^a-zA-Z0-9]/g, '').slice(-4) || '????';
}

function fmtBytes(n) {
  if (!isFinite(n)) return '?';
  if (n >= 1048576) return (n / 1048576).toFixed(1) + ' MB';
  if (n >= 1024) return (n / 1024).toFixed(1) + ' KB';
  return Math.floor(n) + ' B';
}

function fmtSpeed(bps) {
  const mb = bps / 1048576;
  if (mb >= 1) return mb.toFixed(1) + ' MB/s';
  return (bps / 1024).toFixed(0) + ' KB/s';
}

function safeSend(conn, data) {
  try { conn.send(data); return true; } catch { return false; }
}

// --------------------------- persistence -------------------------------
function saveRooms() {
  const list = [...rooms.values()].map((r) => ({ id: r.id, role: r.role }));
  try { localStorage.setItem(LS_KEY, JSON.stringify(list)); } catch {}
}

function loadRooms() {
  let list = [];
  try { list = JSON.parse(localStorage.getItem(LS_KEY) || '[]'); } catch {}
  for (const r of list) {
    if (!r || !/^[A-Z0-9]{5}$/.test(r.id || '')) continue;
    if (rooms.has(r.id)) continue;
    if (r.role === 'host') createHost(r.id, null, true);
    else createMember(r.id, null, true);
  }
}

// --------------------------- room plumbing -----------------------------
function baseRoom(id, role, restored) {
  const el = h('div');
  el.className = 'room-box';
  const room = {
    id, role, restored: !!restored,
    peer: null,
    conns: new Map(),       // host: peerId -> DataConnection
    conn: null,             // member: DataConnection to host
    labels: new Map(),      // host: peerId -> display label
    members: [],            // member list for display
    recvStates: new Map(),  // conn -> incoming transfer state
    xfers: new Map(),       // display key -> progress line
    xferEls: new Map(),     // display key -> <div>
    sendQueue: [],
    sendBusy: false,
    retryTimer: 0,
    reconnectPending: false,
    spawnTries: 0,
    status: 'Starting...',
    statusEl: null,
    progEl: null,
    destroyed: false,
  };
  room.el = el;
  rooms.set(id, room);
  roomsEl.appendChild(el);
  return room;
}

function setStatus(room, text) {
  room.status = text;
  if (room.statusEl && room.statusEl.isConnected) {
    room.statusEl.textContent = 'status: ' + text;
  }
}

function openConns(room) {
  if (room.role === 'host') return [...room.conns.values()].filter((c) => c.open);
  return room.conn && room.conn.open ? [room.conn] : [];
}

function memberDisplay(room) {
  const mine = room.peer ? 'm-' + shortId(room.peer.id) : '';
  return room.members.map((m) => {
    if (m === 'host') return room.role === 'host' ? 'host (you)' : 'host';
    if (m === mine) return m + ' (you)';
    return m;
  });
}

function renderRoom(room) {
  const el = room.el;
  el.textContent = '';

  // line 1: room code (blurred, click to copy) + role + remove
  const top = h('div');
  top.appendChild(document.createTextNode('room '));
  const code = h('span', room.id);
  code.className = 'hidden-id';
  code.title = 'hover to reveal — click to copy';
  code.addEventListener('click', () => copyId(room));
  top.appendChild(code);
  top.appendChild(document.createTextNode('   '));
  top.appendChild(h('strong', room.role === 'host' ? '[HOST]' : '[MEMBER]'));
  top.appendChild(document.createTextNode('   '));
  const rm = h('button', 'remove');
  rm.addEventListener('click', () => destroyRoom(room));
  top.appendChild(rm);
  el.appendChild(top);

  // line 2: output folder
  const dir = roomFolders.get(room.id);
  const fl = h('div');
  if (!window.showDirectoryPicker) {
    fl.textContent = 'folder: n/a (browser will download incoming files)';
  } else {
    fl.appendChild(document.createTextNode('folder: ' + (dir ? dir.name : 'none selected') + ' '));
    const cf = h('button', 'change folder');
    cf.addEventListener('click', () => changeFolder(room));
    fl.appendChild(cf);
  }
  el.appendChild(fl);

  // line 3: members
  const names = memberDisplay(room);
  el.appendChild(h('div', 'members (' + names.length + '): ' + (names.join(', ') || '—')));

  // line 4: status
  room.statusEl = h('div', 'status: ' + room.status);
  el.appendChild(room.statusEl);

  // dropzone
  const peers = openConns(room).length;
  const dz = h('div', peers
    ? 'drop file here to send to ' + peers + ' peer' + (peers > 1 ? 's' : '') + ' (or click)'
    : 'drop file here (nobody connected yet)');
  dz.className = 'drop-zone';
  dz.addEventListener('dragover', (e) => e.preventDefault());
  dz.addEventListener('drop', (e) => {
    e.preventDefault();
    if (e.dataTransfer) for (const f of e.dataTransfer.files) queueSend(room, f);
  });
  const fi = document.createElement('input');
  fi.type = 'file';
  fi.multiple = true;
  fi.hidden = true;
  fi.addEventListener('change', () => {
    for (const f of fi.files) queueSend(room, f);
    fi.value = '';
  });
  dz.addEventListener('click', () => fi.click());
  el.appendChild(dz);
  el.appendChild(fi);

  // progress lines (kept fresh by the ticker)
  room.progEl = h('div');
  room.xferEls.clear();
  el.appendChild(room.progEl);
}

// ------------------------------- host ----------------------------------
function createHost(id, folder, restored) {
  const room = baseRoom(id, 'host', restored);
  if (folder) roomFolders.set(id, folder);
  room.status = 'registering on signaling server...';
  spawnHostPeer(room);
  renderRoom(room);
  saveRooms();
}

function spawnHostPeer(room) {
  const peer = new Peer(NS + room.id, PEER_OPTS);
  room.peer = peer;
  peer.on('open', () => {
    setStatus(room, 'host online — share the room code');
  });
  peer.on('connection', (conn) => attachHostConn(room, conn));
  peer.on('disconnected', () => scheduleReconnect(room));
  peer.on('error', (err) => hostError(room, err));
  renderRoom(room);
}

function hostError(room, err) {
  if (err.type === 'unavailable-id') {
    if (room.spawnTries < 10) {
      room.spawnTries++;
      if (room.restored) {
        // the old registration may linger on the cloud server for a bit
        setStatus(room, 'code still registered from last session — retrying...');
        setTimeout(() => {
          if (room.destroyed) return;
          try { room.peer.destroy(); } catch {}
          spawnHostPeer(room);
        }, 5000);
      } else {
        rekeyHost(room); // fresh host: just pick another code
      }
      return;
    }
    setStatus(room, 'code already in use — remove this room and host again');
    return;
  }
  if (err.type === 'peer-unavailable') return; // stale offer, ignore
  if (err.type === 'browser-incompatible') {
    setStatus(room, 'FATAL: browser lacks WebRTC support');
    return;
  }
  setStatus(room, 'signaling problem (' + err.type + ') — retrying...');
}

function rekeyHost(room) {
  const oldId = room.id;
  const folder = roomFolders.get(oldId);
  rooms.delete(oldId);
  roomFolders.delete(oldId);
  room.id = genId();
  if (folder) roomFolders.set(room.id, folder);
  rooms.set(room.id, room);
  try { room.peer.destroy(); } catch {}
  spawnHostPeer(room);
  renderRoom(room);
  saveRooms();
}

function attachHostConn(room, conn) {
  room.conns.set(conn.peer, conn);
  conn.on('open', () => {
    prepChannel(conn);
    room.labels.set(conn.peer, 'm-' + shortId(conn.peer));
    refreshMembers(room);
  });
  conn.on('data', (d) => onData(room, conn, d));
  conn.on('close', () => {
    finishReceive(room, conn, true);
    room.conns.delete(conn.peer);
    room.labels.delete(conn.peer);
    refreshMembers(room);
  });
  conn.on('error', () => { /* 'close' follows */ });
}

function refreshMembers(room) {
  room.members = ['host', ...[...room.conns.values()]
    .filter((c) => c.open)
    .map((c) => room.labels.get(c.peer) || 'm-' + shortId(c.peer))];
  renderRoom(room);
  const msg = { type: 'members', list: room.members };
  for (const c of room.conns.values()) if (c.open) safeSend(c, msg);
}

// ------------------------------ member ---------------------------------
function createMember(id, folder, restored) {
  const room = baseRoom(id, 'member', restored);
  if (folder) roomFolders.set(id, folder);
  room.status = 'connecting to signaling server...';
  const peer = new Peer(PEER_OPTS);
  room.peer = peer;
  peer.on('open', () => {
    if (!(room.conn && room.conn.open)) connectHost(room);
  });
  peer.on('disconnected', () => scheduleReconnect(room));
  peer.on('error', (err) => {
    if (err.type === 'peer-unavailable') {
      setStatus(room, 'Host offline, retrying...');
      return;
    }
    if (err.type === 'browser-incompatible') {
      setStatus(room, 'FATAL: browser lacks WebRTC support');
      return;
    }
    setStatus(room, 'signaling problem (' + err.type + ') — retrying...');
  });
  // persistent retry: if the host is not online, keep trying every 30 s
  room.retryTimer = setInterval(() => {
    if (room.destroyed || (room.conn && room.conn.open)) return;
    if (room.peer.disconnected) { try { room.peer.reconnect(); } catch {} return; }
    if (room.peer.open) connectHost(room);
  }, RETRY_MS);
  renderRoom(room);
  saveRooms();
}

function connectHost(room) {
  if (room.destroyed) return;
  if (room.conn) { try { room.conn.close(); } catch {} room.conn = null; }
  setStatus(room, 'connecting to host...');
  const conn = room.peer.connect(NS + room.id, { reliable: true });
  room.conn = conn;
  conn.on('open', () => {
    if (room.conn !== conn) { try { conn.close(); } catch {} return; } // stale
    prepChannel(conn);
    setStatus(room, 'connected — drop files below');
  });
  conn.on('data', (d) => onData(room, conn, d));
  conn.on('close', () => {
    if (room.conn !== conn) return; // stale
    finishReceive(room, conn, true);
    room.conn = null;
    room.members = [];
    setStatus(room, 'Host offline, retrying...');
    renderRoom(room);
  });
  conn.on('error', () => { /* peer-unavailable arrives on the peer */ });
}

// --------------------------- connection utils --------------------------
function scheduleReconnect(room) {
  if (room.destroyed || room.reconnectPending) return;
  room.reconnectPending = true;
  setTimeout(() => {
    room.reconnectPending = false;
    if (room.destroyed) return;
    try { room.peer.reconnect(); } catch {}
    setStatus(room, 'signaling reconnecting...');
  }, 3000);
}

function prepChannel(conn) {
  const dc = conn.dataChannel;
  if (dc) dc.bufferedAmountLowThreshold = BUF_LOW;
}

// --------------------------- file sending ------------------------------
function queueSend(room, file) {
  room.sendQueue.push(file);
  pumpSends(room);
}

async function pumpSends(room) {
  if (room.sendBusy || room.destroyed) return;
  room.sendBusy = true;
  try {
    while (room.sendQueue.length && !room.destroyed) {
      await sendToAll(room, room.sendQueue.shift());
    }
  } finally {
    room.sendBusy = false;
  }
}

async function sendToAll(room, file) {
  const targets = openConns(room);
  if (!targets.length) {
    setStatus(room, 'nobody connected — "' + file.name + '" not sent');
    return;
  }
  for (let i = 0; i < targets.length; i++) {
    if (room.destroyed) return;
    await sendToConn(room, targets[i], file, i + 1, targets.length);
  }
  if (!room.destroyed) {
    setStatus(room, 'sent "' + file.name + '" to ' + targets.length + ' peer(s)');
  }
}

async function sendToConn(room, conn, file, idx, total) {
  const xf = addXfer(room, '↑', file.name, file.size);
  xf.status = 'sending → ' + labelOf(room, conn) + ' (' + idx + '/' + total + ')';
  if (!safeSend(conn, { type: 'meta', name: file.name, size: file.size })) {
    xf.status = 'failed (connection closed)';
    xf.doneAt = performance.now();
    return;
  }
  let offset = 0;
  while (offset < file.size) {
    if (room.destroyed || !conn.open) {
      xf.status = 'aborted';
      xf.doneAt = performance.now();
      return;
    }
    const buf = await file.slice(offset, offset + CHUNK).arrayBuffer(); // never whole file in RAM
    await drain(conn); // backpressure: wait until < 1 MB buffered
    safeSend(conn, buf);
    offset += buf.byteLength;
    xf.bytes = offset;
  }
  safeSend(conn, { type: 'done' });
  xf.status = 'done (' + fmtBytes(file.size) + ')';
  xf.doneAt = performance.now();
}

// pause while the data channel holds > BUF_HIGH bytes; resume on
// 'bufferedamountlow' (threshold BUF_LOW) or if the channel dies
function drain(conn) {
  const dc = conn.dataChannel;
  if (!dc || dc.readyState !== 'open' || dc.bufferedAmount < BUF_HIGH) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      dc.removeEventListener('bufferedamountlow', finish);
      dc.removeEventListener('close', finish);
      resolve();
    };
    dc.addEventListener('bufferedamountlow', finish);
    dc.addEventListener('close', finish);
  });
}

function labelOf(room, conn) {
  if (room.role === 'member') return 'host';
  return room.labels.get(conn.peer) || 'm-' + shortId(conn.peer);
}

// -------------------------- file receiving -----------------------------
function isBinary(d) {
  return d instanceof ArrayBuffer ||
         ArrayBuffer.isView(d) ||
         (typeof Blob !== 'undefined' && d instanceof Blob);
}

async function onData(room, conn, data) {
  try {
    if (isBinary(data)) {
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = data;
      } else if (ArrayBuffer.isView(data)) {
        buf = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
      } else {
        buf = await data.arrayBuffer(); // Blob (peerjs chunk reassembly)
      }
      await receiveChunk(room, conn, buf);
      return;
    }
    if (data && typeof data === 'object') {
      if (data.type === 'members') { room.members = data.list || []; renderRoom(room); return; }
      if (data.type === 'meta')    { await startReceive(room, conn, data); return; }
      if (data.type === 'done')    { await finishReceive(room, conn, false); return; }
    }
  } catch (err) {
    setStatus(room, 'transfer error: ' + (err && err.message ? err.message : err));
  }
}

async function startReceive(room, conn, meta) {
  const name = String(meta.name || 'unnamed').split(/[\\/]/).pop() || 'unnamed';
  const size = Number(meta.size) || 0;
  const st = { name, size, bytes: 0, writable: null, chunks: null, open: true };
  st.xf = addXfer(room, '↓', name, size);
  room.recvStates.set(conn, st);

  const dir = roomFolders.get(room.id);

  // no File System Access API → buffer chunks, then a normal browser download
  if (!window.showDirectoryPicker) {
    st.chunks = [];
    st.xf.status = 'receiving (download mode)';
    setStatus(room, 'no direct-to-disk API — "' + name + '" will download when done');
    return;
  }

  if (!dir) {
    st.open = false;
    st.xf.status = 'REFUSED — no output folder selected';
    st.xf.doneAt = performance.now();
    room.recvStates.delete(conn);
    setStatus(room, 'no folder selected — set one with "change folder" and ask for a resend');
    return;
  }

  try {
    const fh = await dir.getFileHandle(name, { create: true });
    st.writable = await fh.createWritable(); // streams straight to disk
    st.xf.status = 'receiving';
  } catch (err) {
    st.open = false;
    st.xf.status = 'REFUSED — cannot write file';
    st.xf.doneAt = performance.now();
    room.recvStates.delete(conn);
    setStatus(room, 'cannot write "' + name + '": ' + (err && err.message ? err.message : err));
  }
}

async function receiveChunk(room, conn, buf) {
  const st = room.recvStates.get(conn);
  if (!st || !st.open) return; // stray chunk after an error/abort
  st.bytes += buf.byteLength;
  st.xf.bytes = st.bytes;
  try {
    if (st.writable) await st.writable.write(buf);
    else if (st.chunks) st.chunks.push(buf);
  } catch (err) {
    st.open = false;
    st.xf.status = 'write error';
    st.xf.doneAt = performance.now();
    room.recvStates.delete(conn);
    setStatus(room, 'write failed on "' + st.name + '": ' + (err && err.message ? err.message : err));
  }
}

async function finishReceive(room, conn, aborted) {
  const st = room.recvStates.get(conn);
  if (!st) return;
  room.recvStates.delete(conn);
  st.open = false;
  if (st.writable) {
    try { await st.writable.close(); } catch {
      st.xf.status = 'close error';
      st.xf.doneAt = performance.now();
      return;
    }
    st.writable = null;
  } else if (st.chunks) {
    downloadBlob(new Blob(st.chunks), st.name); // fallback path
    st.chunks = null;
  }
  st.xf.status = aborted ? 'aborted (sender left)' : 'done (' + fmtBytes(st.bytes) + ')';
  st.xf.doneAt = performance.now();
  setStatus(room, 'received "' + st.name + '" (' + fmtBytes(st.bytes) + (aborted ? ', incomplete' : '') + ')');
}

function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = h('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}

// ------------------------- progress display ----------------------------
function addXfer(room, dir, name, size) {
  const key = dir + name;
  const old = room.xferEls.get(key);
  if (old) { old.remove(); room.xferEls.delete(key); }
  room.xfers.delete(key);
  const xf = {
    dir, name, size,
    bytes: 0, speed: 0,
    status: '',
    tRef: performance.now(), bRef: 0, doneAt: 0,
  };
  room.xfers.set(key, xf);
  return xf;
}

function lineFor(xf) {
  const pct = xf.size ? Math.min(100, Math.floor((xf.bytes / xf.size) * 100)) : 0;
  const f = Math.round(pct / 10);
  const bar = '[' + '#'.repeat(f) + '-'.repeat(10 - f) + ']';
  const total = xf.size ? ' / ' + fmtBytes(xf.size) : '';
  const spd = xf.speed > 1 ? ' ' + fmtSpeed(xf.speed) : '';
  return xf.dir + ' ' + xf.name + ' ' + bar + ' ' + pct + '% — ' +
         fmtBytes(xf.bytes) + total + spd + ' — ' + xf.status;
}

setInterval(() => {
  const now = performance.now();
  for (const room of rooms.values()) {
    if (room.destroyed) continue;
    // rolling speed
    for (const xf of room.xfers.values()) {
      if (xf.doneAt) continue;
      const dt = (now - xf.tRef) / 1000;
      if (dt >= 0.3) {
        xf.speed = (xf.bytes - xf.bRef) / dt;
        xf.tRef = now;
        xf.bRef = xf.bytes;
      }
    }
    // prune finished lines
    for (const [key, xf] of room.xfers) {
      if (xf.doneAt && now - xf.doneAt > 8000) {
        room.xfers.delete(key);
        const div = room.xferEls.get(key);
        if (div) { div.remove(); room.xferEls.delete(key); }
      }
    }
    // redraw
    if (room.progEl && room.progEl.isConnected) {
      for (const [key, xf] of room.xfers) {
        let div = room.xferEls.get(key);
        if (!div || !div.isConnected) {
          div = h('div');
          room.progEl.appendChild(div);
          room.xferEls.set(key, div);
        }
        div.textContent = lineFor(xf);
      }
    }
  }
}, 300);

// ----------------------------- room ops --------------------------------
async function changeFolder(room) {
  if (!window.showDirectoryPicker) return;
  let handle;
  try {
    handle = await window.showDirectoryPicker({ id: 'zippy-out', mode: 'readwrite' });
  } catch {
    return; // user cancelled
  }
  roomFolders.set(room.id, handle);
  setStatus(room, 'output folder set: ' + handle.name);
  renderRoom(room);
}

async function copyId(room) {
  const id = room.id;
  try {
    await navigator.clipboard.writeText(id);
    setStatus(room, 'copied ' + id + ' to clipboard');
  } catch {
    const ta = h('textarea');
    ta.value = id;
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    setStatus(room, ok ? 'copied ' + id + ' to clipboard' : 'copy failed — room code: ' + id);
  }
}

function destroyRoom(room) {
  if (room.destroyed) return;
  room.destroyed = true;
  clearInterval(room.retryTimer);
  for (const c of room.conns.values()) { try { c.close(); } catch {} }
  if (room.conn) { try { room.conn.close(); } catch {} }
  if (room.peer) { try { room.peer.destroy(); } catch {} }
  rooms.delete(room.id);
  roomFolders.delete(room.id);
  saveRooms();
  room.el.remove();
}

// ------------------------------ wiring ---------------------------------
async function pickFolder() {
  if (!window.showDirectoryPicker) return null;   // unsupported → download mode
  try {
    return await window.showDirectoryPicker({ id: 'zippy-out', mode: 'readwrite' });
  } catch {
    return undefined;                             // user cancelled → abort flow
  }
}

document.getElementById('btn-host').addEventListener('click', async () => {
  if (typeof Peer === 'undefined') { alert('PeerJS failed to load — check your connection.'); return; }
  // the directory picker MUST run inside this direct click handler
  // (File System Access API user-gesture requirement)
  const folder = await pickFolder();
  if (folder === undefined) return;
  createHost(genId(), folder, false);
});

document.getElementById('btn-join').addEventListener('click', async () => {
  if (typeof Peer === 'undefined') { alert('PeerJS failed to load — check your connection.'); return; }
  const id = (prompt('Room code (5 characters):') || '').trim().toUpperCase();
  if (!id) return;
  if (!/^[A-Z0-9]{5}$/.test(id)) { alert('Room code must be exactly 5 letters/numbers.'); return; }
  if (rooms.has(id)) { alert('You are already in room ' + id + '.'); return; }
  const folder = await pickFolder();
  if (folder === undefined) return;
  createMember(id, folder, false);
});

// restore persisted rooms on load (folder handles can't be persisted —
// set them again with "change folder"; both buttons call the picker
// directly inside their click handlers)
loadRooms();

if (!window.showDirectoryPicker) {
  compatEl.textContent =
    'note: this browser lacks the File System Access API (Chrome/Edge have it). ' +
    'Incoming files will be saved through normal browser downloads instead.';
}
