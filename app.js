'use strict';

// ------------------------------ config ---------------------------------
const NS = 'p2pdrop_';        // peer-id namespace
const CHUNK = 64 * 1024;         // 64 KB wire chunks
const BUF_HIGH = 1024 * 1024;       // pause sender above 1 MB buffered
const BUF_LOW = 256 * 1024;        // drain() resumes below this
const RETRY_MS = 30000;             // member host-retry interval
const LS_KEY = 'zippy.rooms';
const ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no 0/O/1/I/L
const PEER_OPTS = {
  debug: 1,
  config: { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
};
// PeerJS treats these signaling errors as fatal (peer destroyed) -
// the only cure is building a fresh peer instance
const FATAL_SIGNALING = ['network', 'socket-error', 'socket-closed-error', 'server-error', 'ssl-unavailable'];
// status-dot colors: ok / working on it / broken
const LEVEL_COLORS = { ok: '#2da44e', warn: '#eab308', err: '#cf222e' };
const IS_FIREFOX = /firefox|fxios/i.test(navigator.userAgent);

// ------------------------------ state ----------------------------------
const rooms = new Map();        // roomId -> room record
const roomFolders = new Map();  // roomId -> FileSystemDirectoryHandle

const roomsEl = document.getElementById('rooms');
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
  try { localStorage.setItem(LS_KEY, JSON.stringify(list)); } catch { }
}

function loadRooms() {
  let list = [];
  try { list = JSON.parse(localStorage.getItem(LS_KEY) || '[]'); } catch { }
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
    memberCount: 1,         // online in this room (including you)
    recvStates: new Map(),  // conn -> incoming file state
    recvBatches: new Map(), // conn -> current incoming folder batch
    recvRoots: new Set(),   // folder roots already used on disk this session
    recvChains: new Map(),  // conn -> promise: strict in-order data handling
    peerList: new Set(),    // member: peer ids of everyone else in the room
    xfers: new Map(),       // transfer id -> transfer record
    xferEls: new Map(),     // transfer id -> .transfer-item <div>
    sendQueues: new Map(),  // conn -> [{ batch, xf }] (serialized per conn)
    sending: new Set(),     // conns with an active send pump
    sendingByConn: new Map(), // conn -> active outgoing xf
    retryTimer: 0,
    meshTimer: 0,
    spawnTries: 0,
    status: 'Starting...',
    level: 'warn',          // 'ok' | 'warn' | 'err' → status-dot color
    destroyed: false,
  };
  room.el = el;
  room.ui = buildRoomUI(room);
  updateHeader(room);
  updateFolderLabel(room);
  rooms.set(id, room);
  roomsEl.appendChild(el);
  return room;
}

function setConn(room, text, level) {
  // CONNECTION status only - drives the colored dot + its tooltip
  room.status = text;
  if (level) room.level = level;
  room.ui.dot.style.background = LEVEL_COLORS[room.level];
  room.ui.dot.title = room.status;
}

// healthy/idle state: color the dot, clear the tooltip text
function setLevel(room, level) {
  room.status = '';
  room.level = level;
  room.ui.dot.style.background = LEVEL_COLORS[level];
  room.ui.dot.title = '';
}

// the one status line for everything that is NOT connection state:
// transfer results, folder problems, warnings. Stays until replaced.
function setStatus(room, text, level) {
  room.ui.status.textContent = text || '';
  room.ui.status.className = 'room-status' + (text && level ? ' ' + level : '');
}

// header state: dot color/tooltip + online count. The share button manages
// its own label in copyId(); it never needs a re-render.
function updateHeader(room) {
  room.ui.dot.style.background = LEVEL_COLORS[room.level];
  room.ui.dot.title = room.status;
  room.ui.memberCount.textContent = room.memberCount + ' online';
}

// footer: the chosen output folder
function updateFolderLabel(room) {
  if (!room.ui.folderLabel) return;
  const dir = roomFolders.get(room.id);
  room.ui.folderLabel.textContent = dir ? dir.name : 'choose output folder';
}

// Build the whole card once: header / status line / dropzone / transfers /
// footer. Everything later mutates room.ui.* directly - no innerHTML wipes.
function buildRoomUI(room) {
  const ui = {};
  const el = room.el;

  // header: share ... dot + count + close
  const header = h('div');
  header.className = 'room-header';
  const left = h('span');
  left.className = 'header-left';
  ui.shareBtn = h('button', 'share link');
  ui.shareBtn.className = 'share-btn';
  ui.shareBtn.title = 'copies an invite link that auto-joins this room';
  ui.shareBtn.addEventListener('click', () => copyId(room));
  left.appendChild(ui.shareBtn);
  header.appendChild(left);
  const right = h('span');
  right.className = 'header-right';
  // status dot sits immediately left of the online count
  ui.dot = h('span');
  ui.dot.style.display = 'inline-block';
  ui.dot.style.width = '10px';
  ui.dot.style.height = '10px';
  ui.dot.style.borderRadius = '50%';
  ui.dot.style.verticalAlign = 'middle';
  right.appendChild(ui.dot);
  ui.memberCount = h('span');
  ui.memberCount.className = 'member-count';
  right.appendChild(ui.memberCount);
  const x = h('button', '×');
  x.className = 'close-btn';
  x.title = room.role === 'host' ? 'delete room' : 'leave room';
  x.addEventListener('click', () => {
    if (room.role === 'host' &&
      !confirm('Delete room ' + room.id + '? Connected members will be kicked.')) return;
    destroyRoom(room);
  });
  right.appendChild(x);
  header.appendChild(right);
  el.appendChild(header);

  // single status line (transfer results, folder problems - never the
  // connection state, that is the dot's job)
  ui.status = h('div');
  ui.status.className = 'room-status';
  el.appendChild(ui.status);

  // dropzone: files and folders (drop either, or click either line)
  const dz = h('div');
  dz.className = 'drop-zone';
  const dzMain = h('span', 'upload files');
  dzMain.className = 'drop-main';
  const dzSub = h('span', 'or pick a folder');
  dzSub.className = 'drop-sub';
  dz.appendChild(dzMain);
  dz.appendChild(dzSub);
  dz.addEventListener('dragover', (e) => e.preventDefault());
  dz.addEventListener('drop', (e) => {
    e.preventDefault();
    if (e.dataTransfer) queueDrop(room, e.dataTransfer);
  });
  const fi = document.createElement('input');
  fi.type = 'file';
  fi.multiple = true;
  fi.hidden = true;
  fi.addEventListener('change', () => {
    for (const f of fi.files) queueSend(room, f);
    fi.value = '';
  });
  const di = document.createElement('input');
  di.type = 'file';
  di.multiple = true;
  di.hidden = true;
  if ('webkitdirectory' in di) di.webkitdirectory = true;
  else dzSub.style.display = 'none'; // no folder picker in this browser
  di.addEventListener('change', () => {
    sendPickedFolder(room, di.files);
    di.value = '';
  });
  dz.addEventListener('click', () => fi.click());
  dzSub.addEventListener('click', (e) => { e.stopPropagation(); di.click(); });
  el.appendChild(dz);
  el.appendChild(fi);
  el.appendChild(di);

  // transfers (Drive-style list): below the dropzone, above the folder link
  ui.transfersList = h('div');
  ui.transfersList.className = 'transfers-list';
  el.appendChild(ui.transfersList);

  // footer: folder config (click the link to pick a folder)
  const footer = h('div');
  footer.className = 'room-footer';
  if (window.showDirectoryPicker) {
    const link = h('span');
    link.className = 'folder-link';
    link.appendChild(h('span', '📁'));
    link.appendChild(document.createTextNode(' '));
    ui.folderLabel = h('span');
    ui.folderLabel.className = 'folder-text';
    link.appendChild(ui.folderLabel);
    link.addEventListener('click', () => changeFolder(room));
    footer.appendChild(link);
  } else {
    const note = h('span', '📁 browser downloads');
    note.title = 'no File System Access API - incoming files use browser downloads';
    footer.appendChild(note);
  }
  el.appendChild(footer);
  return ui;
}

function openConns(room) {
  const list = [];
  if (room.role === 'member' && room.conn && room.conn.open) list.push(room.conn);
  for (const c of room.conns.values()) if (c.open) list.push(c);
  return list;
}

// ------------------------------- host ----------------------------------
function createHost(id, folder, restored) {
  const room = baseRoom(id, 'host', restored);
  if (folder) roomFolders.set(id, folder);
  room.status = 'registering on signaling server...';
  spawnHostPeer(room);
  updateHeader(room);
  saveRooms();
}

function spawnHostPeer(room) {
  clearPeerConns(room);
  const peer = new Peer(NS + room.id, PEER_OPTS);
  room.peer = peer;
  peer.on('open', () => {
    setLevel(room, 'ok'); // host online
  });
  peer.on('connection', (conn) => attachHostConn(room, conn));
  peer.on('disconnected', () => onPeerDisconnected(room));
  peer.on('error', (err) => hostError(room, err));
}

function hostError(room, err) {
  if (err.type === 'unavailable-id') {
    if (room.spawnTries < 10) {
      room.spawnTries++;
      if (room.restored) {
        // the old registration may linger on the cloud server for a bit
        setConn(room, 'code still registered from last session - retrying...');
        setTimeout(() => {
          if (room.destroyed) return;
          try { room.peer.destroy(); } catch { }
          spawnHostPeer(room);
        }, 5000);
      } else {
        rekeyHost(room); // fresh host: just pick another code
      }
      return;
    }
    setConn(room, 'code already in use - remove this room and host again', 'err');
    return;
  }
  if (err.type === 'peer-unavailable') return; // stale offer, ignore
  if (err.type === 'browser-incompatible') {
    setConn(room, 'FATAL: browser lacks WebRTC support', 'err');
    return;
  }
  if (FATAL_SIGNALING.indexOf(err.type) !== -1) {
    rebuildPeer(room, 'signaling unreachable (' + err.type + ')');
    return;
  }
  setConn(room, 'signaling problem (' + err.type + ') - retrying...', 'warn');
}

function rekeyHost(room) {
  const oldId = room.id;
  const folder = roomFolders.get(oldId);
  rooms.delete(oldId);
  roomFolders.delete(oldId);
  room.id = genId();
  if (folder) roomFolders.set(room.id, folder);
  rooms.set(room.id, room);
  try { room.peer.destroy(); } catch { }
  spawnHostPeer(room);
  saveRooms();
}

function attachHostConn(room, conn) {
  room.conns.set(conn.peer, conn);
  conn.on('open', () => {
    prepChannel(conn);
    broadcastMembers(room);
  });
  conn.on('data', (d) => enqueueData(room, conn, d));
  conn.on('close', () => {
    cleanupConn(room, conn);
    // a reconnect may already have replaced this entry
    if (room.conns.get(conn.peer) === conn) room.conns.delete(conn.peer);
    broadcastMembers(room);
  });
  conn.on('error', () => { /* 'close' follows */ });
}

function broadcastMembers(room) {
  room.memberCount = 1 + openConns(room).length;
  updateHeader(room);
  // hand every member the peer ids of the others so they can connect directly
  const peers = [...room.conns.values()].filter((c) => c.open).map((c) => c.peer);
  const msg = { type: 'members', count: room.memberCount, peers };
  for (const c of room.conns.values()) if (c.open) safeSend(c, msg);
}

// --------------------------- member mesh -------------------------------
// The host owns membership; every member then dials the other members so
// files go straight from sender to receiver instead of relaying through the
// host. The lower peer id dials, the higher one accepts, so each pair has
// exactly one connection.
function attachPeerConn(room, conn) {
  if (room.destroyed) { try { conn.close(); } catch { } return; }
  const pid = conn.peer;
  if (!pid || pid === (room.peer && room.peer.id)) { try { conn.close(); } catch { } return; }
  if (pid === NS + room.id) { try { conn.close(); } catch { } return; } // already the host link
  const existing = room.conns.get(pid);
  if (existing) {
    if (existing.open) { try { conn.close(); } catch { } return; }
    room.conns.delete(pid);
    try { existing.close(); } catch { }
    cleanupConn(room, existing);
  }
  room.conns.set(pid, conn);
  conn._born = Date.now();
  conn.on('open', () => prepChannel(conn));
  conn.on('data', (d) => enqueueData(room, conn, d));
  conn.on('close', () => {
    if (room.conns.get(pid) === conn) room.conns.delete(pid);
    cleanupConn(room, conn);
  });
  conn.on('error', () => {
    if (room.conns.get(pid) === conn) room.conns.delete(pid);
    cleanupConn(room, conn);
  });
}

function connectPeer(room, pid) {
  if (room.destroyed || !room.peer || !room.peer.open || room.conns.has(pid)) return;
  let conn;
  try { conn = room.peer.connect(pid, { reliable: true }); } catch { return; }
  conn._born = Date.now();
  room.conns.set(pid, conn);
  conn.on('open', () => prepChannel(conn));
  conn.on('data', (d) => enqueueData(room, conn, d));
  conn.on('close', () => {
    if (room.conns.get(pid) === conn) room.conns.delete(pid);
    cleanupConn(room, conn);
  });
  conn.on('error', () => {
    if (room.conns.get(pid) === conn) room.conns.delete(pid);
    cleanupConn(room, conn);
  });
}

function syncPeerList(room, peers) {
  const self = room.peer && room.peer.id;
  room.peerList = new Set(peers.filter((p) => typeof p === 'string' && p && p !== self));
  for (const [pid, conn] of [...room.conns]) {
    if (!room.peerList.has(pid)) {
      room.conns.delete(pid);
      try { conn.close(); } catch { }
      cleanupConn(room, conn);
    }
  }
  meshTick(room);
}

function meshTick(room) {
  if (room.destroyed || room.role !== 'member' || !room.peer || !room.peer.open) return;
  const self = room.peer.id;
  if (!self) return;
  for (const pid of room.peerList) {
    const existing = room.conns.get(pid);
    if (existing) {
      if (existing.open) continue;
      if (Date.now() - (existing._born || 0) < 15000) continue; // still dialing
      room.conns.delete(pid);
      try { existing.close(); } catch { }
      cleanupConn(room, existing);
    }
    if (self < pid) connectPeer(room, pid);
  }
}

// a peer instance is per-room; rebuilding it invalidates every conn it owns
function clearPeerConns(room) {
  const all = [...room.conns.values()];
  if (room.conn) all.push(room.conn);
  room.conns.clear();
  room.conn = null;
  for (const c of all) {
    try { c.close(); } catch { } // handlers may skip on purpose: room.conn was nulled
    cleanupConn(room, c);
  }
}

// ------------------------------ member ---------------------------------
function createMember(id, folder, restored) {
  const room = baseRoom(id, 'member', restored);
  if (folder) roomFolders.set(id, folder);
  room.status = 'connecting to signaling server...';
  spawnMemberPeer(room);
  // the single retry timer: if the host is not online, keep trying every
  // 30 s; a destroyed peer (fatal signaling loss) is rebuilt here too
  room.retryTimer = setInterval(() => {
    if (room.destroyed || (room.conn && room.conn.open)) return;
    if (room.peer.destroyed) { rebuildPeer(room, 'connection lost'); return; }
    if (room.peer.open) connectHost(room);
  }, RETRY_MS);
  // keep the member-to-member mesh alive (retries failed dials)
  room.meshTimer = setInterval(() => meshTick(room), 5000);
  updateHeader(room);
  saveRooms();
}

function spawnMemberPeer(room) {
  clearPeerConns(room);
  const peer = new Peer(PEER_OPTS);
  room.peer = peer;
  peer.on('open', () => {
    if (!(room.conn && room.conn.open)) connectHost(room);
  });
  peer.on('connection', (conn) => attachPeerConn(room, conn));
  peer.on('disconnected', () => onPeerDisconnected(room));
  peer.on('error', (err) => {
    if (err.type === 'peer-unavailable') {
      setConn(room, 'Host offline, retrying...', 'warn');
      return;
    }
    if (err.type === 'browser-incompatible') {
      setConn(room, 'FATAL: browser lacks WebRTC support', 'err');
      return;
    }
    if (FATAL_SIGNALING.indexOf(err.type) !== -1) {
      // the Peer is destroyed; the 30 s retry timer rebuilds it
      setConn(room, 'signaling unreachable (' + err.type + ') - retrying...', 'warn');
      return;
    }
    setConn(room, 'signaling problem (' + err.type + ') - retrying...', 'warn');
  });
}

function connectHost(room) {
  if (room.destroyed) return;
  if (room.conn) { try { room.conn.close(); } catch { } room.conn = null; }
  setConn(room, 'connecting to host...', 'warn');
  const conn = room.peer.connect(NS + room.id, { reliable: true });
  room.conn = conn;
  conn.on('open', () => {
    if (room.conn !== conn) { try { conn.close(); } catch { } return; } // stale
    prepChannel(conn);
    setLevel(room, 'ok'); // connected - the dropzone says the rest
  });
  conn.on('data', (d) => enqueueData(room, conn, d));
  conn.on('close', () => {
    if (room.conn !== conn) return; // stale
    cleanupConn(room, conn);
    room.conn = null;
    room.memberCount = 1;
    setConn(room, 'Host offline, retrying...', 'warn');
    updateHeader(room);
  });
  conn.on('error', () => { /* peer-unavailable arrives on the peer */ });
}

// --------------------------- connection utils --------------------------
// PeerJS dropped the websocket without destroying the peer: reconnect()
// re-establishes signaling. Called straight from the 'disconnected' event.
function onPeerDisconnected(room) {
  if (room.destroyed) return;
  setConn(room, 'signaling reconnecting...', 'warn');
  try { room.peer.reconnect(); } catch { }
}

// PeerJS marks signaling failures (VPN, firewall, offline) as FATAL: the
// Peer instance is destroyed and reconnect() can't save it. The only cure
// is building a fresh peer, straight from the 'error' event (host) or from
// the 30 s retry timer (member).
function rebuildPeer(room, why) {
  if (room.destroyed) return;
  const now = Date.now();
  if (now - (room.lastRebuild || 0) < 1000) return; // debounce error storms
  room.lastRebuild = now;
  setConn(room, why + ' - rebuilding connection...', 'warn');
  try { room.peer && room.peer.destroy(); } catch { }
  if (room.role === 'host') spawnHostPeer(room);
  else spawnMemberPeer(room);
}

function prepChannel(conn) {
  const dc = conn.dataChannel;
  if (dc) dc.bufferedAmountLowThreshold = BUF_LOW;
}

// --------------------------- file sending ------------------------------
let batchSeq = 0; // unique folder-batch id (one per drop, shared by all peers)

// Every send is a batch - a single file is just a batch of one. Folder
// batches additionally tag the wire with bid/batch/bsize/count and end
// with a single {type:'batch-end'}.
function queueSend(room, file) {
  queueBatch(room, {
    folder: false,
    label: file.name,   // transfer-row title
    root: file.name,    // wire name = the file itself
    total: file.size,
    notice: 'sent "' + file.name + '"',
    entries: [{ file, rel: file.name }],
  });
}

// ------------------------- folder sending ------------------------------
// A folder is one batch: the receiver shows one row, and every file keeps
// its relative path ("Photos/2024/a.jpg") so the tree is rebuilt intact.
function queueFolder(room, root, entries) {
  entries = (entries || []).filter((e) => e && e.file);
  if (!entries.length) {
    setStatus(room, 'folder "' + root + '" is empty', 'warn');
    return;
  }
  const total = entries.reduce((n, e) => n + e.file.size, 0);
  queueBatch(room, {
    folder: true,
    label: root + '/',
    root,
    total,
    bid: 'b' + (++batchSeq),
    notice: 'sent folder "' + root + '" (' + entries.length + ' files)',
    entries,
  });
}

// queue one batch for every connected peer: one transfer row + one
// per-connection queue entry each, so drops stack and peers go in parallel
function queueBatch(room, batch) {
  const targets = openConns(room);
  if (!targets.length) {
    setStatus(room, 'nobody connected - "' + batch.label + '" not sent', 'warn');
    return;
  }
  for (const conn of targets) {
    let q = room.sendQueues.get(conn);
    if (!q) { q = []; room.sendQueues.set(conn, q); }
    const xf = addXfer(room, '↑', batch.label, batch.total);
    xf.conn = conn;
    xf.state = 'waiting';
    if (batch.folder) {
      xf.batchId = batch.bid;
      xf.filesTotal = batch.entries.length;
      xf.filesDone = 0;
    }
    q.push({ batch, xf });
    pumpConn(room, conn);
  }
}

// drag/drop may contain folders; the entry API has to be read before any
// await, otherwise the DataTransfer is neutered
function queueDrop(room, dt) {
  const items = dt.items ? Array.from(dt.items) : [];
  const picked = [];
  if (items.length && items[0].webkitGetAsEntry) {
    for (const it of items) {
      if (it.kind !== 'file') continue;
      picked.push({
        entry: it.webkitGetAsEntry ? it.webkitGetAsEntry() : null,
        file: it.getAsFile ? it.getAsFile() : null,
      });
    }
  } else {
    for (const f of dt.files || []) picked.push({ entry: null, file: f });
  }
  (async () => {
    try {
      for (const p of picked) {
        if (p.entry && p.entry.isDirectory) {
          const entries = [];
          await walkEntry(p.entry, '', entries);
          queueFolder(room, p.entry.name, entries);
        } else {
          const f = p.file || (p.entry && await entryFile(p.entry));
          if (f) queueSend(room, f);
        }
      }
    } catch (err) {
      setStatus(room, 'could not read that drop: ' + (err && err.message ? err.message : err), 'err');
    }
  })();
}

function entryFile(entry) {
  return new Promise((resolve) => entry.file(resolve, () => resolve(null)));
}

async function walkEntry(entry, rel, out) {
  if (entry.isFile) {
    const file = await entryFile(entry);
    if (file) out.push({ file, rel: rel || file.name });
    return;
  }
  if (!entry.isDirectory) return;
  const reader = entry.createReader();
  const children = [];
  for (; ;) {
    const batch = await new Promise((resolve) =>
      reader.readEntries(resolve, () => resolve([])));
    if (!batch.length) break;
    children.push(...batch);
  }
  for (const child of children) {
    await walkEntry(child, rel ? rel + '/' + child.name : child.name, out);
  }
}

// <input webkitdirectory> gives every file a "Root/sub/file" relative path
function sendPickedFolder(room, files) {
  const groups = new Map();
  for (const f of files) {
    const parts = String(f.webkitRelativePath || f.name)
      .replace(/\\/g, '/').split('/').filter(Boolean);
    if (!parts.length) continue;
    const root = parts.shift();
    const rel = parts.join('/') || f.name;
    let g = groups.get(root);
    if (!g) { g = []; groups.set(root, g); }
    g.push({ file: f, rel });
  }
  for (const [root, entries] of groups) queueFolder(room, root, entries);
}

// each connection gets its own sequential pump: the meta → chunks → done
// protocol must never interleave on one data channel
async function pumpConn(room, conn) {
  if (room.sending.has(conn)) return;
  room.sending.add(conn);
  try {
    const q = room.sendQueues.get(conn);
    while (q && q.length && !room.destroyed && conn.open) {
      const item = q.shift();
      const xf = item.xf;
      if (xf.cancelled) continue; // cancelled while still waiting in queue
      try {
        await sendBatch(room, conn, item.batch, xf);
      } catch {
        failSend(room, conn, xf, 'transfer error');
      }
    }
  } finally {
    room.sending.delete(conn);
  }
}

function wireName(batch, entry) {
  return batch.folder ? batch.root + '/' + entry.rel : entry.rel;
}

// one pump for single files AND folders: meta → 64 KB chunks (with
// backpressure) → done, then one batch-end for folder batches
async function sendBatch(room, conn, batch, xf) {
  room.sendingByConn.set(conn, xf);
  xf.state = 'active';
  for (const entry of batch.entries) {
    if (xf.cancelled) return cancelSend(room, conn, xf);
    if (room.destroyed || !conn.open) return failSend(room, conn, xf, 'connection closed');
    const meta = { type: 'meta', name: wireName(batch, entry), size: entry.file.size };
    if (batch.folder) {
      meta.bid = batch.bid;
      meta.batch = batch.root;
      meta.bsize = batch.total;
      meta.count = batch.entries.length;
    }
    if (!safeSend(conn, meta)) {
      return failSend(room, conn, xf, 'failed - connection closed');
    }
    let offset = 0;
    while (offset < entry.file.size) {
      if (xf.cancelled) return cancelSend(room, conn, xf);
      if (room.destroyed || !conn.open) return failSend(room, conn, xf, 'connection closed');
      const buf = await entry.file.slice(offset, offset + CHUNK).arrayBuffer(); // never whole file in RAM
      await drain(conn); // backpressure: wait until < 1 MB buffered
      safeSend(conn, buf);
      offset += buf.byteLength;
      xf.bytes += buf.byteLength;
    }
    if (xf.cancelled) return cancelSend(room, conn, xf);
    safeSend(conn, { type: 'done' });
    if (batch.folder) xf.filesDone = (xf.filesDone || 0) + 1;
  }
  if (batch.folder) safeSend(conn, { type: 'batch-end', bid: batch.bid });
  xf.bytes = batch.total;
  xf.state = 'done';
  xf.doneAt = performance.now(); // item lingers 2 s, then the ticker prunes it
  room.sendingByConn.delete(conn);
  if (!room.destroyed) setStatus(room, batch.notice, 'ok');
}

// drop a queued/active send row and release the connection slot
function cancelSend(room, conn, xf) {
  removeXfer(room, xf);
  room.sendingByConn.delete(conn);
}

function failSend(room, conn, xf, note) {
  xf.state = 'failed';
  xf.note = note;
  if (!xf.doneAt) xf.doneAt = performance.now();
  room.sendingByConn.delete(conn);
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

// -------------------------- file receiving -----------------------------
function isBinary(d) {
  return d instanceof ArrayBuffer ||
    ArrayBuffer.isView(d) ||
    (typeof Blob !== 'undefined' && d instanceof Blob);
}

// PeerJS fires 'data' in order, but onData() awaits (Blob reassembly, disk
// writes). Without serializing, a 'done' or the next file's 'meta' could be
// handled while earlier chunks were still pending, truncating the file or
// mixing two transfers. One promise chain per connection keeps wire order.
function enqueueData(room, conn, data) {
  const prev = room.recvChains.get(conn) || Promise.resolve();
  const next = prev
    .then(() => onData(room, conn, data))
    .catch((err) => {
      if (!room.destroyed) {
        setStatus(room, 'transfer error: ' + (err && err.message ? err.message : err), 'err');
      }
    });
  room.recvChains.set(conn, next);
  next.then(() => {
    if (room.recvChains.get(conn) === next) room.recvChains.delete(conn);
  });
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
      if (data.type === 'members') {
        // only the host defines membership; members use the list to mesh up
        if (conn !== room.conn) return;
        room.memberCount = Number(data.count) || 1;
        if (Array.isArray(data.peers)) syncPeerList(room, data.peers);
        updateHeader(room);
        return;
      }
      if (data.type === 'meta') { await startReceive(room, conn, data); return; }
      if (data.type === 'done') { await finishReceive(room, conn); return; }
      if (data.type === 'batch-end') { finishBatch(room, conn, data.bid); return; }
      if (data.type === 'cancel') {
        // the peer cancelled a transfer; the direction tells us which one:
        //   dir '↑' → their outgoing (our incoming)
        //   dir '↓' → their incoming (our outgoing)
        if (data.bid) {
          // folder batch: cancel exactly that batch, not whatever is active
          if (data.dir !== '↓') await abortBatch(room, conn, data.bid, 'cancelled by peer');
          if (data.dir !== '↑') {
            const out = room.sendingByConn.get(conn);
            if (out && out.batchId === data.bid) {
              out.cancelled = true;
              removeXfer(room, out);
            }
          }
          return;
        }
        if (data.dir !== '↓') await abortIncoming(room, conn, 'cancelled by peer');
        if (data.dir !== '↑') {
          const out = room.sendingByConn.get(conn);
          if (out) {
            out.cancelled = true;
            removeXfer(room, out);
          }
        }
        return;
      }
    }
  } catch (err) {
    setStatus(room, 'transfer error: ' + (err && err.message ? err.message : err), 'err');
  }
}

// wire path -> safe parts: no "", ".", "..", ":" and never a drive root
function sanitizeParts(path) {
  return String(path || '')
    .replace(/\\/g, '/')
    .split('/')
    .filter((p) => p && p !== '.' && p !== '..' && p.indexOf(':') === -1);
}

async function entryExists(dir, name) {
  try { await dir.getFileHandle(name); return true; } catch { }
  try { await dir.getDirectoryHandle(name); return true; } catch { }
  return false;
}

// first free slot: "a.txt", "a (1).txt", ...
async function uniqueName(dir, name) {
  let cand = name;
  if (!(await entryExists(dir, cand))) return cand;
  const dot = name.lastIndexOf('.');
  const base = dot > 0 ? name.slice(0, dot) : name;
  const ext = dot > 0 ? name.slice(dot) : '';
  for (let i = 1; ; i++) {
    cand = base + ' (' + i + ')' + ext;
    if (!(await entryExists(dir, cand))) return cand;
  }
}

// folder roots also reserve the name in memory, so two peers sending
// "Photos" at the same time cannot both pick the same directory
async function uniqueDirName(dir, name, reserved) {
  const dot = name.lastIndexOf('.');
  const base = dot > 0 ? name.slice(0, dot) : name;
  const ext = dot > 0 ? name.slice(dot) : '';
  let cand = name;
  let i = 0;
  while (reserved.has(cand) || await entryExists(dir, cand)) {
    i++;
    cand = base + ' (' + i + ')' + ext;
  }
  reserved.add(cand);
  return cand;
}

// remove a partial incoming folder; safe on a tree that was never created
async function dropBatchTree(b) {
  if (!b || !b.parent || !b.actualRoot) return;
  try { await b.parent.removeEntry(b.actualRoot, { recursive: true }); } catch { }
  b.dirHandle = null;
}

async function startReceive(room, conn, meta) {
  const parts = sanitizeParts(meta.name);
  const name = parts.length ? parts[parts.length - 1] : 'unnamed';
  const size = Number(meta.size) || 0;
  const bid = typeof meta.bid === 'string' && meta.bid ? meta.bid : null;

  // ---------- folder batch ----------
  let b = null;
  if (bid) {
    b = room.recvBatches.get(conn) || null;
    if (b && b.bid !== bid) b = null; // a new folder started
    if (!b) {
      b = {
        bid,
        root: sanitizeParts(meta.batch)[0] || parts[0] || 'folder',
        total: Number(meta.bsize) || 0,
        bytes: 0,
        filesDone: 0,
        filesTotal: Number(meta.count) || 0,
        xf: null,
        parent: null,     // output directory handle
        dirHandle: null,  // the unique folder we created for this batch
        actualRoot: null, // its name on disk ("Photos (1)")
        cancelled: false,
        noticeShown: false,
      };
      room.recvBatches.set(conn, b);
    }
    if (b.cancelled) {
      // late bytes for a folder we already dropped: absorb them silently
      const dead = {
        name, size, bytes: 0, writable: null, chunks: null,
        pending: [], open: true, drop: true, batch: b,
      };
      room.recvStates.set(conn, dead);
      return;
    }
  }

  const st = {
    name, size, bytes: 0,
    writable: null,
    chunks: null,   // download-mode buffer
    pending: [],    // chunks that arrive before the writable handle is ready
    open: true,
    drop: false,
    batch: b,
    rel: b ? parts.slice(1) : parts,
  };
  if (b) {
    if (!b.xf) {
      b.xf = addXfer(room, '↓', b.root + '/', b.total || size);
      b.xf.conn = conn;
      b.xf.batchId = b.bid;
      b.xf.batchName = b.root;
      b.xf.filesTotal = b.filesTotal;
      b.xf.filesDone = 0;
    }
    b.xf.state = 'active';
    st.xf = b.xf;
  } else {
    st.xf = addXfer(room, '↓', name, size);
    st.xf.conn = conn;
    st.xf.state = 'active';
  }
  // register BEFORE the awaits below - chunks can arrive immediately
  room.recvStates.set(conn, st);

  const dir = roomFolders.get(room.id);
  const canStream = !!(window.showDirectoryPicker && dir);

  // no output folder (or no File System Access API) → browser download
  if (!canStream) {
    st.chunks = [];
    if (!window.showDirectoryPicker) {
      const huge = size > 2 * 1024 * 1024 * 1024; // 2 GB: risky in a tab
      setStatus(room,
        '"' + name + '" (' + fmtBytes(size) + ')' + (huge
          ? ' may crash this tab - use Chrome or Edge for big files'
          : ' will download when it arrives'),
        huge ? 'warn' : undefined);
    } else if (b) {
      if (!b.noticeShown) {
        b.noticeShown = true;
        setStatus(room, 'no output folder - "' + b.root + '" will arrive as separate browser downloads', 'warn');
      }
    } else {
      setStatus(room, 'no output folder - "' + name + '" will download instead; pick a folder below to save big files safely', 'warn');
    }
    return;
  }

  try {
    let targetDir = dir;
    let targetName = name;
    if (b) {
      // pick one folder tree per batch, once, so every file lands in it
      if (!b.parent) {
        b.parent = dir;
        b.actualRoot = await uniqueDirName(dir, b.root, room.recvRoots);
        b.dirHandle = await dir.getDirectoryHandle(b.actualRoot, { create: true });
      }
      targetDir = b.dirHandle;
      for (const p of parts.slice(1, -1)) {
        targetDir = await targetDir.getDirectoryHandle(p, { create: true });
      }
      targetName = await uniqueName(targetDir, name);
    }
    const fh = await targetDir.getFileHandle(targetName, { create: true });
    const writable = await fh.createWritable(); // streams straight to disk
    if (!st.open) {
      try { await writable.abort(); } catch { }
      if (b) await dropBatchTree(b); // cancelled while we were opening it
      return;
    }
    st.writable = writable;
    // flush whatever arrived while the handle was opening
    const pending = st.pending;
    st.pending = [];
    for (const buf of pending) await writable.write(buf);
  } catch (err) {
    st.open = false;
    st.xf.state = 'failed';
    st.xf.note = 'cannot write file';
    st.xf.doneAt = performance.now();
    room.recvStates.delete(conn);
    if (st.batch) {
      st.batch.cancelled = true;
      await dropBatchTree(st.batch);
    }
    if (conn.open) {
      const msg = { type: 'cancel', dir: '↓' };
      if (bid) msg.bid = bid;
      safeSend(conn, msg);
    }
    setStatus(room, 'cannot write "' + name + '": ' + (err && err.message ? err.message : err), 'err');
  }
}

async function receiveChunk(room, conn, buf) {
  const st = room.recvStates.get(conn);
  if (!st || !st.open || st.drop) return; // stray chunk after an error/abort
  st.bytes += buf.byteLength;
  if (st.batch) {
    if (st.batch.xf) st.batch.xf.bytes = st.batch.bytes + st.bytes;
  } else {
    st.xf.bytes = st.bytes;
  }
  try {
    if (st.writable) await st.writable.write(buf);
    else if (st.chunks) st.chunks.push(buf);
    else st.pending.push(buf); // writable still being created
  } catch (err) {
    st.open = false;
    room.recvStates.delete(conn);
    if (st.batch) {
      st.batch.cancelled = true;
      await dropBatchTree(st.batch);
    }
    if (conn.open) {
      const msg = { type: 'cancel', dir: '↓' };
      if (st.batch) msg.bid = st.batch.bid;
      safeSend(conn, msg); // stop the sender
    }
    if (st.writable) { try { await st.writable.abort(); } catch { } st.writable = null; }
    st.chunks = null;
    st.pending = [];
    st.xf.state = 'failed';
    st.xf.note = 'write error';
    st.xf.doneAt = performance.now();
    setStatus(room, 'write failed on "' + st.name + '": ' + (err && err.message ? err.message : err), 'err');
  }
}

async function finishReceive(room, conn) {
  const st = room.recvStates.get(conn);
  if (!st) return;
  room.recvStates.delete(conn);
  st.open = false;
  if (st.drop) return; // leftovers from a folder we already dropped
  if (st.writable) {
    // extremely unlikely: a chunk landed while startReceive was flushing
    for (const buf of st.pending) { try { await st.writable.write(buf); } catch { } }
    st.pending = [];
    try {
      await st.writable.close();
    } catch {
      st.xf.state = 'failed';
      st.xf.note = 'close error';
      st.xf.doneAt = performance.now();
      if (st.batch) st.batch.cancelled = true;
      return;
    }
    st.writable = null;
  } else if (st.chunks) {
    // no output folder: one flat download per file, browser adds the (N)
    const dl = st.batch
      ? st.batch.root + '_' + (st.rel.join('_') || st.name)
      : st.name;
    downloadBlob(new Blob(st.chunks), dl);
    st.chunks = null;
  } else if (st.pending.length) {
    downloadBlob(new Blob(st.pending), st.name);
    st.pending = [];
  }
  if (st.batch) {
    st.batch.bytes += st.bytes;
    st.batch.filesDone++;
    if (st.batch.xf) st.batch.xf.bytes = st.batch.bytes;
    return; // the folder row finishes on { type: 'batch-end' }
  }
  st.xf.bytes = st.size || st.bytes;
  st.xf.state = 'done';
  st.xf.doneAt = performance.now(); // lingers 2 s, then the ticker prunes it
  setStatus(room, 'received "' + st.name + '" (' + fmtBytes(st.bytes) + ')', 'ok');
}

function finishBatch(room, conn, bid) {
  const b = room.recvBatches.get(conn);
  if (!b || (bid && b.bid !== bid)) return;
  room.recvBatches.delete(conn);
  if (b.cancelled) return;
  if (b.xf) {
    b.xf.bytes = b.bytes;
    b.xf.filesDone = b.filesDone;
    b.xf.state = 'done';
    b.xf.doneAt = performance.now(); // lingers 2 s, then the ticker prunes it
  }
  setStatus(room, 'received folder "' + b.root + '" (' + b.filesDone + ' files, ' + fmtBytes(b.bytes) + ')', 'ok');
}

// drop an incoming transfer and discard the partial file on disk
async function abortIncoming(room, conn, why) {
  const st = room.recvStates.get(conn);
  if (!st) return;
  room.recvStates.delete(conn);
  st.open = false;
  st.pending = [];
  st.chunks = null;
  if (st.writable) {
    try { await st.writable.abort(); } catch { } // does not keep the partial
    st.writable = null;
  }
  if (st.xf && !st.batch && st.xf.state !== 'done') {
    st.xf.state = 'failed';
    st.xf.note = why || 'cancelled';
    st.xf.doneAt = performance.now();
  }
}

// cancel a whole incoming folder: stop the current file and remove the tree
async function abortBatch(room, conn, bid, why) {
  const b = room.recvBatches.get(conn);
  if (!b || (bid && b.bid !== bid)) return;
  b.cancelled = true;
  await abortIncoming(room, conn, why);
  await dropBatchTree(b);
  if (b.xf) {
    b.xf.state = 'failed';
    b.xf.note = why || 'cancelled';
    b.xf.doneAt = performance.now();
  }
  // keep the record briefly so late metas/chunks for it are swallowed
  setTimeout(() => {
    if (room.recvBatches.get(conn) === b) room.recvBatches.delete(conn);
  }, 30000);
}

// connection died: fail its queued/active sends and abort any receive
function cleanupConn(room, conn) {
  const q = room.sendQueues.get(conn);
  if (q) {
    for (const entry of q) {
      entry.xf.state = 'failed';
      entry.xf.note = 'connection closed';
      entry.xf.doneAt = performance.now();
    }
    room.sendQueues.delete(conn);
  }
  const active = room.sendingByConn.get(conn);
  if (active) {
    active.state = 'failed';
    active.note = 'connection closed';
    active.doneAt = performance.now();
  }
  room.sendingByConn.delete(conn);
  room.recvChains.delete(conn);
  const b = room.recvBatches.get(conn);
  if (b) {
    room.recvBatches.delete(conn);
    b.cancelled = true;
    if (b.xf) {
      b.xf.state = 'failed';
      b.xf.note = 'connection closed';
      b.xf.doneAt = performance.now();
    }
  }
  abortIncoming(room, conn, 'connection closed');
  if (b) dropBatchTree(b);
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
let xferSeq = 0; // unique transfer ids (DOM lookups are keyed on them)

function addXfer(room, dir, name, size) {
  const id = (dir === '↑' ? 'up-' : 'down-') + (++xferSeq);
  const xf = {
    id, dir, name, size,
    bytes: 0,
    speed: 0,
    state: 'active',   // 'waiting' | 'active' | 'done' | 'failed'
    note: '',
    cancelled: false,  // set by cancelTransfer / a wire { type: 'cancel' }
    conn: null,
    tRef: performance.now(), bRef: 0, doneAt: 0,
  };
  room.xfers.set(id, xf);
  // render right away: even a fast transfer must be visible (the ticker
  // below only picks up items that were added while detached)
  const list = room.ui.transfersList;
  if (list) {
    const el = buildTransferItem(room, xf);
    list.appendChild(el);
    room.xferEls.set(id, el);
  }
  return xf;
}

// idempotent: safe to call from the pump, the cancel handler and cleanup
function removeXfer(room, xf) {
  if (!xf || !room.xfers.has(xf.id)) return;
  room.xfers.delete(xf.id);
  const el = room.xferEls.get(xf.id);
  if (el) { el.remove(); room.xferEls.delete(xf.id); }
}

function transferStats(xf) {
  if (xf.state === 'done') return '✓ complete';
  if (xf.state === 'failed') return '✗ ' + (xf.note || 'failed');
  const pct = xf.size ? Math.min(100, Math.floor((xf.bytes / xf.size) * 100)) : 0;
  let text = pct + '% · ' + fmtBytes(xf.bytes) + (xf.size ? ' / ' + fmtBytes(xf.size) : '');
  if (xf.state === 'waiting') text += ' · queued';
  else if (xf.speed > 1) text += ' · ' + fmtSpeed(xf.speed);
  if (xf.filesTotal) text += ' · ' + (xf.filesDone || 0) + '/' + xf.filesTotal + ' files';
  return text;
}

function updateTransferItem(xf, el) {
  const pct = xf.state === 'done'
    ? 100
    : (xf.size ? Math.min(100, Math.floor((xf.bytes / xf.size) * 100)) : 0);
  const fill = el.querySelector('.progress-fill');
  if (fill) fill.style.width = pct + '%';
  const stats = el.querySelector('.transfer-stats');
  if (stats) stats.textContent = transferStats(xf);
}

function buildTransferItem(room, xf) {
  const item = h('div');
  item.className = 'transfer-item';

  const header = h('div');
  header.className = 'transfer-header';
  const name = h('span', xf.dir + ' ' + xf.name);
  name.className = 'file-name';
  name.title = xf.name;
  header.appendChild(name);

  const meta = h('span');
  meta.className = 'transfer-meta';
  const stats = h('span', transferStats(xf));
  stats.className = 'transfer-stats';
  meta.appendChild(stats);
  const cancel = h('button', '×');
  cancel.className = 'cancel-btn';
  cancel.title = 'cancel transfer';
  cancel.addEventListener('click', () => cancelTransfer(room, xf));
  meta.appendChild(cancel);
  header.appendChild(meta);
  item.appendChild(header);

  const track = h('div');
  track.className = 'progress-track';
  const fill = h('div');
  fill.className = 'progress-fill';
  track.appendChild(fill);
  item.appendChild(track);

  updateTransferItem(xf, item);
  return item;
}

// cancel exactly one transfer: tell the other side which direction it is,
// drop the partial file, and remove the item right away
function cancelTransfer(room, xf) {
  if (xf.cancelled || xf.state === 'done') return;
  xf.cancelled = true;
  if (xf.conn && xf.conn.open) {
    const msg = { type: 'cancel', dir: xf.dir };
    if (xf.batchId) msg.bid = xf.batchId;
    safeSend(xf.conn, msg);
  }
  if (xf.dir === '↓') {
    if (xf.batchId) abortBatch(room, xf.conn, xf.batchId, 'cancelled');
    else abortIncoming(room, xf.conn, 'cancelled');
  }
  removeXfer(room, xf);
}

setInterval(() => {
  const now = performance.now();
  for (const room of rooms.values()) {
    if (room.destroyed) continue;
    // rolling speed for active transfers
    for (const xf of room.xfers.values()) {
      if (xf.state !== 'active' || xf.doneAt) continue;
      const dt = (now - xf.tRef) / 1000;
      if (dt >= 0.3) {
        xf.speed = (xf.bytes - xf.bRef) / dt;
        xf.tRef = now;
        xf.bRef = xf.bytes;
      }
    }
    // finished items linger 2 s, then disappear (empty list = no space)
    for (const xf of [...room.xfers.values()]) {
      if (xf.doneAt && now - xf.doneAt > 2000) removeXfer(room, xf);
    }
    // lightweight updates only: mutate text/width on the cached elements
    for (const xf of room.xfers.values()) {
      const el = room.xferEls.get(xf.id);
      if (el) updateTransferItem(xf, el);
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
  room.recvRoots.clear(); // names are relative to the new folder
  setStatus(room, 'output folder set: ' + handle.name, 'ok');
  updateFolderLabel(room);
}

// copy an invite link (origin + path #XXXXX) that auto-joins the room when
// opened; the share button flips to "copied!" for 1.5 s
async function copyId(room) {
  const url = window.location.origin + window.location.pathname + '#' + room.id;
  let ok = true;
  try {
    await navigator.clipboard.writeText(url);
  } catch {
    ok = false;
  }
  room.ui.shareBtn.textContent = ok ? 'copied!' : 'copy failed';
  setTimeout(() => {
    if (!room.destroyed) room.ui.shareBtn.textContent = 'share link';
  }, 1500);
}

function destroyRoom(room) {
  if (room.destroyed) return;
  room.destroyed = true;
  clearInterval(room.retryTimer);
  clearInterval(room.meshTimer);
  for (const c of room.conns.values()) { try { c.close(); } catch { } }
  if (room.conn) { try { room.conn.close(); } catch { } }
  if (room.peer) { try { room.peer.destroy(); } catch { } }
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
  if (typeof Peer === 'undefined') { alert('PeerJS failed to load - check your connection.'); return; }
  // the directory picker MUST run inside this direct click handler
  // (File System Access API user-gesture requirement)
  const folder = await pickFolder();
  if (folder === undefined) return;
  createHost(genId(), folder, false);
});

// restore persisted rooms on load (folder handles can't be persisted -
// set them again via the footer link; all picker calls stay inside
// direct click handlers)
loadRooms();

// The invite link is the only way to join now: opening .../#K7X9Q (or
// pasting the hash into a tab that is already open) joins that room.
// The folder picker can't open without a click, so the room starts
// folderless; "choose output folder" in the card footer sets it later.
function joinFromHash() {
  const code = (window.location.hash || '').replace('#', '').trim().toUpperCase();
  if (!/^[A-Z0-9]{5}$/.test(code)) return;
  // never leave the code in the address bar: a refresh should not re-join
  history.replaceState(null, '', window.location.pathname);
  if (rooms.has(code)) return;            // already in that room
  if (typeof Peer === 'undefined') return; // PeerJS failed to load
  createMember(code, null, false);        // createMember() persists the room
}

if (document.readyState === 'loading') {
  window.addEventListener('DOMContentLoaded', joinFromHash);
} else {
  joinFromHash();
}
window.addEventListener('hashchange', joinFromHash);

if (!window.showDirectoryPicker) {
  if (IS_FIREFOX) {
    compatEl.className = 'compat-warn';
    compatEl.textContent =
      'warning: Firefox may crash on very large incoming files - use Chrome or Edge for those.';
  } else {
    compatEl.textContent =
      'note: this browser can\'t save incoming files straight to a folder, so very large ' +
      'files may run out of memory. Chrome or Edge handle big files better.';
  }
}
