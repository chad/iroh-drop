// iroh-drop web app — share & receive flows on top of the wasm node.
import init, { WebDrop } from '../pkg/iroh_drop_web.js';

const $ = (id) => document.getElementById(id);
const logEl = $('log');
const log = (msg) => {
  const t = new Date().toLocaleTimeString();
  logEl.textContent += `[${t}] ${msg}\n`;
  logEl.scrollTop = logEl.scrollHeight;
};

const fmtSize = (n) =>
  n > 1e9 ? (n / 1e9).toFixed(2) + ' GB' :
  n > 1e6 ? (n / 1e6).toFixed(1) + ' MB' :
  n > 1e3 ? (n / 1e3).toFixed(1) + ' KB' : n + ' B';

// Relay: null = n0's public relays (rate-limited for big transfers).
// Self-hosting? Set e.g. 'https://relay.example.com' and redeploy.
const RELAY_URL = null;

// Identity: 32 bytes in localStorage, so peers recognize us across reloads.
const b64 = (u8) => btoa(String.fromCharCode(...u8));
const unb64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));

// Pull a ticket out of whatever is pasted or in the URL: a bare drop2…
// string, an iroh-drop://receive/… link, an https://…#drop2… link, or any
// of those inside chat chatter. Same semantics as the desktop extractors.
const extractTicket = (input) => {
  const i2 = input.indexOf('drop2');
  const i1 = input.indexOf('drop1');
  const i = i2 === -1 ? i1 : (i1 === -1 ? i2 : Math.min(i1, i2));
  if (i < 0) return '';
  const m = input.slice(i).match(/^[a-z0-9]+/);
  // Base32 with no padding; anything this short is a typo, not a ticket.
  return m && m[0].length > 32 ? m[0] : '';
};

let session = null;
let peers = 0;
const offers = new Map(); // hash -> {name, size, mediaType, have, el, bar, btn}

// ---------- rendering ----------

function statusText() {
  return $('status-text');
}

function setStatus(text, up) {
  statusText().textContent = text;
  $('status-dot').classList.toggle('up', !!up);
}

function offerRow(hash, o) {
  const li = document.createElement('li');
  const left = document.createElement('div');
  left.style.minWidth = '0';
  const name = document.createElement('span');
  name.className = 'fname';
  name.textContent = o.name;
  const size = document.createElement('span');
  size.className = 'fsize';
  size.textContent = fmtSize(o.size);
  left.append(name, size);
  const bar = document.createElement('div');
  bar.className = 'progress';
  bar.hidden = true;
  bar.append(document.createElement('div'));
  left.append(bar);
  li.append(left);

  if (o.have) {
    const st = document.createElement('span');
    st.className = 'fstate';
    st.textContent = 'on this device';
    li.append(st);
  } else {
    const btn = document.createElement('button');
    btn.className = 'btn small';
    btn.textContent = 'Accept';
    btn.onclick = () => fetchAndSave(hash, o);
    li.append(btn);
    o.btn = btn;
  }
  o.el = li;
  o.bar = bar;
  return li;
}

function renderOffers() {
  const el = $('offers');
  el.innerHTML = '';
  for (const [hash, o] of offers) el.append(offerRow(hash, o));
  $('receive-empty').hidden = offers.size !== 0;
}

function renderShared(hash, file) {
  const li = document.createElement('li');
  const left = document.createElement('div');
  const name = document.createElement('span');
  name.className = 'fname';
  name.textContent = file.name;
  const size = document.createElement('span');
  size.className = 'fsize';
  size.textContent = fmtSize(file.size);
  left.append(name, size);
  const st = document.createElement('span');
  st.className = 'fstate';
  st.textContent = 'offered';
  li.append(left, st);
  $('shared-list').append(li);
}

// ---------- transfers ----------

async function fetchAndSave(hash, o) {
  if (o.btn) {
    o.btn.disabled = true;
    o.btn.textContent = 'fetching…';
  }
  if (o.bar) o.bar.hidden = false;
  try {
    const bytes = await session.fetch(hash);
    const blob = new Blob([bytes], { type: o.mediaType || 'application/octet-stream' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = o.name;
    a.click();
    URL.revokeObjectURL(a.href);
    o.have = true;
    renderOffers();
    log(`saved ${o.name} (${fmtSize(o.size)})`);
  } catch (e) {
    log(`fetch failed for ${o.name}: ${e}`);
    if (o.btn) { o.btn.disabled = false; o.btn.textContent = 'Retry'; }
    if (o.bar) o.bar.hidden = true;
  }
}

async function addFiles(files) {
  for (const f of files) {
    try {
      const bytes = new Uint8Array(await f.arrayBuffer());
      await session.publish(f.name, bytes, f.type || null);
      renderShared(null, f);
      log(`offered ${f.name} (${fmtSize(f.size)}) — bytes stay here until accepted`);
    } catch (e) {
      log(`failed to offer ${f.name}: ${e}`);
    }
  }
}

// ---------- events ----------

function onEvent(ev) {
  log(JSON.stringify(ev));
  switch (ev.kind) {
    case 'peerJoined':
      peers++;
      break;
    case 'peerLeft':
      peers = Math.max(0, peers - 1);
      break;
    case 'offerReceived': {
      const o = offers.get(ev.hash) || { have: false };
      o.name = ev.name; o.size = ev.size; o.mediaType = ev.mediaType;
      offers.set(ev.hash, o);
      renderOffers();
      break;
    }
    case 'fetchProgress': {
      const o = offers.get(ev.hash);
      if (o && o.bar && !o.bar.hidden && ev.total) {
        o.bar.firstElementChild.style.width = Math.min(100, (ev.downloaded / ev.total) * 100) + '%';
      }
      return; // too chatty for status
    }
    default:
      return;
  }
  const ep = $('status-text').dataset.ep || '';
  setStatus(`${ep} · ${peers} peer${peers === 1 ? '' : 's'}`, true);
}

// ---------- main ----------

async function main() {
  // Nav links scroll without clobbering the ticket in location.hash.
  document.querySelectorAll('[data-scroll]').forEach((a) => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      const id = a.getAttribute('data-scroll');
      (id === 'top' ? document.body : document.getElementById(id))
        .scrollIntoView({ behavior: 'smooth' });
    });
  });

  await init();
  const stored = localStorage.getItem('iroh-drop:identity');
  const drop = await WebDrop.start(stored ? unb64(stored) : null, RELAY_URL);
  localStorage.setItem('iroh-drop:identity', b64(drop.identity()));
  const ep = `endpoint ${drop.endpoint_id().slice(0, 10)}…`;
  $('status-text').dataset.ep = ep;
  setStatus(`${ep} · 0 peers`, true);

  const ticket = extractTicket(location.hash);

  if (ticket.startsWith('drop2') || ticket.startsWith('drop1')) {
    // Receive mode.
    document.title = 'iroh-drop — you received files';
    $('receive-banner').hidden = false;
    $('receive-view').hidden = false;
    try {
      session = await drop.join(ticket);
    } catch (e) {
      setStatus('that ticket does not look right — ask for a fresh link', false);
      log(`join failed: ${e}`);
      return;
    }
    session.on_event(onEvent);
    $('open-in-app').href = `iroh-drop://receive/${ticket}`;
    log('joined drop; syncing history…');
    for (const o of await session.offers()) {
      offers.set(o.hash, { name: o.name, size: o.size, mediaType: o.mediaType, have: o.have });
    }
    renderOffers();
    $('reset-link').onclick = (e) => {
      e.preventDefault();
      location.hash = '';
      location.reload();
    };
  } else {
    // Share mode.
    $('share-view').hidden = false;
    session = await drop.create(null);
    session.on_event(onEvent);
    const link = `${location.origin}${location.pathname}#${session.ticket()}`;
    $('share-link').textContent = link;
    log('created drop');

    $('copy-link').onclick = async () => {
      try {
        await navigator.clipboard.writeText(link);
        $('copy-link').textContent = 'Copied';
        setTimeout(() => ($('copy-link').textContent = 'Copy'), 1500);
      } catch {
        log('clipboard unavailable — copy the link manually');
      }
    };

    const dz = $('dropzone');
    dz.onclick = () => $('file').click();
    dz.onkeydown = (e) => { if (e.key === 'Enter' || e.key === ' ') $('file').click(); };
    $('file').onchange = (e) => addFiles(e.target.files);
    dz.ondragover = (e) => { e.preventDefault(); dz.classList.add('drag'); };
    dz.ondragleave = () => dz.classList.remove('drag');
    dz.ondrop = (e) => { e.preventDefault(); dz.classList.remove('drag'); addFiles(e.dataTransfer.files); };

    $('join-btn').onclick = () => {
      const t = extractTicket($('ticket-input').value);
      if (t.startsWith('drop2') || t.startsWith('drop1')) { location.hash = '#' + t; location.reload(); }
      else log('not a valid ticket — expected drop2… or an iroh-drop:// link');
    };
  }
}

main().catch((e) => {
  setStatus(`failed to start: ${e}`, false);
  console.error(e);
});
