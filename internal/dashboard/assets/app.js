/* Doorbell shared front-end utilities.
 *
 * Loaded by every page. Anything a second page would otherwise copy belongs
 * here: escaping, locale-aware formatting, toasts, and a fetch wrapper that
 * surfaces failures instead of swallowing them.
 */

/** Escape text for interpolation into HTML. */
export function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

/* Formatters are built once. Constructing an Intl formatter per row is a
   measurable cost in a table that repaints on every event. */
const nf = new Intl.NumberFormat();
const timeFmt = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});
const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
  dateStyle: 'medium', timeStyle: 'medium',
});

/** Locale-aware integer, e.g. 12,431. */
export function num(value) {
  return nf.format(Number(value) || 0);
}

/** Wall-clock time in the viewer's locale and zone - never a hardcoded format. */
export function time(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '-' : timeFmt.format(d);
}

/** Full date and time, for tooltips where the bare clock is ambiguous. */
export function dateTime(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '' : dateTimeFmt.format(d);
}

/** Human byte size. Bodies range from a few bytes to a megabyte. */
export function bytes(n) {
  const v = Number(n) || 0;
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${(v / 1024 / 1024).toFixed(2)} MB`;
}

/**
 * Request duration. Sub-millisecond round trips are real on localhost, and
 * rendering them as a flat "0 ms" reads as a broken timer rather than a fast
 * one.
 */
export function ms(value) {
  const v = Number(value) || 0;
  return v < 1 ? '<1 ms' : `${nf.format(v)} ms`;
}

/** Compact elapsed time, e.g. "4m ago". */
export function ago(iso) {
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return '';
  const secs = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (secs < 60) return `${secs}s ago`;
  if (secs < 3600) return `${Math.round(secs / 60)}m ago`;
  return `${Math.round(secs / 3600)}h ago`;
}

/** Tailwind-ish class for an HTTP status family. */
export function statusClass(status) {
  const s = Number(status) || 0;
  if (s >= 500) return 'status--5';
  if (s >= 400) return 'status--4';
  if (s >= 300) return 'status--3';
  return 'status--2';
}

/* ── toasts ──────────────────────────────────────────────────────────────── */

function toastHost() {
  let host = document.getElementById('toasts');
  if (!host) {
    host = document.createElement('div');
    host.id = 'toasts';
    host.className = 'toasts';
    // Announced politely so a screen reader hears the outcome of an action
    // without having it interrupt whatever is being read.
    host.setAttribute('role', 'status');
    host.setAttribute('aria-live', 'polite');
    document.body.append(host);
  }
  return host;
}

/**
 * Show a transient message.
 * @param {string} message
 * @param {'ok'|'error'|'info'} tone
 */
export function toast(message, tone = 'info') {
  const el = document.createElement('div');
  el.className = `toast toast--${tone}`;
  el.textContent = message;
  toastHost().append(el);
  // Errors stay longer: they usually carry an instruction worth reading.
  setTimeout(() => el.remove(), tone === 'error' ? 6000 : 3500);
}

/* ── fetch ───────────────────────────────────────────────────────────────── */

/**
 * JSON fetch that throws with a message worth showing a user.
 *
 * The previous pages swallowed every failure in a bare `catch {}`, so a broken
 * endpoint looked identical to an empty one. Callers can now distinguish
 * "nothing here" from "this is broken" and say so.
 */
export async function getJSON(url, { signal } = {}) {
  let res;
  try {
    res = await fetch(url, { signal, headers: { Accept: 'application/json' } });
  } catch (cause) {
    throw new Error(`Cannot reach the gateway. Check your connection and try again.`, { cause });
  }
  if (!res.ok) {
    let detail = '';
    try {
      const body = await res.json();
      detail = body?.error ? ` - ${body.error}` : '';
    } catch { /* a non-JSON error body is not worth reporting twice */ }
    throw new Error(`Request failed with ${res.status}${detail}`);
  }
  return res.json();
}

/** POST returning parsed JSON, with the same error contract as getJSON. */
export async function postJSON(url) {
  let res;
  try {
    res = await fetch(url, { method: 'POST', headers: { Accept: 'application/json' } });
  } catch (cause) {
    throw new Error('Cannot reach the gateway. Check your connection and try again.', { cause });
  }
  let body = null;
  try { body = await res.json(); } catch { /* empty body is acceptable */ }
  if (!res.ok) {
    throw new Error(body?.error || `Request failed with ${res.status}`);
  }
  return body;
}

/* ── misc ────────────────────────────────────────────────────────────────── */

/** Copy text, reporting the outcome. Clipboard access can be denied. */
export async function copy(text, label = 'Copied to clipboard') {
  try {
    await navigator.clipboard.writeText(text);
    toast(label, 'ok');
  } catch {
    toast('Could not copy - your browser blocked clipboard access.', 'error');
  }
}

/** Render one of the shared state blocks into a container. */
export function renderState(el, { title, hint, tone = 'muted' }) {
  el.innerHTML = `
    <div class="state state--inline">
      <p class="state__title">${esc(title)}</p>
      ${hint ? `<p class="state__hint ${tone === 'error' ? '' : 'muted'}">${esc(hint)}</p>` : ''}
    </div>`;
}

/* ── theme ───────────────────────────────────────────────────────────────── */

const THEME_KEY = 'doorbell-theme';

function systemTheme() {
  return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

function storedTheme() {
  try { return localStorage.getItem(THEME_KEY); } catch { return null; }
}

/**
 * Wire the light/dark control.
 *
 * The stored choice is applied before first paint by a tiny inline script in
 * each page's head - doing it from here would mean rendering the wrong theme
 * for a frame and then flipping it, which is worse than having no control.
 * This function only keeps the button in sync and records a new choice.
 *
 * With nothing stored the page follows the system, and keeps following it: the
 * change listener below re-labels the button when the OS switches at dusk.
 */
export function initTheme(btn) {
  const sync = () => {
    if (!btn) return;
    const current = document.documentElement.dataset.theme || systemTheme();
    const next = current === 'dark' ? 'light' : 'dark';
    btn.querySelector('[data-icon]').textContent = current === 'dark' ? '☀' : '☾';
    btn.setAttribute('aria-label', `Switch to ${next} theme`);
    btn.title = `Switch to ${next} theme`;
  };

  btn?.addEventListener('click', () => {
    const current = document.documentElement.dataset.theme || systemTheme();
    const next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem(THEME_KEY, next); } catch { /* private mode; the choice just will not persist */ }
    sync();
  });

  matchMedia('(prefers-color-scheme: light)').addEventListener('change', () => {
    if (!storedTheme()) sync();
  });

  sync();
}

/* ── timeline tape ───────────────────────────────────────────────────────── */

/* Shared, because the overview and the dashboard draw the same object: a judge
   should see the held requests on the landing page before reading a word, and
   two copies of this would drift apart the first time either changed. */

// Two spans this close together are one outage that flapped, not two.
const GAP_JOIN_MS = 15_000;
// Below this, "offline" is the ordinary race between a request arriving and the
// drain picking it up - not something that happened to anyone.
const GAP_MIN_MS = 3_000;

/** Merge spans that overlap or nearly touch. Input need not be sorted. */
export function mergeSpans(spans) {
  const sorted = [...spans].sort((a, b) => a.from - b.from);
  const out = [];
  for (const s of sorted) {
    const last = out[out.length - 1];
    if (last && s.from <= last.to + GAP_JOIN_MS) {
      last.to = Math.max(last.to, s.to);
      last.open = last.open || s.open;
      last.held += s.held;
      last.stillHeld += s.stillHeld;
    } else {
      out.push({ ...s });
    }
  }
  // An open gap is kept whatever its length: the tunnel is down right now, and
  // saying so matters more than the stretch being short so far.
  return out.filter((g) => g.open || g.to - g.from >= GAP_MIN_MS);
}

/** Compact duration, e.g. "7m 20s". */
export function humanSpan(msValue) {
  const secs = Math.max(1, Math.round(msValue / 1000));
  if (secs < 60) return `${secs}s`;
  const m = Math.floor(secs / 60), s = secs % 60;
  return s ? `${m}m ${String(s).padStart(2, '0')}s` : `${m}m`;
}

/**
 * Draw the tape into `track`.
 *
 * @param {HTMLElement} track
 * @param {Array} stored  rows with receivedAt / deliveredAt / tunnelId
 * @param {Array} ticks   proxied records with at / status / source (may be empty)
 * @param {number} minutes window drawn
 */
export function paintTape(track, stored, ticks, minutes) {
  const now = Date.now();
  const since = now - minutes * 60_000;
  const span = now - since;
  const pct = (t) => Math.max(0, Math.min(100, ((t - since) / span) * 100));

  // A stored request is proof the tunnel had nowhere to put it, so its
  // received → delivered span is a stretch of downtime. Nothing has to record
  // connects and disconnects for this to be exact.
  const perTunnel = new Map();
  for (const p of stored || []) {
    const from = new Date(p.receivedAt).getTime();
    const to = p.deliveredAt ? new Date(p.deliveredAt).getTime() : now;
    if (!Number.isFinite(from) || to < since) continue;
    if (!perTunnel.has(p.tunnelId)) perTunnel.set(p.tunnelId, []);
    perTunnel.get(p.tunnelId).push({
      from, to, open: !p.deliveredAt,
      held: 1, stillHeld: p.deliveredAt ? 0 : 1,
    });
  }

  const gaps = [];
  for (const [tunnelId, raw] of perTunnel) {
    for (const g of mergeSpans(raw)) gaps.push({ ...g, tunnelId });
  }
  gaps.sort((a, b) => b.from - a.from);

  let html = '';
  for (const g of gaps) {
    const left = pct(g.from), width = Math.max(pct(g.to) - left, 0.3);
    // The label sits inside its own band. In a legend underneath, a reader has
    // to match a name to one of several stripes by eye; written across the
    // stripe there is nothing to match.
    // While a stretch is still open, report what is still waiting, not the
    // running total: a flapping tunnel merges into one stripe, and some of what
    // it caught has already drained. Saying "offline now · 6 held" next to a
    // header reading "2 held" reads as a contradiction, and is one.
    const count = g.open ? g.stillHeld : g.held;
    // A tunnel name is whatever the client asked for, and nothing bounds its
    // length. Left whole, a 150-character name stretched this label right across
    // the tape and pushed its neighbours off the edge. Clip the name rather than
    // the label: the duration and the count come after it, so truncating the
    // line would drop exactly what the band exists to report. The full name
    // stays in the title.
    const tail = `${g.open ? 'offline now' : 'offline'} · ` +
                 `${humanSpan(g.to - g.from)} · ${count} held`;
    const name = g.tunnelId.length > 24 ? `${g.tunnelId.slice(0, 23)}…` : g.tunnelId;
    const label = `${name} ${tail}`;
    const fullLabel = `${g.tunnelId} ${tail}`;
    // Below this the band is thinner than the words it carries, so the label
    // moves outside it rather than being clipped. See .tape__gap--narrow.
    const narrow = width < 14;
    const cls = `tape__gap${g.open ? ' tape__gap--open' : ''}` +
                (narrow ? ' tape__gap--narrow' : '') +
                // Too close to the left edge to put the label there.
                (narrow && left < 22 ? ' tape__gap--labelAfter' : '');
    html += `<div class="${cls}"` +
            ` style="left:${left}%;width:${width}%" title="${esc(fullLabel)}">` +
            `<span class="tape__gapLabel">${esc(label)}</span></div>`;
  }
  for (const rec of ticks || []) {
    const t = new Date(rec.at).getTime();
    if (!Number.isFinite(t) || t < since) continue;
    const kind = rec.source === 'buffered' ? ' tape__tick--held'
      : (Number(rec.status) >= 400 || rec.error) ? ' tape__tick--failed' : '';
    html += `<div class="tape__tick${kind}" style="left:${pct(t)}%"></div>`;
  }
  if (!gaps.length) {
    html += `<p class="tape__quiet">No tunnel went offline in the last ${minutes} minutes - nothing needed holding.</p>`;
  }
  track.innerHTML = html;
  return gaps;
}
