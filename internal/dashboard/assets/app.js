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

/** Wall-clock time in the viewer's locale and zone — never a hardcoded format. */
export function time(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '—' : timeFmt.format(d);
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
      detail = body?.error ? ` — ${body.error}` : '';
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
    toast('Could not copy — your browser blocked clipboard access.', 'error');
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
