/* global initSqlJs */
let SQL = null;
let db = null;

const DB_URL = "data/aqua.sqlite";

// --- helpers ---
function $(id) { return document.getElementById(id); }

function setStatus(text, kind) {
  const el = $("dbStatus");
  el.textContent = text;
  el.classList.remove("ok", "warn", "bad");
  if (kind) el.classList.add(kind);
}

function setMeta(text) {
  $("dbMeta").textContent = text || "";
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toHashNow() { location.hash = "#/now"; }
function toHashPermit(key) { location.hash = `#/permit/${encodeURIComponent(key)}`; }
function toHashOwner(identity) { location.hash = `#/owner/${encodeURIComponent(identity)}`; }

function setActiveTab(tabId) {
  for (const id of ["tab-now", "tab-permit", "tab-owner"]) {
    $(id).classList.toggle("active", id === tabId);
  }
}

function showView(viewId) {
  for (const id of ["view-now", "view-permit", "view-owner"]) {
    $(id).style.display = (id === viewId) ? "block" : "none";
  }
}

function execAll(sql, params = []) {
  // sql.js supports positional bind via "?" with array params using prepare
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function one(sql, params = []) {
  const rows = execAll(sql, params);
  return rows.length ? rows[0] : null;
}

// --- load db ---
async function loadDatabase() {
  setStatus("Laster database…");
  setMeta("");

  // init sql.js
  if (!SQL) {
    SQL = await initSqlJs({
      locateFile: (f) => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${f}`,
    });
  }

  // fetch sqlite
  const res = await fetch(`${DB_URL}?v=${Date.now()}`, { cache: "no-store" });

  if (!res.ok) throw new Error(`Kunne ikke hente ${DB_URL} (HTTP ${res.status})`);
  const buf = await res.arrayBuffer();

  // open
  if (db) db.close();
  db = new SQL.Database(new Uint8Array(buf));

  // meta
  const snap = one(`SELECT MAX(snapshot_date) AS max_date, COUNT(*) AS n FROM snapshots;`);
  const pc = one(`SELECT COUNT(*) AS n FROM permit_current;`);
  const oh = one(`SELECT COUNT(*) AS n FROM ownership_history;`);

  const last = snap?.max_date ? `Sist snapshot: ${snap.max_date}` : "Ingen snapshots";
  setStatus("DB lastet", "ok");
  setMeta(`${last} • permit_current: ${pc?.n ?? "?"} • ownership_history: ${oh?.n ?? "?"} • ${Math.round(buf.byteLength/1024)} KB`);

  // render current route
  renderRoute();
}

// --- NOW view ---
function renderNow() {
  setActiveTab("tab-now");
  showView("view-now");

  const q = $("nowSearch").value.trim().toLowerCase();
  const only = $("onlyGrunnrente").checked;

  const baseSql = `
    SELECT permit_key, owner_name, owner_identity, owner_orgnr
    FROM permit_current
    ${only ? "WHERE grunnrente_pliktig = 1" : ""}
    ORDER BY permit_key
  `;
  const rows = execAll(baseSql);

  const filtered = q
    ? rows.filter(r =>
        String(r.permit_key ?? "").toLowerCase().includes(q) ||
        String(r.owner_name ?? "").toLowerCase().includes(q) ||
        String(r.owner_identity ?? "").toLowerCase().includes(q)
      )
    : rows;

  $("nowSummary").textContent =
    `Viser ${filtered.length} av ${rows.length} tillatelser` + (only ? " (grunnrentepliktig)" : "");

  const tbody = $("nowTable").querySelector("tbody");
  tbody.innerHTML = "";

  // cap rendering to keep it snappy; still searchable by narrowing
  const MAX = 2500;
  const displayRows = filtered.slice(0, MAX);

  for (const r of displayRows) {
    const tr = document.createElement("tr");

    const permit = escapeHtml(r.permit_key);
    const ownerName = escapeHtml(r.owner_name);
    const ownerIdent = escapeHtml(r.owner_identity);
    
    const orgnrOrIdent = (r.owner_orgnr && String(r.owner_orgnr).trim())
      ? String(r.owner_orgnr).trim()
      : String(r.owner_identity ?? "");

    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${permit}</a></td>
      <td>${ownerName}</td>
      <td><a class="link" href="#/owner/${encodeURIComponent(r.owner_identity)}">${escapeHtml(orgnrOrIdent)}</a></td>
    `;

    tbody.appendChild(tr);
  }

  if (filtered.length > MAX) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td colspan="3" class="muted">
        Viser kun de første ${MAX} radene. Begrens søket for å se resten.
      </td>
    `;
    tbody.appendChild(tr);
  }
}

// --- PERMIT view ---
function renderPermit(permitKey) {
  setActiveTab("tab-permit");
  showView("view-permit");

  $("permitEmpty").textContent = "";
  $("permitHistoryTable").querySelector("tbody").innerHTML = "";
  $("permitCard").classList.add("hidden");
  if (permitKey) $("permitInput").value = permitKey;

  if (!permitKey) {
    $("permitEmpty").textContent = "Skriv en permit_key i feltet over, eller klikk en tillatelse fra Nå-status.";
    return;
  }

  const now = one(`
    SELECT permit_key, owner_orgnr, owner_name, owner_identity, snapshot_date, grunnrente_pliktig
    FROM permit_current
    WHERE permit_key = ?;
  `, [permitKey]);

  if (!now) {
    $("permitEmpty").textContent = `Fant ikke permit_key: ${permitKey}`;
    return;
  }

  const card = $("permitCard");
  card.classList.remove("hidden");
  card.innerHTML = `
    <div><strong>${escapeHtml(now.permit_key)}</strong></div>
    <div class="muted">Snapshot: ${escapeHtml(now.snapshot_date)} • Grunnrente: ${Number(now.grunnrente_pliktig) === 1 ? "1" : "0"}</div>
    <div style="margin-top:8px">
      <div><span class="muted">Eier:</span> ${escapeHtml(now.owner_name)}</div>
      <div><span class="muted">Owner identity:</span>
        <a class="link" href="#/owner/${encodeURIComponent(now.owner_identity)}">${escapeHtml(now.owner_identity)}</a>
      </div>
      <div><span class="muted">Org.nr:</span> ${escapeHtml(now.owner_orgnr || "")}</div>
    </div>
  `;

  const hist = execAll(`
    SELECT
      valid_from,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to,
      owner_name,
      owner_orgnr,
      owner_identity
    FROM ownership_history
    WHERE permit_key = ?
    ORDER BY date(valid_from), id;
  `, [permitKey]);

  const tbody = $("permitHistoryTable").querySelector("tbody");
  tbody.innerHTML = "";

  for (const r of hist) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(r.valid_from)}</td>
      <td>${escapeHtml(r.valid_to)}</td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td><a class="link" href="#/owner/${encodeURIComponent(r.owner_identity)}">${escapeHtml(r.owner_identity)}</a></td>
      <td>${escapeHtml(r.owner_orgnr || "")}</td>
    `;
    tbody.appendChild(tr);
  }
}

// --- OWNER view ---
function renderOwner(ownerIdentity) {
  setActiveTab("tab-owner");
  showView("view-owner");

  $("ownerEmpty").textContent = "";
  $("ownerActiveTable").querySelector("tbody").innerHTML = "";
  $("ownerHistoryTable").querySelector("tbody").innerHTML = "";
  $("ownerCard").classList.add("hidden");
  if (ownerIdentity) $("ownerInput").value = ownerIdentity;

  if (!ownerIdentity) {
    $("ownerEmpty").textContent = "Skriv en owner_identity i feltet over, eller klikk en eier fra Nå-status/historikk.";
    return;
  }

  const stats = one(`
    SELECT
      owner_identity,
      MAX(owner_name) AS owner_name,
      SUM(CASE WHEN valid_to IS NULL OR valid_to = '' THEN 1 ELSE 0 END) AS active_permits,
      COUNT(*) AS total_periods
    FROM ownership_history
    WHERE owner_identity = ?
    GROUP BY owner_identity;
  `, [ownerIdentity]);

  if (!stats) {
    $("ownerEmpty").textContent = `Fant ikke owner_identity: ${ownerIdentity}`;
    return;
  }

  const card = $("ownerCard");
  card.classList.remove("hidden");
  card.innerHTML = `
    <div><strong>${escapeHtml(stats.owner_name || "(ukjent)")}</strong></div>
    <div class="muted">${escapeHtml(stats.owner_identity)}</div>
    <div style="margin-top:8px" class="muted">
      Aktive tillatelser: ${stats.active_permits} • Historiske perioder: ${stats.total_periods}
    </div>
  `;

  const active = execAll(`
    SELECT permit_key, owner_name, snapshot_date, grunnrente_pliktig
    FROM permit_current
    WHERE owner_identity = ?
    ORDER BY permit_key;
  `, [ownerIdentity]);

  const activeBody = $("ownerActiveTable").querySelector("tbody");
  for (const r of active) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${escapeHtml(r.snapshot_date)}</td>
      <td>${Number(r.grunnrente_pliktig) === 1 ? "1" : "0"}</td>
    `;
    activeBody.appendChild(tr);
  }

  const hist = execAll(`
    SELECT
      permit_key,
      valid_from,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to,
      owner_name,
      owner_orgnr
    FROM ownership_history
    WHERE owner_identity = ?
    ORDER BY date(valid_from), permit_key, id;
  `, [ownerIdentity]);

  const histBody = $("ownerHistoryTable").querySelector("tbody");
  for (const r of hist) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.valid_from)}</td>
      <td>${escapeHtml(r.valid_to)}</td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${escapeHtml(r.owner_orgnr || "")}</td>
    `;
    histBody.appendChild(tr);
  }
}

// --- routing ---
function parseHash() {
  const h = (location.hash || "#/now").replace(/^#\/?/, "");
  const parts = h.split("/").filter(Boolean);

  // "now"
  if (parts.length === 0 || parts[0] === "now") return { view: "now" };

  // "permit" or "permit/<key>"
  if (parts[0] === "permit") {
    const key = parts[1] ? decodeURIComponent(parts[1]) : null;
    return { view: "permit", key };
  }

  // "owner" or "owner/<identity>"
  if (parts[0] === "owner") {
    const ident = parts[1] ? decodeURIComponent(parts[1]) : null;
    return { view: "owner", ident };
  }

  return { view: "now" };
}

function renderRoute() {
  if (!db) return;

  const r = parseHash();
  if (r.view === "now") return renderNow();
  if (r.view === "permit") return renderPermit(r.key);
  if (r.view === "owner") return renderOwner(r.ident);
  renderNow();
}

// --- events ---
function wireEvents() {
  window.addEventListener("hashchange", () => renderRoute());

  $("nowSearch").addEventListener("input", () => renderNow());
  $("onlyGrunnrente").addEventListener("change", () => renderNow());

  $("permitGo").addEventListener("click", () => {
    const key = $("permitInput").value.trim();
    if (!key) return;
    toHashPermit(key);
  });
  $("permitInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("permitGo").click();
  });

  $("ownerGo").addEventListener("click", () => {
    const ident = $("ownerInput").value.trim();
    if (!ident) return;
    toHashOwner(ident);
  });
  $("ownerInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("ownerGo").click();
  });
}

function showError(err) {
  console.error(err);
  setStatus("Feil ved lasting", "bad");
  setMeta(String(err?.message || err));
}

(async function main() {
  wireEvents();
  if (!location.hash) toHashNow();

  try {
    await loadDatabase();
  } catch (e) {
    showError(e);
  }
})();
