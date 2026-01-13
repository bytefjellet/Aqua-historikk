/* global initSqlJs */

// =========================
// app.js (full replacement)
// =========================

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

function iso10(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (!t) return null;
  return t.slice(0, 10);
}

// --- sort state (NOW) ---
const sortState = {
  now: { key: "permit_key", dir: 1 } // dir: 1 asc, -1 desc
};

function compare(a, b) {
  if (a == null && b == null) return 0;
  if (a == null) return -1;
  if (b == null) return 1;
  const as = String(a).toLowerCase();
  const bs = String(b).toLowerCase();
  if (as < bs) return -1;
  if (as > bs) return 1;
  return 0;
}

function clearSortIndicators(tableId) {
  const ths = document.querySelectorAll(`#${tableId} thead th`);
  ths.forEach(th => th.classList.remove("sort-asc", "sort-desc"));
}

function setSortIndicator(tableId, key, dir) {
  clearSortIndicators(tableId);
  const th = document.querySelector(`#${tableId} thead th[data-sort="${CSS.escape(key)}"]`);
  if (th) th.classList.add(dir === 1 ? "sort-asc" : "sort-desc");
}

// --- load db ---
async function loadDatabase() {
  setStatus("Laster database…");
  setMeta("");

  if (!SQL) {
    SQL = await initSqlJs({
      locateFile: (f) => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${f}`,
    });
  }

  const res = await fetch(`${DB_URL}?v=${Date.now()}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Kunne ikke hente ${DB_URL} (HTTP ${res.status})`);
  const buf = await res.arrayBuffer();

  if (db) db.close();
  db = new SQL.Database(new Uint8Array(buf));

  const snap = one(`SELECT MAX(snapshot_date) AS max_date, COUNT(*) AS n FROM snapshots;`);
  const pc = one(`SELECT COUNT(*) AS n FROM permit_current;`);
  const oh = one(`SELECT COUNT(*) AS n FROM ownership_history;`);

  const last = snap?.max_date ? `Sist snapshot: ${snap.max_date}` : "Ingen snapshots";
  setStatus("DB lastet", "ok");
  setMeta(
    `${last} • permit_current: ${pc?.n ?? "?"} • ownership_history: ${oh?.n ?? "?"} • ${Math.round(buf.byteLength / 1024)} KB`
  );

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
        String(r.owner_identity ?? "").toLowerCase().includes(q) ||
        String(r.owner_orgnr ?? "").toLowerCase().includes(q)
      )
    : rows;

  $("nowSummary").textContent =
    `Viser ${filtered.length} av ${rows.length} tillatelser` + (only ? " (grunnrentepliktig)" : "");

  const tbody = $("nowTable").querySelector("tbody");
  tbody.innerHTML = "";

  const MAX = 2500;
  const displayRows = filtered.slice(0, MAX);

  for (const r of displayRows) {
    const tr = document.createElement("tr");

    const orgnrOrIdent = (r.owner_orgnr && String(r.owner_orgnr).trim())
      ? String(r.owner_orgnr).trim()
      : String(r.owner_identity ?? "");

    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.owner_name)}</td>
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
    $("permitEmpty").textContent =
      "Skriv en permit_key i feltet over, eller klikk en tillatelse fra Nå-status.";
    return;
  }

  const now = one(`
  SELECT permit_key, owner_orgnr, owner_name, owner_identity, snapshot_date, grunnrente_pliktig, art, row_json
  FROM permit_current
  WHERE permit_key = ?;
`, [permitKey]);

  const hist = execAll(`
    SELECT
      valid_from,
      valid_to,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
      owner_name,
      owner_orgnr,
      owner_identity,
      tidsbegrenset
    FROM ownership_history
    WHERE permit_key = ?
    ORDER BY date(valid_from), id;
  `, [permitKey]);

  if (!now && hist.length === 0) {
    $("permitEmpty").textContent = `Fant ikke permit_key: ${permitKey}`;
    return;
  }

  const card = $("permitCard");
  card.classList.remove("hidden");

  if (now) {
  // art: bruk aggregert DB-kolonne hvis den finnes, ellers fallback til row_json["ART"]
  let rowDict = {};
  try { rowDict = now.row_json ? JSON.parse(now.row_json) : {}; } catch (e) { rowDict = {}; }

  const artText = (now.art && String(now.art).trim()) ? now.art : (rowDict["ART"] ?? "");
  const artHtml = artText ? escapeHtml(artText).replaceAll(", ", "<br>") : "";

  card.innerHTML = `
    <div><strong>${escapeHtml(now.permit_key)}</strong></div>
    <div class="muted">
      Snapshot: ${escapeHtml(now.snapshot_date)} • Grunnrente: ${Number(now.grunnrente_pliktig) === 1 ? "1" : "0"}
    </div>

    <div style="margin-top:8px">
      <div><span class="muted">Eier:</span> ${escapeHtml(now.owner_name)}</div>
      <div><span class="muted">Owner identity:</span>
        <a class="link" href="#/owner/${encodeURIComponent(now.owner_identity)}">${escapeHtml(now.owner_identity)}</a>
      </div>
      <div><span class="muted">Org.nr:</span> ${escapeHtml(now.owner_orgnr || "")}</div>
      ${artText ? `<div style="margin-top:8px"><span class="muted">Arter:</span><div>${artHtml}</div></div>` : ""}
    </div>
  `;
} else {
    // not active in permit_current => show last known from history
    const last = hist[hist.length - 1];
    const lastTo = iso10(last.valid_to);
    const tb = iso10(last.tidsbegrenset);

    let endText = "Ikke aktiv";
    if (lastTo) {
      endText = (tb && tb === lastTo)
        ? `Utløpt (tidsbegrenset ${tb})`
        : `Avsluttet (${lastTo})`;
    }

    const maxSnap = one(`SELECT MAX(snapshot_date) AS max_date FROM snapshots;`);
    const maxDate = maxSnap?.max_date ? String(maxSnap.max_date) : "";

    // Hent sist kjente art fra permit_snapshot (lagret ved build) – også for utløpte tillatelser
    const lastSnap = one(`
      SELECT art
      FROM permit_snapshot
      WHERE permit_key = ?
      ORDER BY snapshot_date DESC
      LIMIT 1;
    `, [permitKey]);

    const artText = (lastSnap?.art && String(lastSnap.art).trim()) ? lastSnap.art : "";
    const artHtml = artText ? escapeHtml(artText).replaceAll(", ", "<br>") : "";


    card.innerHTML = `
      <div><strong>${escapeHtml(permitKey)}</strong></div>
      <div class="muted">Ikke aktiv i siste snapshot${maxDate ? ` (${escapeHtml(maxDate)})` : ""} • ${escapeHtml(endText)}</div>
      <div style="margin-top:8px">
        <div><span class="muted">Siste kjente eier:</span> ${escapeHtml(last.owner_name || "")}</div>
        <div><span class="muted">Owner identity:</span>
          <a class="link" href="#/owner/${encodeURIComponent(last.owner_identity)}">${escapeHtml(last.owner_identity || "")}</a>
        </div>
        <div><span class="muted">Org.nr:</span> ${escapeHtml(last.owner_orgnr || "")}</div>
        ${tb ? `<div><span class="muted">Tidsbegrenset:</span> ${escapeHtml(tb)}</div>` : ""}
        ${artText ? `<div style="margin-top:8px"><span class="muted">Arter:</span><div>${artHtml}</div></div>` : ""}
      </div>
    `;
  }

  // render history table
  const tbody = $("permitHistoryTable").querySelector("tbody");
  tbody.innerHTML = "";

  for (let i = 0; i < hist.length; i++) {
    const r = hist[i];
    const next = hist[i + 1] || null;

    const validTo = iso10(r.valid_to);
    const tb = iso10(r.tidsbegrenset);

    let reason = "";
    if (!validTo) {
      reason = "";
    } else if (tb && tb === validTo) {
      reason = `Utløpt (tidsbegrenset ${tb})`;
    } else if (next) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(r.valid_from)}</td>
      <td>${escapeHtml(r.valid_to_label)}</td>
      <td class="muted">${escapeHtml(reason)}</td>
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
    $("ownerEmpty").textContent =
      "Skriv en owner_identity i feltet over, eller klikk en eier fra Nå-status/historikk.";
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
    SELECT permit_key, art, row_json
    FROM permit_current
    WHERE owner_identity = ?
    ORDER BY permit_key;
  `, [ownerIdentity]);


const activeBody = $("ownerActiveTable").querySelector("tbody");
activeBody.innerHTML = "";

for (const r of active) {
  const rowDict = r.row_json ? (() => { try { return JSON.parse(r.row_json); } catch { return {}; } })() : {};

  const art = (r.art && String(r.art).trim()) ? r.art : (rowDict["ART"] ?? "");
  const formal = rowDict["FORMÅL"] ?? "";
  const produksjonsform = rowDict["PRODUKSJONSFORM"] ?? "";
  const kap = rowDict["TILL_KAP"] ?? "";
  const enh = rowDict["TILL_ENHET"] ?? "";
  const prodOmr = rowDict["PROD_OMR"] ?? "";

  const kapasitet = String(kap).trim()
    ? `${String(kap).trim()}${String(enh).trim() ? " " + String(enh).trim() : ""}`
    : "";

  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${escapeHtml(r.permit_key)}</a></td>
    <td>${escapeHtml(art).replaceAll(", ", "<br>")}</td>
    <td>${escapeHtml(formal)}</td>
    <td>${escapeHtml(produksjonsform)}</td>
    <td>${escapeHtml(kapasitet)}</td>
    <td>${escapeHtml(prodOmr)}</td>
  `;
  activeBody.appendChild(tr);
}



  const hist = execAll(`
    SELECT
      permit_key,
      valid_from,
      valid_to,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
      owner_name,
      owner_orgnr,
      tidsbegrenset
    FROM ownership_history
    WHERE owner_identity = ?
    ORDER BY permit_key, date(valid_from), id;
  `, [ownerIdentity]);

  const histBody = $("ownerHistoryTable").querySelector("tbody");
  histBody.innerHTML = "";

  for (let i = 0; i < hist.length; i++) {
    const r = hist[i];
    const next = hist[i + 1] || null;

    const validTo = iso10(r.valid_to);
    const tb = iso10(r.tidsbegrenset);
    const hasNextSamePermit = Boolean(next && next.permit_key === r.permit_key);

    let reason = "";
    if (!validTo) {
      reason = "";
    } else if (tb && tb === validTo) {
      reason = `Utløpt (tidsbegrenset ${tb})`;
    } else if (hasNextSamePermit) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(r.permit_key)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.valid_from)}</td>
      <td>${escapeHtml(r.valid_to_label)}</td>
      <td class="muted">${escapeHtml(reason)}</td>
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

  if (parts.length === 0 || parts[0] === "now") return { view: "now" };

  if (parts[0] === "permit") {
    const key = parts[1] ? decodeURIComponent(parts[1]) : null;
    return { view: "permit", key };
  }

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

  // NOW search (debounced)
  let nowTimer = null;
  $("nowSearch").addEventListener("input", () => {
    clearTimeout(nowTimer);
    nowTimer = setTimeout(() => renderNow(), 80);
  });
  $("onlyGrunnrente").addEventListener("change", () => renderNow());

  // NOW sorting via data-sort headers (safe even if headers don't have data-sort)
  document.querySelectorAll("#nowTable thead th[data-sort]").forEach(th => {
    th.addEventListener("click", () => {
      const key = th.getAttribute("data-sort");
      if (!key) return;

      if (sortState.now.key === key) sortState.now.dir *= -1;
      else { sortState.now.key = key; sortState.now.dir = 1; }

      renderNow();
    });
  });

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

// --- main ---
(async function main() {
  wireEvents();
  if (!location.hash) toHashNow();

  try {
    await loadDatabase();
  } catch (e) {
    showError(e);
  }
})();
