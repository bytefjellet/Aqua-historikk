/* global initSqlJs */

// =========================
// app.js (FULL REPLACEMENT)
// - Robust permit-søk (inkl. historiske): normalisering + rydd først + vis resultater kun når data finnes
// - Unified permit card: samme infokort for aktive og historiske tillatelser
// - Viser "Tidsbegrenset" i infokortet når ownership_history.tidsbegrenset finnes
// - Owner (Unified owner card):
//   * Ingen klikkbar lenke i overskriften
//   * Ingen piller i eierkort
//   * Riktige tellinger (aktive, tidligere unike ikke-lenger-aktive, grunnrente aktive)
// - Owner empty-state:
//   * Tom-tilstand skjules når treff vises (slik som Permit)
//   * Permit-lik feilmelding ved ugyldig org.nr (9 siffer uten mellomrom)
// =========================

let SQL = null;
let db = null;

const DB_URL = "data/aqua.sqlite";

// Schema flags (settes etter DB lastes)
const schema = {
  permit_current_has_art: false,
  permit_snapshot_has_art: false,
  permit_snapshot_has_row_json: false,
  permit_snapshot_has_grunnrente: false,
};
// --- Owner filter state ---
const ownerFilters = {
  formal: null, // valgt formål (string) eller null
};

// Liste over alle formål i DB (fra permit_current)
let allFormals = []; // array av strings

// --- helpers ---
function $(id) { return document.getElementById(id); }

function safeEl(id) {
  const el = $(id);
  if (!el) throw new Error(`Mangler element i HTML: #${id}`);
  return el;
}

function setStatus(text, kind) {
  const el = safeEl("dbStatus");
  el.textContent = text;

  el.classList.add("pill");
  el.classList.remove("ok", "warn", "bad");
  if (kind) el.classList.add(kind);
}

function setMeta(text) {
  safeEl("dbMeta").textContent = text || "";
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

function normalizePermitKey(raw) {
  return (raw ?? "")
    .toString()
    .trim()
    .replace(/\s+/g, "")
    .toUpperCase();
}

function toHashPermit(key) {
  const norm = normalizePermitKey(key);
  location.hash = `#/permit/${encodeURIComponent(norm)}`;
}

function toHashOwner(identity) {
  const norm = String(identity ?? "").trim().replace(/\s+/g, "");
  location.hash = `#/owner/${encodeURIComponent(norm)}`;
}

function setActiveTab(tabId) {
  for (const id of ["tab-now", "tab-permit", "tab-owner"]) {
    const el = $(id);
    if (!el) continue;
    el.classList.toggle("active", id === tabId);
  }
}

function showView(viewId) {
  for (const id of ["view-now", "view-permit", "view-owner"]) {
    const el = $(id);
    if (!el) continue;
    el.style.display = (id === viewId) ? "block" : "none";
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

function displayDate(s) {
  if (s == null) return "";
  return iso10(s) || String(s);
}

function formatNorwegianDate(isoDate) {
  const d = new Date(isoDate + "T00:00:00");
  return new Intl.DateTimeFormat("nb-NO", {
    day: "numeric",
    month: "long",
    year: "numeric"
  }).format(d);
}

function hasColumn(table, col) {
  const rows = execAll(`PRAGMA table_info(${table});`);
  return rows.some(r => String(r.name) === col);
}

function isNineDigits(s) {
  return /^[0-9]{9}$/.test(String(s || "").trim().replace(/\s+/g, ""));
}

function parseJsonSafe(s) {
  try { return s ? JSON.parse(s) : {}; } catch { return {}; }
}

// --- Owner formål filter buttons ---
function renderOwnerFormalButtons(countsByFormal) {
  const root = $("ownerFormalFilters");
  if (!root) return;

  root.innerHTML = "";

  for (const formal of allFormals) {
    const count = Number(countsByFormal.get(formal) || 0);

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "filter-btn";
    if (count === 0) btn.classList.add("zero");
    if (ownerFilters.formal === formal) btn.classList.add("active");

    btn.textContent = `${formal} (${count})`;

    btn.addEventListener("click", () => {
      ownerFilters.formal =
        ownerFilters.formal === formal ? null : formal;

      const r = parseHash();
      if (r.view === "owner") renderOwner(r.ident);
    });

    root.appendChild(btn);
  }
}

function extractFormalFromRowJson(rowJsonText) {
  const d = parseJsonSafe(rowJsonText);
  return String(d["FORMÅL"] ?? "").trim();
}

function valueOrDash(v) {
  const t = String(v ?? "").trim();
  return t ? t : "—";
}

function formatKapNoTrailing00(kapRaw) {
  const t = String(kapRaw ?? "").trim();
  if (!t) return "";

  // normaliser til punktum for parsing
  const s = t.replace(",", ".");
  if (!/^-?\d+(\.\d+)?$/.test(s)) return t;

  const num = Number(s);
  if (!Number.isFinite(num)) return t;

  // Hvis heltall (inkl. .00 / ,00) -> ingen desimaler
  if (Number.isInteger(num)) return String(num);

  // Ellers: fjern trailing nuller og bytt tilbake til komma
  let out = String(num);
  // noen ganger kan String(num) gi vitenskapelig notasjon; fallback:
  if (out.includes("e") || out.includes("E")) out = num.toFixed(2);

  out = out.replace(/(\.\d*?[1-9])0+$/u, "$1").replace(/\.0+$/u, "");
  return out.replace(".", ",");
}

// --- PERMIT empty state helpers ---
function setPermitEmptyStateVisible(visible) {
  const el = $("permitEmptyState");
  if (el) el.classList.toggle("hidden", !visible);
}

function setPermitEmptyStateContent({ icon, title, text }) {
  const root = $("permitEmptyState");
  if (!root) return;

  const iconEl = root.querySelector(".empty-icon");
  const titleEl = root.querySelector(".empty-title");
  const textEl  = root.querySelector(".empty-text");

  if (iconEl && icon != null) iconEl.textContent = icon;
  if (titleEl && title != null) titleEl.textContent = title;
  if (textEl  && text  != null) textEl.textContent  = text;
}

function setPermitResultsVisible(visible) {
  const split = safeEl("view-permit").querySelector(".split");
  if (split) split.classList.toggle("hidden", !visible);
}

// --- OWNER empty state helpers (lik permit) ---
function setOwnerEmptyStateVisible(visible) {
  const el = $("ownerEmptyState");
  if (el) el.classList.toggle("hidden", !visible);
}

function setOwnerEmptyStateContent({ icon, title, text }) {
  const root = $("ownerEmptyState");
  if (!root) return;

  const iconEl = root.querySelector(".empty-icon");
  const titleEl = root.querySelector(".empty-title");
  const textEl  = root.querySelector(".empty-text");

  if (iconEl && icon != null) iconEl.textContent = icon;
  if (titleEl && title != null) titleEl.textContent = title;
  if (textEl  && text  != null) textEl.textContent  = text;
}

function setOwnerResultsVisible(visible) {
  const root = $("ownerResults");
  if (root) root.classList.toggle("hidden", !visible);
}


// --- clear helpers ---
function clearPermitView() {
  setPermitResultsVisible(false);
  setPermitEmptyStateVisible(true);

  const empty = $("permitEmpty");
  if (empty) empty.textContent = "";

  const card = $("permitCard");
  if (card) card.classList.add("hidden");

  const tbody = safeEl("permitHistoryTable").querySelector("tbody");
  if (!tbody) throw new Error("Mangler <tbody> i #permitHistoryTable");
  tbody.innerHTML = "";

  const reasonTh = $("permitReasonTh");
  if (reasonTh) reasonTh.style.display = "";
}

function clearOwnerView() {
  setOwnerResultsVisible(false);
  setOwnerEmptyStateVisible(true);

  safeEl("ownerEmpty").textContent = "";
  safeEl("ownerCard").classList.add("hidden");

  const a = safeEl("ownerActiveTable").querySelector("tbody");
  const h = safeEl("ownerHistoryTable").querySelector("tbody");
  if (!a) throw new Error("Mangler <tbody> i #ownerActiveTable");
  if (!h) throw new Error("Mangler <tbody> i #ownerHistoryTable");
  a.innerHTML = "";
  h.innerHTML = "";
}

// --- UNIFIED permit card renderer (ACTIVE + HISTORIC) ---
function renderPermitCardUnified({
  permitKey,
  permitUrl,
  isActive,
  subline,
  ownerName,
  ownerIdentity,
  grunnrenteValue,
  artText,
  formal,
  produksjonsstadium,
  kapasitet,
  prodOmr,
  tidsbegrenset,
}) {
  const card = safeEl("permitCard");
  card.classList.remove("hidden");

  const statusPillClass = isActive ? "pill--green" : "pill--yellow";
  const statusPillText  = isActive ? "Aktiv tillatelse" : "Historisk";

  let grunnPillClass = "pill--yellow";
  let grunnPillText = "Grunnrente: ukjent";

  if (grunnrenteValue === true) {
    grunnPillClass = "pill--blue";
    grunnPillText = "Grunnrentepliktig";
  } else if (grunnrenteValue === false) {
    grunnPillClass = "pill--yellow";
    grunnPillText = "Ikke grunnrentepliktig";
  }

  const identRaw = String(ownerIdentity ?? "").trim();
  const ident = identRaw.replace(/\s+/g, "");

  card.innerHTML = `
    <div>
      <a href="${permitUrl}" target="_blank" rel="noopener noreferrer" class="permit-title-link">
        ${escapeHtml(permitKey)}
      </a>
    </div>

    <div class="pills">
      <span class="pill ${statusPillClass}">${escapeHtml(statusPillText)}</span>
      <span class="pill ${grunnPillClass}">${escapeHtml(grunnPillText)}</span>
    </div>

    ${subline ? `<div class="muted" style="margin-top:6px">${escapeHtml(subline)}</div>` : ""}

    <div style="margin-top:10px">
      <div><span class="muted">Eier:</span> ${escapeHtml(valueOrDash(ownerName))}</div>
      <div><span class="muted">Org.nr.:</span>
        ${ident ? `<a class="link" href="#/owner/${encodeURIComponent(ident)}">${escapeHtml(ident)}</a>` : "—"}
      </div>

      ${tidsbegrenset ? `<div style="margin-top:8px"><span class="muted">Tidsbegrenset:</span> ${escapeHtml(tidsbegrenset)}</div>` : ""}

      ${artText ? `<div style="margin-top:8px"><span class="muted">Arter:</span> ${escapeHtml(artText)}</div>` : ""}

      <div style="margin-top:10px">
        <div><span class="muted">Formål:</span> ${escapeHtml(valueOrDash(formal))}</div>
        <div><span class="muted">Produksjonsstadium:</span> ${escapeHtml(valueOrDash(produksjonsstadium))}</div>
        <div><span class="muted">Tillatelseskapasitet:</span> ${escapeHtml(valueOrDash(kapasitet))}</div>
        <div><span class="muted">Produksjonsområde:</span> ${escapeHtml((String(prodOmr ?? "").trim() || "N/A"))}</div>
      </div>
    </div>
  `;
}


// --- UNIFIED owner card renderer ---
function renderOwnerCardUnified({
  ownerName,
  ownerIdentity,
  activeCount,
  grunnrenteActiveCount,
  formerPermitCount
}) {
  const card = safeEl("ownerCard");
  card.classList.remove("hidden");

  const name = valueOrDash(ownerName);
  const ident = String(ownerIdentity ?? "").trim();

  const grunnCount = Number(grunnrenteActiveCount ?? 0);
  const grunnPillHtml = grunnCount > 0
    ? `<span class="pill pill--blue">Grunnrentepliktig</span>`
    : `<span class="pill pill--yellow">Ikke grunnrentepliktig</span>`;

  card.innerHTML = `
    <div style="font-size:1.1rem;font-weight:700">
      ${escapeHtml(name)}
    </div>

    <div class="pills" style="margin-top:8px">
      ${grunnPillHtml}
    </div>

    <div style="margin-top:10px">
      <div><span class="muted">Org.nr.:</span> ${escapeHtml(ident || "—")}</div>

      <div style="margin-top:10px">
        <div><span class="muted">Aktive tillatelser:</span> ${escapeHtml(String(activeCount ?? 0))}</div>
        <div><span class="muted">Grunnrentepliktige tillatelser:</span> ${escapeHtml(String(grunnrenteActiveCount ?? 0))}</div>
        <div><span class="muted">Historiske tillatelser:</span> ${escapeHtml(String(formerPermitCount ?? 0))}</div>
      </div>
    </div>
  `;
}

// --- sort state (NOW) ---
const sortState = {
  now: { key: "permit_key", dir: 1 }
};

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

    schema.permit_current_has_art = hasColumn("permit_current", "art");
  schema.permit_snapshot_has_art = hasColumn("permit_snapshot", "art");
  schema.permit_snapshot_has_row_json = hasColumn("permit_snapshot", "row_json");
  schema.permit_snapshot_has_grunnrente = hasColumn("permit_snapshot", "grunnrente_pliktig");

  // Bygg liste over alle formål i databasen (fra permit_current.row_json)
  try {
    const rows = execAll(`SELECT row_json AS row_json FROM permit_current;`);
    const set = new Set();
    for (const r of rows) {
      const f = extractFormalFromRowJson(r.row_json);
      if (f) set.add(f);
    }
    allFormals = Array.from(set).sort((a, b) =>
      a.localeCompare(b, "nb", { sensitivity: "base" })
    );
  } catch (e) {
    console.warn("Kunne ikke bygge formål-liste:", e);
    allFormals = [];
  }

  const snap = one(`SELECT MAX(snapshot_date) AS max_date, COUNT(*) AS n FROM snapshots;`);

  setStatus("DB lastet", "ok");

  const snapIso = snap?.max_date ? String(snap.max_date).slice(0, 10) : "";
  const snapNo = snapIso ? formatNorwegianDate(snapIso) : "";

  setMeta(
    snapNo
      ? `Visningen senest oppdatert mot Akvakulturregisteret: ${snapNo}`
      : "Visningen senest oppdatert mot Akvakulturregisteret: (ukjent dato)"
  );

  renderRoute();
}

// --- NOW view ---
function renderNow() {
  setActiveTab("tab-now");
  showView("view-now");

  const q = safeEl("nowSearch").value.trim().toLowerCase();
  const only = safeEl("onlyGrunnrente").checked;

  const rows = execAll(`
    SELECT
      permit_key AS permit_key,
      owner_name AS owner_name,
      owner_identity AS owner_identity,
      owner_orgnr AS owner_orgnr,
      grunnrente_pliktig AS grunnrente_pliktig
    FROM permit_current
    ${only ? "WHERE grunnrente_pliktig = 1" : ""}
  `);

  const filtered = q
    ? rows.filter(r =>
        String(r.permit_key ?? "").toLowerCase().includes(q) ||
        String(r.owner_name ?? "").toLowerCase().includes(q) ||
        String(r.owner_identity ?? "").toLowerCase().includes(q) ||
        String(r.owner_orgnr ?? "").toLowerCase().includes(q)
      )
    : rows;

  const { key, dir } = sortState.now;
  filtered.sort((a, b) => {
    const av = String(a[key] ?? "");
    const bv = String(b[key] ?? "");
    return av.localeCompare(bv, "nb", { numeric: true, sensitivity: "base" }) * dir;
  });

  safeEl("nowSummary").textContent =
    `Antall tillatelser i visningen: ${filtered.length}` + (only ? " (grunnrentepliktig)" : "");

  const tbody = safeEl("nowTable").querySelector("tbody");
  if (!tbody) throw new Error("Mangler <tbody> i #nowTable");
  tbody.innerHTML = "";

  const MAX = 2500;
  const displayRows = filtered.slice(0, MAX);

  for (const r of displayRows) {
    const tr = document.createElement("tr");

    const orgnrOrIdent = (r.owner_orgnr && String(r.owner_orgnr).trim())
      ? String(r.owner_orgnr).trim()
      : String(r.owner_identity ?? "");

    const ownerIdent = (r.owner_identity && String(r.owner_identity).trim())
      ? String(r.owner_identity).trim()
      : "";

    const ownerIdentNorm = ownerIdent.replace(/\s+/g, "");
    const permitHrefKey = normalizePermitKey(r.permit_key);

    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(permitHrefKey)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${
        ownerIdentNorm
          ? `<a class="link" href="#/owner/${encodeURIComponent(ownerIdentNorm)}">${escapeHtml(orgnrOrIdent)}</a>`
          : `${escapeHtml(orgnrOrIdent)}`
      }</td>
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

  setPermitEmptyStateContent({
    icon: "🔍",
    title: "Søk etter tillatelse",
    text: "Skriv et tillatelsesnummer i feltet over for å se detaljer og historikk."
  });

  clearPermitView();

  if (permitKey != null) safeEl("permitInput").value = String(permitKey);

  const inputEl = safeEl("permitInput");
  const raw = String(permitKey || "");
  const norm = normalizePermitKey(raw);

  if (!norm) return;

  if (isNineDigits(raw)) {
    const msg = "Dette ser ut som et org.nr. (9 siffer). Bruk fanen Innehaver for org.nr., eller skriv et tillatelsesnr. (f.eks. H-F-0910).";
    const legacy = $("permitEmpty");
    if (legacy) legacy.textContent = msg;

    setPermitEmptyStateContent({
      icon: "ℹ️",
      title: "Dette ser ut som org.nr.",
      text: msg
    });
    setPermitEmptyStateVisible(true);
    return;
  }

  permitKey = norm;
  inputEl.value = permitKey;

  const permitUrl =
    `https://sikker.fiskeridir.no/akvakulturregisteret/web/licenses/${encodeURIComponent(permitKey)}`;

  const now = one(`
    SELECT
      permit_key AS permit_key,
      owner_name AS owner_name,
      owner_identity AS owner_identity,
      grunnrente_pliktig AS grunnrente_pliktig,
      ${schema.permit_current_has_art ? "art AS art" : "NULL AS art"},
      row_json AS row_json
    FROM permit_current
    WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?;
  `, [permitKey]);

  const hist = execAll(`
    SELECT
      valid_from AS valid_from,
      valid_to   AS valid_to,
      COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
      owner_name AS owner_name,
      owner_identity AS owner_identity,
      tidsbegrenset AS tidsbegrenset
    FROM ownership_history
    WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?
    ORDER BY date(valid_from), id;
  `, [permitKey]);

  if (!now && hist.length === 0) {
    setPermitEmptyStateContent({
      icon: "⚠️",
      title: "Ingen tillatelser funnet",
      text: `Fant ingen tillatelser med nummeret ${permitKey}.`
    });
    setPermitEmptyStateVisible(true);
    return;
  }

  setPermitResultsVisible(true);
  setPermitEmptyStateVisible(false);

  // Skal vi vise Årsak-kolonnen?
  let showReasonColumn = false;
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

    if (reason) { showReasonColumn = true; break; }
  }

  const reasonTh = $("permitReasonTh");
  if (reasonTh) reasonTh.style.display = showReasonColumn ? "" : "none";

  // Tidsbegrenset for kort: siste ikke-tomme i historikk
  let tidsbegrensetCard = "";
  for (let i = hist.length - 1; i >= 0; i--) {
    const tb = iso10(hist[i].tidsbegrenset);
    if (tb) { tidsbegrensetCard = tb; break; }
  }
  const tidsbegrensetCardDisplay = tidsbegrensetCard
    ? formatNorwegianDate(tidsbegrensetCard)
    : "";

  if (now) {
    const rowDict = parseJsonSafe(now.row_json);

    const artText = (now.art && String(now.art).trim())
      ? String(now.art).trim()
      : String(rowDict["ART"] ?? "").trim();

    const formal = String(rowDict["FORMÅL"] ?? "").trim();
    const produksjonsstadium = String(rowDict["PRODUKSJONSSTADIUM"] ?? rowDict["PRODUKSJONSFORM"] ?? "").trim();

    const kapRaw = String(rowDict["TILL_KAP"] ?? "").trim();
    const enh = String(rowDict["TILL_ENHET"] ?? "").trim();

    let kap = kapRaw;
    if (kapRaw && /^-?\d+([.,]\d+)?$/.test(kapRaw)) {
      const num = Number(kapRaw.replace(",", "."));
      if (Number.isFinite(num)) {
        kap = Number.isInteger(num) ? String(Math.trunc(num)) : String(num).replace(".", ",");
      }
    }
    const kapasitet = kap ? `${kap}${enh ? " " + enh : ""}` : "";

    const prodOmrRaw = String(rowDict["PROD_OMR"] ?? "").trim();
    const prodOmr = prodOmrRaw ? prodOmrRaw : "N/A";

    const grunnrente = Number(now.grunnrente_pliktig) === 1;

    renderPermitCardUnified({
      permitKey: String(now.permit_key ?? permitKey),
      permitUrl,
      isActive: true,
      subline: "",
      ownerName: now.owner_name ?? "",
      ownerIdentity: now.owner_identity ?? "",
      grunnrenteValue: grunnrente,
      artText,
      formal,
      produksjonsstadium,
      kapasitet,
      prodOmr,
      tidsbegrenset: tidsbegrensetCardDisplay,
    });
  } else {
    const last = hist[hist.length - 1];

    // Hent siste snapshot for ekstra detaljer (hvis mulig)
    let snapRow = null;
    if (schema.permit_snapshot_has_row_json || schema.permit_snapshot_has_art || schema.permit_snapshot_has_grunnrente) {
      const cols = [
        "snapshot_date AS snapshot_date",
        schema.permit_snapshot_has_row_json ? "row_json AS row_json" : "NULL AS row_json",
        schema.permit_snapshot_has_art ? "art AS art" : "NULL AS art",
        schema.permit_snapshot_has_grunnrente ? "grunnrente_pliktig AS grunnrente_pliktig" : "NULL AS grunnrente_pliktig",
      ].join(", ");

      snapRow = one(`
        SELECT ${cols}
        FROM permit_snapshot
        WHERE UPPER(REPLACE(TRIM(permit_key), ' ', '')) = ?
        ORDER BY snapshot_date DESC
        LIMIT 1;
      `, [permitKey]);
    }

    const snapDict = parseJsonSafe(snapRow?.row_json);

    const artText = String((snapRow?.art ?? snapDict["ART"] ?? "")).trim();
    const formal = String(snapDict["FORMÅL"] ?? "").trim();
    const produksjonsstadium = String(snapDict["PRODUKSJONSSTADIUM"] ?? snapDict["PRODUKSJONSFORM"] ?? "").trim();

    const kapRaw = String(snapDict["TILL_KAP"] ?? "").trim();
    const enh = String(snapDict["TILL_ENHET"] ?? "").trim();

    let kap = kapRaw;
    if (kapRaw && /^-?\d+([.,]\d+)?$/.test(kapRaw)) {
      const num = Number(kapRaw.replace(",", "."));
      if (Number.isFinite(num)) {
        kap = Number.isInteger(num) ? String(Math.trunc(num)) : String(num).replace(".", ",");
      }
    }
    const kapasitet = kap ? `${kap}${enh ? " " + enh : ""}` : "";

    const prodOmrRaw = String(snapDict["PROD_OMR"] ?? "").trim();
    const prodOmr = prodOmrRaw ? prodOmrRaw : "N/A";

    let grunnrenteValue = false;
    if (snapRow && snapRow.grunnrente_pliktig != null && snapRow.grunnrente_pliktig !== "") {
      grunnrenteValue = Number(snapRow.grunnrente_pliktig) === 1;
    }

    renderPermitCardUnified({
      permitKey,
      permitUrl,
      isActive: false,
      subline: "",
      ownerName: last.owner_name ?? "",
      ownerIdentity: last.owner_identity ?? "",
      grunnrenteValue,
      artText,
      formal,
      produksjonsstadium,
      kapasitet,
      prodOmr,
      tidsbegrenset: tidsbegrensetCardDisplay,
    });
  }

  // --- Render history table ---
  const tbody = safeEl("permitHistoryTable").querySelector("tbody");
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
    if (!reason) reason = "--";

    const vf = displayDate(r.valid_from);
    const vtLabel = (r.valid_to_label === "Aktiv") ? "Aktiv" : displayDate(r.valid_to_label);

    const identRaw = String(r.owner_identity ?? "").trim();
    const ident = identRaw.replace(/\s+/g, "");

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(vf)}</td>
      <td>${escapeHtml(vtLabel)}</td>
      ${showReasonColumn ? `<td class="muted">${escapeHtml(reason)}</td>` : ""}
      <td>${escapeHtml(r.owner_name || "")}</td>
      <td>${ident ? `<a class="link" href="#/owner/${encodeURIComponent(ident)}">${escapeHtml(ident)}</a>` : "—"}</td>
    `;
    tbody.appendChild(tr);
  }
}

let selectedOwnerFormal = null;

function renderOwnerFormalButtons(countsByFormal) {
  const root = $("ownerFormalFilters");
  if (!root) return;

  root.innerHTML = "";

  for (const formal of allFormals) {
    const count = Number(countsByFormal.get(formal) || 0);

    const btn = document.createElement("button");
    btn.type = "button";

    // ✅ RIKTIGE KLASSER
    btn.className = "filter-btn";
    if (count === 0) btn.classList.add("zero");
    if (ownerFilters.formal === formal) btn.classList.add("active");

    btn.textContent = `${formal} (${count})`;

    btn.addEventListener("click", () => {
      // toggle
      ownerFilters.formal =
        ownerFilters.formal === formal ? null : formal;

      const r = parseHash();
      if (r.view === "owner") renderOwner(r.ident);
    });

    root.appendChild(btn);
  }
}


// --- OWNER view ---
function renderOwner(ownerIdentity) {
  console.log("renderOwner()", ownerIdentity);

  setActiveTab("tab-owner");
  showView("view-owner");

  // Default tom-tilstand (lik permit)
  setOwnerEmptyStateContent({
    icon: "🔍",
    title: "Søk etter innehaver",
    text: "Skriv et org.nr. (9 siffer) i feltet over for å se aktive tillatelser og historikk."
  });

  clearOwnerView();

  if (ownerIdentity != null) safeEl("ownerInput").value = String(ownerIdentity);

  const inputEl = safeEl("ownerInput");
  const raw = String(ownerIdentity || "");
  const identTrim = raw.trim();

  // Ingen input -> bare tom-tilstand
  if (!identTrim) {
    setOwnerEmptyStateVisible(true);
    setOwnerResultsVisible(false);
    return;
  }

  // Ugyldig input -> permit-lik feilmelding
  if (!/^[0-9]{9}$/.test(identTrim)) {
    setOwnerEmptyStateContent({
      icon: "⚠️",
      title: "Ugyldig org.nr.",
      text: "Org.nr. må være et tall på 9 siffer uten mellomrom."
    });
    setOwnerEmptyStateVisible(true);
    setOwnerResultsVisible(false);
    return;
  }

  const ownerIdentityNorm = identTrim;
  inputEl.value = ownerIdentityNorm;

  // --- Owner stats: aktive (permit_current) + tidligere (unike permit_key som ikke lenger er aktive) ---
  const stats = one(`
    WITH owner_rows AS (
      SELECT
        permit_key,
        owner_name,
        REPLACE(TRIM(owner_identity), ' ', '') AS owner_identity,
        valid_to
      FROM ownership_history
      WHERE REPLACE(TRIM(owner_identity), ' ', '') = ?
    ),
    per_permit AS (
      SELECT
        permit_key,
        MAX(owner_name) AS owner_name,
        SUM(CASE WHEN valid_to IS NULL OR TRIM(valid_to) = '' THEN 1 ELSE 0 END) AS has_active,
        SUM(CASE WHEN valid_to IS NOT NULL AND TRIM(valid_to) <> '' THEN 1 ELSE 0 END) AS has_ended
      FROM owner_rows
      GROUP BY permit_key
    )
    SELECT
      ? AS owner_identity,
      MAX(owner_name) AS owner_name,
      (SELECT COUNT(*) FROM permit_current WHERE REPLACE(TRIM(owner_identity), ' ', '') = ?) AS active_permits,
      SUM(CASE WHEN has_active = 0 AND has_ended > 0 THEN 1 ELSE 0 END) AS former_permits
    FROM per_permit;
  `, [ownerIdentityNorm, ownerIdentityNorm, ownerIdentityNorm]);

  if (!stats || (
    Number(stats.active_permits ?? 0) === 0 &&
    Number(stats.former_permits ?? 0) === 0
  )) {
    setOwnerEmptyStateContent({
      icon: "⚠️",
      title: "Ingen treff",
      text: `Fant ingen informasjon for org.nr. ${ownerIdentityNorm}.`
    });
    setOwnerEmptyStateVisible(true);
    setOwnerResultsVisible(false);
    return;
  }

  // --- Active permits (for table + grunnrenteActiveCount) ---
  const active = execAll(`
    SELECT
      permit_key AS permit_key,
      ${schema.permit_current_has_art ? "art AS art" : "NULL AS art"},
      row_json AS row_json,
      grunnrente_pliktig AS grunnrente_pliktig
    FROM permit_current
    WHERE REPLACE(TRIM(owner_identity), ' ', '') = ?
    ORDER BY permit_key;
  `, [ownerIdentityNorm]);

  // (valgfritt) denne trenger du ikke lenger hvis du bruker parseHash i click handler
  // window.__currentOwnerIdentity = ownerIdentityNorm;

  // grunnrente-filter for aktiv-tabellen
  const onlyGrunnrente = $("ownerOnlyGrunnrente")?.checked === true;
  const activeAfterGrunnrente = onlyGrunnrente
    ? active.filter(r => Number(r.grunnrente_pliktig) === 1)
    : active;

  // counts per formål (til knappene) — bygges fra activeAfterGrunnrente
  const countsByFormal = new Map();
  for (const r of activeAfterGrunnrente) {
    const d = parseJsonSafe(r.row_json);
    const f = String(d["FORMÅL"] ?? "").trim();
    if (!f) continue;
    countsByFormal.set(f, (countsByFormal.get(f) || 0) + 1);
  }

  // Render formål-knapper
  renderOwnerFormalButtons(countsByFormal);

  // formål-filter
  const selectedFormal = ownerFilters.formal;
  const activeDisplay = selectedFormal
    ? activeAfterGrunnrente.filter(r => {
        const d = parseJsonSafe(r.row_json);
        return String(d["FORMÅL"] ?? "").trim() === selectedFormal;
      })
    : activeAfterGrunnrente;

  // tom-melding når filter gir 0 treff
  if (activeDisplay.length === 0) {
    setOwnerActiveEmptyMessage("Ingen tillatelser funnet");
  } else {
    setOwnerActiveEmptyMessage("");
    }



  const grunnrenteActiveCount = active.reduce((acc, r) =>
    acc + (Number(r.grunnrente_pliktig) === 1 ? 1 : 0)
  , 0);


  // Treffer: skjul tom-tilstand, vis resultater
  setOwnerEmptyStateVisible(false);
  setOwnerResultsVisible(true);

  // Render owner card
  renderOwnerCardUnified({
    ownerName: stats.owner_name || "(ukjent)",
    ownerIdentity: ownerIdentityNorm,
    activeCount: Number(stats.active_permits ?? 0),
    grunnrenteActiveCount,
    formerPermitCount: Number(stats.former_permits ?? 0),
  });

  // Render active table
  // ✅ Punkt 4: tom-melding når filter gir 0 treff
  if (activeDisplay.length === 0) {
    setOwnerActiveEmptyMessage("Ingen tillatelser funnet");
  } else {
    setOwnerActiveEmptyMessage("");
  }

const activeBody = safeEl("ownerActiveTable").querySelector("tbody");
activeBody.innerHTML = "";

  for (const r of activeDisplay) {
  const rowDict = parseJsonSafe(r.row_json);

  const prodOmr = String(rowDict["PROD_OMR"] ?? "").trim();

  const artRaw = (r.art && String(r.art).trim())
  ? String(r.art).trim()
  : String(rowDict["ART"] ?? "").trim();

  let art = artRaw;

  if (artRaw) {
    const parts = artRaw
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    if (parts.length > 3) {
      art = `${parts.slice(0, 3).join(", ")}*`;
    } else {
      art = parts.join(", ");
    }
  }


  const formal = String(rowDict["FORMÅL"] ?? "").trim();
  const produksjonsform = String(rowDict["PRODUKSJONSFORM"] ?? "").trim();
  const kapRaw = String(rowDict["TILL_KAP"] ?? "").trim();
  const enh = String(rowDict["TILL_ENHET"] ?? "").trim();

  const kapFmt = formatKapNoTrailing00(kapRaw);
  const kapasitet = kapFmt ? `${kapFmt}${enh ? " " + enh : ""}` : "";

  const tr = document.createElement("tr");
  tr.innerHTML = `
    <td><a class="link" href="#/permit/${encodeURIComponent(normalizePermitKey(r.permit_key))}">${escapeHtml(r.permit_key)}</a></td>
    <td>${escapeHtml(art)}</td>
    <td>${escapeHtml(formal)}</td>
    <td>${escapeHtml(produksjonsform)}</td>
    <td>${escapeHtml(kapasitet)}</td>
    <td>${escapeHtml(prodOmr || "N/A")}</td>
  `;
  activeBody.appendChild(tr);
}


  // Owner history table
  const hist = execAll(`
  SELECT
    permit_key AS permit_key,
    valid_from AS valid_from,
    valid_to   AS valid_to,
    COALESCE(NULLIF(valid_to,''), 'Aktiv') AS valid_to_label,
    owner_name AS owner_name,
    owner_orgnr AS owner_orgnr,
    tidsbegrenset AS tidsbegrenset
  FROM ownership_history
  WHERE
    REPLACE(TRIM(owner_identity), ' ', '') = ?
    AND valid_to IS NOT NULL
    AND valid_to <> ''
  ORDER BY permit_key, date(valid_from), id;
`, [ownerIdentityNorm]);


  const histBody = safeEl("ownerHistoryTable").querySelector("tbody");
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
      reason = `Utløpt (tidsbegrenset ${formatNorwegianDate(tb)})`;
    } else if (hasNextSamePermit) {
      reason = "Overført / ny periode";
    } else {
      reason = "Avsluttet";
    }
    if (!reason) reason = "--";

    const vf = displayDate(r.valid_from);
    const vtLabel = (r.valid_to_label === "Aktiv") ? "Aktiv" : displayDate(r.valid_to_label);

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(normalizePermitKey(r.permit_key))}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(vf)}</td>
      <td>${escapeHtml(vtLabel)}</td>
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
  console.log("renderRoute()", location.hash);
  if (!db) return;
  const r = parseHash();
  if (r.view === "now") return renderNow();
  if (r.view === "permit") return renderPermit(r.key);
  if (r.view === "owner") return renderOwner(r.ident);
  renderNow();
}

// --- events ---
function wireEvents() {
  window.addEventListener("hashchange", () => {
    console.log("hashchange ->", location.hash);
    renderRoute();
  });

  // NOW search (debounced)
  let nowTimer = null;
  safeEl("nowSearch").addEventListener("input", () => {
    clearTimeout(nowTimer);
    nowTimer = setTimeout(() => renderNow(), 80);
  });
  safeEl("onlyGrunnrente").addEventListener("change", () => renderNow());

  // PERMIT actions
  safeEl("permitGo").addEventListener("click", () => {
    const raw = safeEl("permitInput").value;

    if (!String(raw || "").trim()) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "🔍",
        title: "Søk etter tillatelse",
        text: "Skriv et tillatelsesnr. (f.eks. H-F-0910)."
      });
      setPermitEmptyStateVisible(true);
      return;
    }

    if (isNineDigits(raw)) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "ℹ️",
        title: "Dette ser ut som org.nr.",
        text: "Dette er et org.nr. (9 siffer). Bruk fanen Innehaver for org.nr."
      });
      setPermitEmptyStateVisible(true);
      return;
    }

    toHashPermit(raw);
  });

  safeEl("permitInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") safeEl("permitGo").click();
  });

  // Tøm permit-visning når input slettes
  safeEl("permitInput").addEventListener("input", (e) => {
    const v = String(e.target.value || "").trim();
    if (!v) {
      clearPermitView();
      setPermitEmptyStateContent({
        icon: "🔍",
        title: "Søk etter tillatelse",
        text: "Skriv et tillatelsesnummer i feltet over for å se detaljer og historikk."
      });
      setPermitEmptyStateVisible(true);
      location.hash = "#/permit";
    }
  });

  // OWNER actions
  safeEl("ownerGo").addEventListener("click", () => {
    const ident = safeEl("ownerInput").value.trim();

    if (!ident) {
      clearOwnerView();
      setOwnerEmptyStateContent({
        icon: "🔍",
        title: "Søk etter innehaver",
        text: "Skriv et org.nr. (9 siffer) i feltet over for å se aktive tillatelser og historikk."
      });
      setOwnerEmptyStateVisible(true);
      setOwnerResultsVisible(false);
      return;
    }

    // Strengt krav i UI: 9 siffer uten mellomrom
    if (!/^[0-9]{9}$/.test(ident)) {
      clearOwnerView();
      setOwnerEmptyStateContent({
        icon: "⚠️",
        title: "Ugyldig org.nr.",
        text: "Org.nr. må være et tall på 9 siffer uten mellomrom."
      });
      setOwnerEmptyStateVisible(true);
      setOwnerResultsVisible(false);
      return;
    }

    toHashOwner(ident);
  });

  safeEl("ownerInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") safeEl("ownerGo").click();
  });

  // Tøm owner-visning når input slettes
  safeEl("ownerInput").addEventListener("input", (e) => {
    const v = e.target.value.trim();
    if (!v) {
      clearOwnerView();
      setOwnerEmptyStateContent({
        icon: "🔍",
        title: "Søk etter innehaver",
        text: "Skriv et org.nr. (9 siffer) i feltet over for å se aktive tillatelser og historikk."
      });
      setOwnerEmptyStateVisible(true);
      setOwnerResultsVisible(false);
      location.hash = "#/owner";
    }
  });
}

function showError(err) {
  console.error(err);
  setStatus("Feil ved lasting", "bad");
  setMeta(String(err?.message || err));
}

const ownerOnly = $("ownerOnlyGrunnrente");
if (ownerOnly) {
  ownerOnly.addEventListener("change", () => {
    const r = parseHash();
    if (r.view === "owner") renderOwner(r.ident);
  });
}

// --- main ---
(async function main() {
  try {
    wireEvents();
    if (!location.hash) toHashNow();
    await loadDatabase();
  } catch (e) {
    showError(e);
  }
})();
