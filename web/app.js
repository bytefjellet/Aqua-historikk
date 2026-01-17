/* global initSqlJs */

let SQL = null;
let db = null;
let areaIndexBuilt = false;
let areaOwnersIndex = new Map(); // code -> Map(key -> { owner_name, owner_identity, count })
let areaPermitCount = new Map(); // code -> total permits

const DB_URL = "data/aqua.sqlite";

// Schema flags (settes etter DB lastes)
const schema = {
  permit_current_has_art: false,
  permit_snapshot_has_art: false,
  permit_snapshot_has_row_json: false,
  permit_snapshot_has_grunnrente: false,
  production_area_has_name: false,
  production_area_has_status: false,
  production_area_has_date: false,
};

// Owner filter state
const ownerFilters = {
  formal: null, // valgt formål (string) eller null
};

// Liste over alle formål i DB (fra permit_current.row_json)
let allFormals = [];

// --- helpers ---
function $(id) { return document.getElementById(id); }

function safeEl(id) {
  const el = $(id);
  if (!el) throw new Error(`Mangler element i HTML: #${id}`);
  return el;
}

function escapeHtml(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
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

function setActiveTab(tabId) {
  for (const id of ["tab-now", "tab-permit", "tab-owner", "tab-history", "tab-areas"]) {
    const el = $(id);
    if (!el) continue;
    el.classList.toggle("active", id === tabId);
  }
}



function showView(viewId) {
  for (const id of ["view-now", "view-permit", "view-owner", "view-history", "view-areas"]) {
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

function normTrafficStatus(s) {
  const t = String(s ?? "").trim().toUpperCase();
  if (!t) return null;
  if (t.startsWith("GRØNN") || t.startsWith("GRONN") || t === "GREEN") return "GREEN";
  if (t.startsWith("GUL") || t === "YELLOW") return "YELLOW";
  if (t.startsWith("RØD") || t.startsWith("ROD") || t === "RED") return "RED";
  return null;
}

function trafficClass(statusNorm) {
  if (statusNorm === "GREEN") return "traffic--green";
  if (statusNorm === "YELLOW") return "traffic--yellow";
  if (statusNorm === "RED") return "traffic--red";
  return "traffic--unknown";
}

// Henter siste status + navn for et produksjonsområde (1-13)
// Tilpasset ditt schema: production_area_status(snapshot_date, prod_area_code, prod_area_status, ...)
function getProductionAreaInfo(prodAreaRaw) {
  const code = parseProdAreaCode(prodAreaRaw);
  if (!code) {
    return { code: String(prodAreaRaw ?? "").trim(), name: "", status: null };
  }

  const row = one(`
    SELECT prod_area_status
    FROM production_area_status
    WHERE prod_area_code = ?
    ORDER BY date(snapshot_date) DESC
    LIMIT 1;
  `, [code]);

  return {
    code,
    name: PRODUCTION_AREA_NAMES[code] || "",
    status: normTrafficStatus(row?.prod_area_status)
  };
}


function parseProdAreaCode(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return null;

  // finn første tallsekvens i strengen (f.eks "7", "PO7", "7 - Helgeland", osv.)
  const m = s.match(/\d+/);
  if (!m) return null;

  const n = Number(m[0]);
  if (!Number.isFinite(n)) return null;
  if (n < 1 || n > 13) return null;
  return n;
}


function hasColumn(table, col) {
  const rows = execAll(`PRAGMA table_info(${table});`);
  return rows.some(r => String(r.name) === col);
}

function parseJsonSafe(s) {
  try { return s ? JSON.parse(s) : {}; } catch { return {}; }
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

function pillHtml(text, kind) {
  const cls = kind === "blue" ? "pill--blue" : "pill--yellow";
  return `<span class="pill ${cls}">${escapeHtml(text)}</span>`;
}

function formatNorwegianDate(isoDate) {
  const d = new Date(String(isoDate).slice(0, 10) + "T00:00:00");
  return new Intl.DateTimeFormat("nb-NO", {
    day: "numeric",
    month: "long",
    year: "numeric"
  }).format(d);
}

function valueOrDash(v) {
  const t = String(v ?? "").trim();
  return t ? t : "—";
}

function isNineDigits(s) {
  return /^[0-9]{9}$/.test(String(s || "").trim().replace(/\s+/g, ""));
}

function normalizePermitKey(raw) {
  return (raw ?? "")
    .toString()
    .trim()
    .replace(/\s+/g, "")
    .toUpperCase();
}

function toHashNow() { location.hash = "#/now"; }
function toHashPermit(key) {
  const norm = normalizePermitKey(key);
  location.hash = `#/permit/${encodeURIComponent(norm)}`;
}
function toHashOwner(identity) {
  location.hash = `#/owner/${encodeURIComponent(String(identity ?? "").trim())}`;
}

function formatKapNoTrailing00(kapRaw) {
  const t = String(kapRaw ?? "").trim();
  if (!t) return "";
  const normalized = t.replace(",", ".");
  const num = Number(normalized);
  if (!Number.isFinite(num)) return t;

  if (Math.abs(num - Math.round(num)) < 1e-9) return String(Math.round(num));

  const s = String(num);
  return s.replace(".", ",");
}

function extractCapacityTN(rowJsonText) {
  const d = parseJsonSafe(rowJsonText);

  const kapRaw = String(d["TILL_KAP"] ?? "").trim();
  const enh = String(d["TILL_ENHET"] ?? "").trim().toUpperCase();

  if (!kapRaw || enh !== "TN") return 0;

  // normaliser tall (komma → punkt)
  const num = Number(kapRaw.replace(",", "."));
  return Number.isFinite(num) ? num : 0;
}

// -------------------------------
// NYTT: pill-regler (aktive)
// -------------------------------
function normUpper(s) {
  return (s ?? "").toString().trim().toUpperCase();
}

function normStage(s) {
  return (s ?? "")
    .toString()
    .toUpperCase()
    .replace(/[–—]/g, "-")      // lang dash → vanlig dash
    .replace(/\s*-\s*/g, " - ") // alltid mellomrom rundt -
    .replace(/\s+/g, " ")       // komprimer whitespace
    .trim();
}

function toArray(val) {
  if (val == null) return [];
  if (Array.isArray(val)) return val;
  return val
    .toString()
    .split(/[;,/]/)
    .map(x => x.trim())
    .filter(Boolean);
}

// Art: blå hvis Laks/Regnbuørret/Ørret inngår
const BLUE_ART = new Set(["LAKS", "REGNBUØRRET", "ØRRET"]);
function isBlueArt(artValue) {
  const arts = toArray(artValue).map(normUpper);
  return arts.some(a => BLUE_ART.has(a));
}

// Formål: blå hvis KOMMERSIELL
function isBlueFormal(formalValue) {
  return normUpper(formalValue) === "KOMMERSIELL";
}


// Produksjonsstadium: blå hvis i whitelist
const BLUE_STAGE = new Set([
  "MATFISK",
  "MATFISK - (5% MTB ØKNING)",
  "MATFISK - GRØNN A",
  "MATFISK - GRØNN A(5% MTB ØKNING)",
  "MATFISK - GRØNN B",
  "MATFISK - GRØNN C",
  "MATFISK - GRØNN KONVERTERT",
  "ØKOLOGISK MATFISK",
].map(normUpper));

function isBlueProduksjonsstadium(stageValue) {
  return BLUE_STAGE.has(normStage(stageValue));
}


function pillSpanByRule(text, isBlue) {
  if (!String(text ?? "").trim()) return "";
  return `<span class="pill ${isBlue ? "pill--blue" : "pill--yellow"}">${escapeHtml(String(text).trim())}</span>`;
}
// -------------------------------
// PRODUKSJONSOMRÅDER – trafikklys
// -------------------------------

// Faste navn på produksjonsområder (1–13)
const PRODUCTION_AREA_NAMES = {
  1: "Svenskegrensen – Jæren",
  2: "Ryfylke",
  3: "Karmøy – Sotra",
  4: "Nordhordland – Stadt",
  5: "Stadt – Hustadvika",
  6: "Nordmøre – Sør-Trøndelag",
  7: "Nord-Trøndelag med Bindal",
  8: "Helgeland – Bodø",
  9: "Vestfjorden og Vesterålen",
  10: "Andøya – Senja",
  11: "Kvaløya – Loppa",
  12: "Vest-Finnmark",
  13: "Øst-Finnmark"
};

function normTrafficStatus(s) {
  const t = String(s ?? "").trim().toUpperCase();
  if (t === "GRØNN") return "GREEN";
  if (t === "GUL")   return "YELLOW";
  if (t === "RØD")   return "RED";
  return null;
}

function getProductionAreaInfo(prodAreaRaw) {
  const code = parseProdAreaCode(prodAreaRaw);
  if (!code) {
    return { code: String(prodAreaRaw ?? "").trim(), name: "", status: null };
  }

  const row = one(`
    SELECT prod_area_status
    FROM production_area_status
    WHERE prod_area_code = ?
    ORDER BY date(snapshot_date) DESC
    LIMIT 1;
  `, [code]);

  return {
    code, // <- alltid 1–13 her
    name: PRODUCTION_AREA_NAMES[code] || "",
    status: normTrafficStatus(row?.prod_area_status)
  };
}


function trafficHtml(code, statusNorm) {
  const cls =
    statusNorm === "GREEN"  ? "traffic--green"  :
    statusNorm === "YELLOW" ? "traffic--yellow" :
    statusNorm === "RED"    ? "traffic--red"    :
                              "traffic--unknown";

  const label =
    statusNorm === "GREEN"  ? "Grønn"  :
    statusNorm === "YELLOW" ? "Gul"    :
    statusNorm === "RED"    ? "Rød"    :
                              "Ukjent";

  return `
    <span class="traffic ${cls}" title="Status: ${label}">
      <span class="traffic-dot" aria-hidden="true"></span>
      <span class="traffic-num">${escapeHtml(code)}</span>

    </span>
  `;
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

// --- OWNER empty/results helpers ---
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
  const el = $("ownerResults");
  if (el) el.classList.toggle("hidden", !visible);
}

function setOwnerActiveEmptyMessage(text) {
  const el = $("ownerActiveEmpty");
  if (!el) return;
  el.textContent = text || "";
  el.classList.toggle("hidden", !text);
}

function clearPermitView() {
  setPermitResultsVisible(false);
  setPermitEmptyStateVisible(true);

  const legacy = $("permitEmpty");
  if (legacy) legacy.textContent = "";

  const card = $("permitCard");
  if (card) card.classList.add("hidden");

  const tbody = safeEl("permitHistoryTable").querySelector("tbody");
  tbody.innerHTML = "";

  const reasonTh = $("permitReasonTh");
  if (reasonTh) reasonTh.style.display = "";
}

function clearOwnerView() {
  setOwnerResultsVisible(false);
  setOwnerEmptyStateVisible(true);

  const ownerCard = $("ownerCard");
  if (ownerCard) ownerCard.classList.add("hidden");

  const ownerEmpty = $("ownerEmpty");
  if (ownerEmpty) ownerEmpty.textContent = "";

  setOwnerActiveEmptyMessage("");

  const filters = $("ownerFormalFilters");
  if (filters) filters.innerHTML = "";

  const activeBody = safeEl("ownerActiveTable").querySelector("tbody");
  activeBody.innerHTML = "";

  const histBody = safeEl("ownerHistoryTable").querySelector("tbody");
  histBody.innerHTML = "";
}

// --- owner row_json FORMÅL helper ---
function extractFormalFromRowJson(rowJsonText) {
  const d = parseJsonSafe(rowJsonText);
  return String(d["FORMÅL"] ?? "").trim();
}

// --- Owner formål filter buttons ---
function renderOwnerFormalButtons(countsByFormal /* Map */) {
  const root = $("ownerFormalFilters");
  if (!root) return;

  root.innerHTML = "";
  if (!allFormals.length) return;

  for (const formal of allFormals) {
    const count = Number(countsByFormal.get(formal) || 0);

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "filter-btn";
    if (count === 0) btn.classList.add("zero");
    if (ownerFilters.formal === formal) btn.classList.add("active");
    btn.textContent = `${formal} (${count})`;

    btn.addEventListener("click", () => {
      ownerFilters.formal = (ownerFilters.formal === formal) ? null : formal;
      const r = parseHash();
      if (r.view === "owner") renderOwner(r.ident);
    });

    root.appendChild(btn);
  }
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
  vannmiljo,
  lokPlass,
  tidsbegrenset,
}) {
  const card = safeEl("permitCard");
  card.classList.remove("hidden");

  // NYTT: Historisk-pill skal være rød (ikke gul)
  const statusPillClass = isActive ? "pill--green" : "pill--red";
  const statusPillText  = isActive ? "Aktiv tillatelse" : "Historisk";

  const vm = String(vannmiljo ?? "").trim();
  const lp = String(lokPlass ?? "").trim();

  const vmPill = vm
    ? `<span class="pill ${vm.toUpperCase() === "SALT" ? "pill--blue" : "pill--yellow"}">${escapeHtml(vm)}</span>`
    : "";

  const lpPill = lp
    ? `<span class="pill ${lp.toUpperCase() === "SJØ" ? "pill--blue" : "pill--yellow"}">${escapeHtml(lp)}</span>`
    : "";

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

  // NYTT: Art/Formål/Produksjonsstadium pill-regler (kun for aktive)
  const artPill = pillSpanByRule(artText, isBlueArt(artText));
  const formalPill = pillSpanByRule(formal, isBlueFormal(formal));
  const prodStagePill = pillSpanByRule(
    produksjonsstadium,
    isBlueProduksjonsstadium(produksjonsstadium)
  );


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

      ${artText ? `
      <div style="margin-top:8px">
        <span class="muted">Arter:</span> ${artPill || escapeHtml(valueOrDash(artText))}
      </div>
    ` : ""}

      <div style="margin-top:10px">
        <div>
          <span class="muted">Formål:</span>
          ${formalPill || escapeHtml(valueOrDash(formal))}
        </div>

        <div style="margin-top:6px">
          <span class="muted">Produksjonsstadium:</span>
          ${prodStagePill || escapeHtml(valueOrDash(produksjonsstadium))}
        </div>

        <div style="margin-top:6px"><span class="muted">Tillatelseskapasitet:</span> ${escapeHtml(valueOrDash(kapasitet))}</div>
      
        ${(() => {
          const areaRaw = String(prodOmr ?? "").trim();
          if (!areaRaw || areaRaw === "N/A") {
            return `
              <div style="margin-top:6px">
                <span class="muted">Produksjonsområde:</span> ${escapeHtml(areaRaw || "N/A")}
              </div>
            `;
          }

          const info = getProductionAreaInfo(areaRaw);

          const areaLine = trafficHtml(info.code, info.status);
          const nameInline = info.name
            ? `<span class="muted" style="margin-left:6px">${escapeHtml(info.name)}</span>`
            : "";

          return `
            <div style="margin-top:6px">
              <span class="muted">Produksjonsområde:</span>
              ${areaLine}${nameInline}
            </div>
          `;

        })()}

        ${(vmPill || lpPill) ? `
          <div style="margin-top:8px">
            ${vmPill ? `<div><span class="muted">Vannmiljø:</span> ${vmPill}</div>` : ""}
            ${lpPill ? `<div style="margin-top:6px"><span class="muted">Plassering:</span> ${lpPill}</div>` : ""}
          </div>
        ` : ""}
      </div>
    </div>
  `;
}

// --- UNIFIED owner card renderer (med blå/gul grunnrente-pill) ---
function renderOwnerCardUnified({
  ownerName,
  ownerIdentity,
  activeCount,
  grunnrenteActiveCount,
  formerPermitCount,
  activeCapacityTN = 0,
  grunnrenteCapacityTN = 0,
}) {
  const card = safeEl("ownerCard");
  card.classList.remove("hidden");

  const name = valueOrDash(ownerName);
  const ident = String(ownerIdentity ?? "").trim();

  // Formatter TN-sum (midlertidig: vis også 0 for debugging)
  function fmtTN(n) {
  const v = Number(n || 0);
  if (v <= 0) return "";
  return ` <span class="muted-small">(samlet kapasitet: ${Math.round(v).toLocaleString("nb-NO")} TN)</span>`;
}




  console.log("activeCapacityTN:", activeCapacityTN, "grunnrenteCapacityTN:", grunnrenteCapacityTN);

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
        <div>
          <span class="muted">Aktive tillatelser:</span>
          ${escapeHtml(String(activeCount ?? 0))}${fmtTN(activeCapacityTN)}
        </div>
        <div>
          <span class="muted">Grunnrentepliktige tillatelser:</span>
          ${escapeHtml(String(grunnrenteActiveCount ?? 0))}${fmtTN(grunnrenteCapacityTN)}
        </div>
        <div>
          <span class="muted">Historiske tillatelser:</span>
          ${escapeHtml(String(formerPermitCount ?? 0))}
        </div>
      </div>
    </div>
  `;
}


// --- sort state (NOW) ---
const sortState = { now: { key: "permit_key", dir: 1 } };

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

    // production_area_status kolonner (robusthet)
  schema.production_area_has_name =
    hasColumn("production_area_status", "production_area_name") ||
    hasColumn("production_area_status", "area_name") ||
    hasColumn("production_area_status", "name");

  schema.production_area_has_status =
    hasColumn("production_area_status", "status") ||
    hasColumn("production_area_status", "color");

  schema.production_area_has_date =
    hasColumn("production_area_status", "status_date") ||
    hasColumn("production_area_status", "date") ||
    hasColumn("production_area_status", "snapshot_date") ||
    hasColumn("production_area_status", "as_of");


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

  const snap = one(`SELECT MAX(snapshot_date) AS max_date FROM snapshots;`);
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
  
    const empty = $("historyEmpty");
      if (empty) {
        empty.classList.toggle("hidden", filtered.length !== 0);
      }

  const { key, dir } = sortState.now;
  filtered.sort((a, b) => {
    const av = String(a[key] ?? "");
    const bv = String(b[key] ?? "");
    return av.localeCompare(bv, "nb", { numeric: true, sensitivity: "base" }) * dir;
  });

  safeEl("nowSummary").textContent =
    `Antall tillatelser i visningen: ${filtered.length}` + (only ? " (grunnrentepliktig)" : "");

  const tbody = safeEl("nowTable").querySelector("tbody");
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

    const permitHrefKey = normalizePermitKey(r.permit_key);

    tr.innerHTML = `
      <td><a class="link" href="#/permit/${encodeURIComponent(permitHrefKey)}">${escapeHtml(r.permit_key)}</a></td>
      <td>${escapeHtml(r.owner_name)}</td>
      <td>${
        ownerIdent
          ? `<a class="link" href="#/owner/${encodeURIComponent(ownerIdent)}">${escapeHtml(orgnrOrIdent)}</a>`
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
    setPermitEmptyStateContent({ icon: "ℹ️", title: "Dette ser ut som org.nr.", text: msg });
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

  // Årsak-kolonne: vis hvis noen har årsak
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
  const tidsbegrensetCardDisplay = tidsbegrensetCard ? formatNorwegianDate(tidsbegrensetCard) : "";

  if (now) {
    const rowDict = parseJsonSafe(now.row_json);

    const artText = (now.art && String(now.art).trim())
      ? String(now.art).trim()
      : String(rowDict["ART"] ?? "").trim();

    const formal = String(rowDict["FORMÅL"] ?? "").trim();
    const produksjonsstadium = String(rowDict["PRODUKSJONSSTADIUM"] ?? rowDict["PRODUKSJONSFORM"] ?? "").trim();
    const kapRaw = String(rowDict["TILL_KAP"] ?? "").trim();
    const enh = String(rowDict["TILL_ENHET"] ?? "").trim();
    const kapFmt = formatKapNoTrailing00(kapRaw);
    const kapasitet = kapFmt ? `${kapFmt}${enh ? " " + enh : ""}` : "";
    const prodOmr = String(rowDict["PROD_OMR"] ?? "").trim() || "N/A";
    const grunnrente = Number(now.grunnrente_pliktig) === 1;
    const vannmiljo = String(rowDict["VANNMILJØ"] ?? rowDict["VANNMILJO"] ?? rowDict["VANNMILJ"] ?? "").trim();
    const lokPlass  = String(rowDict["LOK_PLASS"] ?? rowDict["LOKPLASS"] ?? rowDict["PLASSERING"] ?? "").trim();

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
      vannmiljo,
      lokPlass,
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
    const kapFmt = formatKapNoTrailing00(kapRaw);
    const kapasitet = kapFmt ? `${kapFmt}${enh ? " " + enh : ""}` : "";
    const prodOmr = String(snapDict["PROD_OMR"] ?? "").trim() || "N/A";
    const vannmiljo = String(snapDict["VANNMILJØ"] ?? snapDict["VANNMILJO"] ?? snapDict["VANNMILJ"] ?? "").trim();
    const lokPlass  = String(snapDict["LOK_PLASS"] ?? snapDict["LOKPLASS"] ?? snapDict["PLASSERING"] ?? "").trim();

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
      vannmiljo,
      lokPlass,
      tidsbegrenset: tidsbegrensetCardDisplay,
    });
  }

  // Render history table
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
    const ident = String(r.owner_identity ?? "").trim();

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

// --- OWNER view ---
function renderOwner(ownerIdentity) {
  setActiveTab("tab-owner");
  showView("view-owner");

  setOwnerEmptyStateContent({
    icon: "🔍",
    title: "Søk etter innehaver",
    text: "Skriv et org.nr. (9 siffer) i feltet over for å se aktive tillatelser og historikk."
  });

  clearOwnerView();

  if (ownerIdentity != null) safeEl("ownerInput").value = String(ownerIdentity);

  const inputEl = safeEl("ownerInput");
  const identTrim = String(ownerIdentity || "").trim();

  if (!identTrim) {
    setOwnerEmptyStateVisible(true);
    setOwnerResultsVisible(false);
    return;
  }

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

  const ownerIdentityNorm = identTrim.replace(/\s+/g, "");
  inputEl.value = ownerIdentityNorm;

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

  // --- Kapasitetssummer (kun TN) ---
const activeCapacityTN = active.reduce(
  (sum, r) => sum + extractCapacityTN(r.row_json),
  0
);

const grunnrenteCapacityTN = active.reduce(
  (sum, r) =>
    Number(r.grunnrente_pliktig) === 1
      ? sum + extractCapacityTN(r.row_json)
      : sum,
  0
);

  const grunnrenteActiveCount = active.reduce((acc, r) => acc + (Number(r.grunnrente_pliktig) === 1 ? 1 : 0), 0);

  setOwnerEmptyStateVisible(false);
  setOwnerResultsVisible(true);

  renderOwnerCardUnified({
  ownerName: stats.owner_name || "(ukjent)",
  ownerIdentity: ownerIdentityNorm,
  activeCount: Number(stats.active_permits ?? 0),
  grunnrenteActiveCount,
  formerPermitCount: Number(stats.former_permits ?? 0),
  activeCapacityTN,
  grunnrenteCapacityTN,
});


  // --- FILTER: grunnrente + formål (kun for aktiv-tabellen) ---
  const onlyGrunnrente = $("ownerOnlyGrunnrente")?.checked === true;

  const activeAfterGrunnrente = onlyGrunnrente
    ? active.filter(r => Number(r.grunnrente_pliktig) === 1)
    : active;

  // counts per formål (for knapper)
  const countsByFormal = new Map();
  for (const r of activeAfterGrunnrente) {
    const f = extractFormalFromRowJson(r.row_json);
    if (!f) continue;
    countsByFormal.set(f, (countsByFormal.get(f) || 0) + 1);
  }
  renderOwnerFormalButtons(countsByFormal);

  // formål-filter
  const selectedFormal = ownerFilters.formal;
  const activeDisplay = selectedFormal
    ? activeAfterGrunnrente.filter(r => extractFormalFromRowJson(r.row_json) === selectedFormal)
    : activeAfterGrunnrente;

  if (activeDisplay.length === 0) {
    setOwnerActiveEmptyMessage("Ingen tillatelser funnet");
  } else {
    setOwnerActiveEmptyMessage("");
  }

  // Render active table
  const activeBody = safeEl("ownerActiveTable").querySelector("tbody");
  activeBody.innerHTML = "";

  for (const r of activeDisplay) {
    const rowDict = parseJsonSafe(r.row_json);
    const prodOmr = String(rowDict["PROD_OMR"] ?? "").trim();

    const artRaw = (r.art && String(r.art).trim())
      ? String(r.art).trim()
      : String(rowDict["ART"] ?? "").trim();

    // maks 3 arter + *
    let art = artRaw;
    if (artRaw) {
      const parts = artRaw.split(",").map(s => s.trim()).filter(Boolean);
      if (parts.length > 3) art = `${parts.slice(0, 3).join(", ")}*`;
      else art = parts.join(", ");
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
      <td>
  ${
    prodOmr
      ? (() => {
          const info = getProductionAreaInfo(prodOmr);
          return `
            ${trafficHtml(info.code, info.status)}
            ${info.name
              ? `<div class="muted-small">${escapeHtml(info.name)}</div>`
              : ""}
          `;
        })()
      : "N/A"
  }
</td>

    `;
    activeBody.appendChild(tr);
  }

  // Historikk: kun avsluttede perioder
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
      AND TRIM(valid_to) <> ''
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
    const vtLabel = displayDate(r.valid_to_label);

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

// --- HISTORY view ---
function renderHistory() {
  setActiveTab("tab-history");
  showView("view-history");

  const q = $("historySearch")?.value.trim().toLowerCase() || "";
  const onlyGr = $("historyOnlyGrunnrente")?.checked === true;

  const rows = execAll(`
    WITH ended AS (
      SELECT
        permit_key,
        owner_name,
        owner_identity,
        valid_to
      FROM ownership_history
      WHERE valid_to IS NOT NULL
        AND TRIM(valid_to) <> ''
    ),
    last_end AS (
      SELECT
        permit_key,
        MAX(valid_to) AS last_valid_to
      FROM ended
      GROUP BY permit_key
    ),
    last_owner AS (
      SELECT
        e.permit_key,
        e.owner_name,
        e.owner_identity,
        e.valid_to
      FROM ended e
      JOIN last_end le
        ON le.permit_key = e.permit_key
       AND le.last_valid_to = e.valid_to
    ),
    last_snap AS (
      SELECT
        permit_key,
        MAX(snapshot_date) AS last_snapshot_date
      FROM permit_snapshot
      GROUP BY permit_key
    ),
    snap_gr AS (
      SELECT
        ps.permit_key,
        ps.grunnrente_pliktig
      FROM permit_snapshot ps
      JOIN last_snap ls
        ON ls.permit_key = ps.permit_key
       AND ls.last_snapshot_date = ps.snapshot_date
    )
    SELECT
      lo.permit_key AS permit_key,
      lo.owner_name AS owner_name,
      lo.owner_identity AS owner_identity,
      lo.valid_to AS valid_to,
      sg.grunnrente_pliktig AS grunnrente_pliktig
    FROM last_owner lo
    LEFT JOIN permit_current pc
      ON UPPER(REPLACE(TRIM(pc.permit_key), ' ', '')) = UPPER(REPLACE(TRIM(lo.permit_key), ' ', ''))
    LEFT JOIN snap_gr sg
      ON UPPER(REPLACE(TRIM(sg.permit_key), ' ', '')) = UPPER(REPLACE(TRIM(lo.permit_key), ' ', ''))
    WHERE pc.permit_key IS NULL
    ORDER BY lo.valid_to DESC, lo.permit_key;
  `);

  // Filter: kun grunnrentepliktig
  const rowsAfterGr = onlyGr
    ? rows.filter(r => Number(r.grunnrente_pliktig) === 1)
    : rows;

  // Søk
  const filtered = q
    ? rowsAfterGr.filter(r =>
        String(r.permit_key ?? "").toLowerCase().includes(q) ||
        String(r.owner_name ?? "").toLowerCase().includes(q) ||
        String(r.owner_identity ?? "").toLowerCase().includes(q)
      )
    : rowsAfterGr;

  // Empty state
  const empty = $("historyEmpty");
  if (empty) empty.classList.toggle("hidden", filtered.length !== 0);

  // Skjul/vis tabell
  const tableWrap = $("historyTableWrap");
  if (tableWrap) tableWrap.classList.toggle("hidden", filtered.length === 0);

  const tbody = safeEl("view-history").querySelector("#historyTable tbody");
  tbody.innerHTML = "";

  if (filtered.length === 0) return;

  for (const r of filtered) {
    const ident = String(r.owner_identity ?? "").trim();
    const permitKeyNorm = normalizePermitKey(r.permit_key);

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>
        <a class="link" href="#/permit/${encodeURIComponent(permitKeyNorm)}">
          ${escapeHtml(r.permit_key)}
        </a>
      </td>
      <td>${escapeHtml(r.owner_name || "—")}</td>
      <td>
        ${
          ident
            ? `<a class="link" href="#/owner/${encodeURIComponent(ident)}">${escapeHtml(ident)}</a>`
            : "—"
        }
      </td>
      <td>${escapeHtml(displayDate(r.valid_to))}</td>
    `;
    tbody.appendChild(tr);
  }
} 

function renderAreas() {
  setActiveTab("tab-areas");
  showView("view-areas");

  // Bygg indeks over selskaper per område (én gang)
  buildAreaIndexOnce();

  // Hent siste status per område
  const statusRows = execAll(`
    WITH latest AS (
      SELECT prod_area_code, MAX(snapshot_date) AS max_date
      FROM production_area_status
      GROUP BY prod_area_code
    )
    SELECT
      pas.prod_area_code AS prod_area_code,
      pas.prod_area_status AS prod_area_status,
      pas.snapshot_date AS snapshot_date
    FROM production_area_status pas
    JOIN latest l
      ON l.prod_area_code = pas.prod_area_code
     AND l.max_date = pas.snapshot_date
    ORDER BY pas.prod_area_code;
  `);

  const statusByCode = new Map();
  for (const r of statusRows) {
    statusByCode.set(Number(r.prod_area_code), normTrafficStatus(r.prod_area_status));
  }

  const tbody = safeEl("areasTable").querySelector("tbody");
  tbody.innerHTML = "";

  for (let code = 1; code <= 13; code++) {
    const status = statusByCode.get(code) || null;
    const total = Number(areaPermitCount.get(code) || 0);

    const tr = document.createElement("tr");
    tr.dataset.areaCode = String(code);
    tr.innerHTML = `
      <td>
        <button class="expander-btn" type="button" aria-label="Vis detaljer" aria-expanded="false">
          <span class="chev">▶</span>
        </button>
      </td>
      <td>
        ${trafficHtml(code, status)}
        <span class="muted" style="margin-left:6px">${escapeHtml(PRODUCTION_AREA_NAMES[code] || "")}</span>
      </td>
      <td>${escapeHtml(status === "GREEN" ? "Grønn" : status === "YELLOW" ? "Gul" : status === "RED" ? "Rød" : "Ukjent")}</td>
      <td style="text-align:right">${escapeHtml(total.toLocaleString("nb-NO"))}</td>
    `;
    tbody.appendChild(tr);

    const detailsTr = document.createElement("tr");
    detailsTr.className = "details-row hidden";
    detailsTr.dataset.detailsFor = String(code);
    detailsTr.innerHTML = `
      <td colspan="4">
        <div class="details-box">
          <div class="muted" style="margin-bottom:8px">Klikk pilen for å se selskaper…</div>
        </div>
      </td>
    `;
    tbody.appendChild(detailsTr);
  }

  // Klikk-håndtering (delegert)
  tbody.onclick = (e) => {
    const btn = e.target.closest(".expander-btn");
    if (!btn) return;

    const row = e.target.closest("tr");
    if (!row) return;

    const code = Number(row.dataset.areaCode);
    const detailsRow = tbody.querySelector(`tr.details-row[data-details-for="${code}"]`);
    if (!detailsRow) return;

    const isOpen = !detailsRow.classList.contains("hidden");

    detailsRow.classList.toggle("hidden", isOpen);
    row.classList.toggle("is-open", !isOpen);
    btn.setAttribute("aria-expanded", String(!isOpen));

    if (!isOpen) {
      const box = detailsRow.querySelector(".details-box");
      if (!box) return;

      const ownersMap = areaOwnersIndex.get(code) || new Map();
      const owners = Array.from(ownersMap.values())
        .sort((a, b) => (b.count - a.count) || String(a.owner_name).localeCompare(String(b.owner_name), "nb", { sensitivity: "base" }));

      if (owners.length === 0) {
        box.innerHTML = `<div class="details-empty">Ingen aktive tillatelser funnet i dette området.</div>`;
        return;
      }

      box.innerHTML = `
        <div class="details-grid">
          ${owners.map(o => {
            const name = escapeHtml(o.owner_name || "(ukjent)");
            const ident = String(o.owner_identity ?? "").trim();
            const left = ident
              ? `<a class="link" href="#/owner/${encodeURIComponent(ident)}">${name}</a> <span class="muted-small">(${escapeHtml(ident)})</span>`
              : `${name}`;
            return `
              <div class="details-item">
                <div>${left}</div>
                <div class="details-count">${escapeHtml(String(o.count))}</div>
              </div>
            `;
          }).join("")}
        </div>
      `;
    }
  };
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

  // 👇 NYTT: Historikk (støtter både /history og /historikk)
  if (parts[0] === "history" || parts[0] === "historikk") {
    return { view: "history" };
  }

  // 👇 NYTT: Produksjonsområder
  if (parts[0] === "areas" || parts[0] === "produksjonsomrader" || parts[0] === "produksjonsområder") {
    return { view: "areas" };
  }

  return { view: "now" };
}


function renderRoute() {
  if (!db) return;
  const r = parseHash();
  if (r.view === "now") return renderNow();
  if (r.view === "permit") return renderPermit(r.key);
  if (r.view === "owner") return renderOwner(r.ident);
  if (r.view === "history") return renderHistory();
  if (r.view === "areas") return renderAreas();
  renderNow();
}


// --- events ---
function wireEvents() {
  window.addEventListener("hashchange", () => renderRoute());

  // HISTORIKK-tab
  const tabHistory = $("tab-history");
  if (tabHistory) {
    tabHistory.addEventListener("click", () => {
      location.hash = "#/history";
    });
  }

  // NOW
  let nowTimer = null;
  safeEl("nowSearch").addEventListener("input", () => {
    clearTimeout(nowTimer);
    nowTimer = setTimeout(() => renderNow(), 80);
  });
  safeEl("onlyGrunnrente").addEventListener("change", () => renderNow());

  // PERMIT
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

  // OWNER
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

    if (!isNineDigits(ident)) {
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

    ownerFilters.formal = null;
    toHashOwner(ident.replace(/\s+/g, ""));
  });

  safeEl("ownerInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") safeEl("ownerGo").click();
  });

  safeEl("ownerInput").addEventListener("input", (e) => {
    const v = e.target.value.trim();
    if (!v) {
      ownerFilters.formal = null;
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

  // Re-render owner view når checkbox endres (grunnrente filter)
  const ownerOnly = $("ownerOnlyGrunnrente");
  if (ownerOnly) {
    ownerOnly.addEventListener("change", () => {
      const r = parseHash();
      if (r.view === "owner") renderOwner(r.ident);
    });
  }
}

function showError(err) {
  console.error(err);
  setStatus("Feil ved lasting", "bad");
  setMeta(String(err?.message || err));
}
// HISTORY: re-render ved søk/checkbox
const historyOnly = $("historyOnlyGrunnrente");
if (historyOnly) {
  historyOnly.addEventListener("change", () => {
    const r = parseHash();
    if (r.view === "history") renderHistory();
  });
}

const historySearch = $("historySearch");
if (historySearch) {
  let t = null;
  historySearch.addEventListener("input", () => {
    clearTimeout(t);
    t = setTimeout(() => {
      const r = parseHash();
      if (r.view === "history") renderHistory();
    }, 80);
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
