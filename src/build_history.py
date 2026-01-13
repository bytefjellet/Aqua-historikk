# src/build_history.py
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from src.config import CSV_ENCODING, KEY_COL, OWNER_NAME_COL, OWNER_ORG_COL, SNAPSHOT_DIR
from src.db import connect, init_db
from src.grunnrente import is_grunnrente_pliktig
from src.ingest import load_snapshot_csv

# ----------------------------
# Dato-validering (første linje)
# ----------------------------

TITLE_DATE_RE = re.compile(r"PR\.?\s*[:.]?\s*(\d{2}-\d{2}-\d{4})")


def date_from_filename(p: Path) -> str:
    """
    Forventer at filnavnet starter med YYYY-MM-DD, f.eks. '2025-12-21 - ... .csv'
    Returnerer 'YYYY-MM-DD'.
    """
    stem = p.stem
    if len(stem) < 10:
        raise ValueError(f"Filnavn for kort til å inneholde dato: {p.name}")
    s = stem[:10]
    datetime.strptime(s, "%Y-%m-%d")  # valider
    return s


def read_title_date(csv_path: Path, encoding: str = "utf-8-sig") -> str:
    """
    Leser første linje og finner dato etter 'PR.' i format DD-MM-YYYY.
    Returnerer ISO-format: YYYY-MM-DD.
    """
    with csv_path.open("r", encoding=encoding, errors="replace") as f:
        first_line = f.readline().strip()

    m = TITLE_DATE_RE.search(first_line)
    if not m:
        raise ValueError(
            f"Fant ikke dato i første linje i {csv_path.name}. Linje 1 var: {first_line!r}"
        )
    ddmmyyyy = m.group(1)
    d = datetime.strptime(ddmmyyyy, "%d-%m-%Y").date()
    return d.isoformat()


def prev_day(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return (d - timedelta(days=1)).isoformat()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def as_text(v) -> str:
    """
    Robust konvertering fra pandas/NaN/None til str.
    """
    if v is None:
        return ""
    try:
        if isinstance(v, float) and v != v:  # NaN
            return ""
    except Exception:
        pass
    return str(v).strip()


def parse_ddmmyyyy(s: str) -> Optional[str]:
    """
    Parser dato i format DD-MM-YYYY (f.eks. '23-12-2025')
    og returnerer ISO-format YYYY-MM-DD.
    Returnerer None hvis tom eller ugyldig.
    """
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d-%m-%Y").date().isoformat()
    except ValueError:
        return None


# ----------------------------
# Canonical JSON + hashing
# ----------------------------

def canonicalize_row_dict(d: dict) -> dict:
    """
    Normaliserer en pandas-row dict slik at 'samme innhold' gir samme JSON.
    - Stripper keys
    - Konverterer tom/NaN-ish til None
    - Stripper string-verdier
    """
    def norm(v):
        if v is None:
            return None
        try:
            if isinstance(v, float) and v != v:
                return None
        except Exception:
            pass

        s = str(v).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            return None
        return s

    return {str(k).strip(): norm(v) for k, v in d.items()}


def canonical_json(d: dict) -> str:
    dd = canonicalize_row_dict(d)
    return json.dumps(dd, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def latest_snapshot_hash(conn: sqlite3.Connection, permit_key: str) -> Optional[str]:
    cur = conn.execute(
        """
        SELECT row_hash
        FROM permit_snapshot
        WHERE permit_key = ?
        ORDER BY snapshot_date DESC
        LIMIT 1;
        """,
        (permit_key,),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ----------------------------
# Normalisering / identity
# ----------------------------

def normalize_permit_key(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s.replace(" ", "-")


def make_owner_identity(owner_orgnr: str, owner_name: str) -> str:
    org = as_text(owner_orgnr)
    name = as_text(owner_name)
    if org:
        return org
    if name:
        return f"PN:{name}"
    return "PN:UKJENT"


# ----------------------------
# Preflight
# ----------------------------

@dataclass
class PreflightIssue:
    level: str  # "ERROR" eller "WARN"
    file: str
    message: str


@dataclass
class PreflightResult:
    ok: bool
    issues: List[PreflightIssue]
    files_checked: int


def preflight(snapshot_dir: Path = SNAPSHOT_DIR, fail_on_warn: bool = False) -> PreflightResult:
    issues: List[PreflightIssue] = []
    files = sorted(snapshot_dir.glob("*.csv"))

    if not files:
        issues.append(PreflightIssue("ERROR", "-", f"Ingen CSV-filer funnet i {snapshot_dir}"))
        return PreflightResult(ok=False, issues=issues, files_checked=0)

    required_cols = {KEY_COL, OWNER_ORG_COL, OWNER_NAME_COL}

    for p in files:
        # Filnavn-dato
        try:
            fname_date = date_from_filename(p)
        except Exception as e:
            issues.append(PreflightIssue("ERROR", p.name, f"Ugyldig filnavn-dato: {e}"))
            continue

        # Tittel-dato
        try:
            title_date = read_title_date(p, encoding=CSV_ENCODING)
        except Exception as e:
            issues.append(PreflightIssue("ERROR", p.name, f"Kunne ikke lese dato fra første linje: {e}"))
            continue

        if title_date != fname_date:
            issues.append(
                PreflightIssue(
                    "ERROR",
                    p.name,
                    f"Datomismatch: filnavn={fname_date}, første linje={title_date}",
                )
            )

        # CSV parse
        try:
            df = load_snapshot_csv(p)
        except Exception as e:
            issues.append(PreflightIssue("ERROR", p.name, f"Kunne ikke lese CSV: {e}"))
            continue

        missing = required_cols - set(df.columns)
        if missing:
            issues.append(PreflightIssue("ERROR", p.name, f"Mangler påkrevde kolonner: {sorted(missing)}"))

        # WARN om duplikater per KEY_COL
        if KEY_COL in df.columns:
            key_series = df[KEY_COL].astype(str).str.strip()
            dup_mask = key_series.duplicated(keep=False) & (key_series != "")
            dup_count = int(dup_mask.sum())
            if dup_count > 0:
                issues.append(
                    PreflightIssue(
                        "WARN",
                        p.name,
                        f"Fant {dup_count} rader med duplisert {KEY_COL}. "
                        f"Dette er normalt om tillatelser har flere lokaliteter, "
                        f"men 'permit_snapshot' lagrer kun én representativ rad per {KEY_COL} per dag.",
                    )
                )

    has_error = any(i.level == "ERROR" for i in issues)
    has_warn = any(i.level == "WARN" for i in issues)
    ok = (not has_error) and (not (fail_on_warn and has_warn))
    return PreflightResult(ok=ok, issues=issues, files_checked=len(files))


def print_preflight_report(result: PreflightResult) -> None:
    print(f"\nPreflight: sjekket {result.files_checked} filer.")
    if not result.issues:
        print("Ingen feil eller advarsler funnet ✅")
        return

    errors = [i for i in result.issues if i.level == "ERROR"]
    warns = [i for i in result.issues if i.level == "WARN"]

    if errors:
        print(f"\nERROR ({len(errors)}):")
        for i in errors:
            print(f" - {i.file}: {i.message}")

    if warns:
        print(f"\nWARN ({len(warns)}):")
        for i in warns:
            print(f" - {i.file}: {i.message}")


# ----------------------------
# Hovedlogikk: bygg DB fra snapshots
# ----------------------------

TodayTuple = Tuple[str, str, str, str, Optional[str], str]
# (owner_orgnr, owner_name, owner_identity, row_json, tidsbegrenset, art)


def apply_snapshot(
    conn: sqlite3.Connection,
    snapshot_date: str,
    csv_path: Path,
    strict_date_validation: bool = True,
    enable_transfers: bool = True,
) -> None:
    # 0) Valider dato i første linje
    title_date = read_title_date(csv_path, encoding=CSV_ENCODING)
    if title_date != snapshot_date:
        msg = f"Datomismatch i {csv_path.name}: filnavn={snapshot_date}, første linje={title_date}"
        if strict_date_validation:
            raise ValueError(msg)
        print(f"ADVARSEL: {msg}")

    # 1) Les CSV (header på linje 2)
    df = load_snapshot_csv(csv_path)

    required = {KEY_COL, OWNER_ORG_COL, OWNER_NAME_COL}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Mangler kolonner i CSV {csv_path.name}: {sorted(missing)}")

    # 2) Bygg rader
    rows = df.to_dict(orient="records")
    # Samle alle arter per permit_key (fra alle rader i CSV, ikke bare representativ rad)
    art_map: Dict[str, set] = {}

    for r in rows:
        permit_key = normalize_permit_key(as_text(r.get(KEY_COL)))
        if not permit_key:
            continue

        a = as_text(r.get("ART"))
        if not a:
            continue

        art_map.setdefault(permit_key, set()).add(a)


    # --- NYTT: tidsbegrenset per permit_key fra ALLE rader ---
    tidsbegrenset_per_permit: Dict[str, Optional[str]] = {}
    for r in rows:
        pk = normalize_permit_key(as_text(r.get(KEY_COL)))
        if not pk:
            continue
        tb = parse_ddmmyyyy(as_text(r.get("TIDSBEGRENSET")))
        if not tb:
            continue
        prev = tidsbegrenset_per_permit.get(pk)
        if not prev:
            tidsbegrenset_per_permit[pk] = tb
        else:
            tidsbegrenset_per_permit[pk] = max(prev, tb)

    def lok_nr_sort_key(v) -> int:
        s = as_text(v)
        if not s:
            return 10**18
        s2 = s.replace(".0", "")
        return int(s2) if s2.isdigit() else 10**18

    def rep_row_key(r: dict) -> tuple:
        return (
            lok_nr_sort_key(r.get("LOK_NR")),
            as_text(r.get("LOK_KOMNR")),
            as_text(r.get("LOK_NAVN")),
            as_text(r.get("LOK_PLASS")),
            as_text(r.get("VANNMILJØ")),
            as_text(r.get("N_GEOWGS84")),
            as_text(r.get("Ø_GEOWGS84")),
        )

    today: Dict[str, TodayTuple] = {}
    best_key: Dict[str, tuple] = {}

    for r in rows:
        permit_key = normalize_permit_key(as_text(r.get(KEY_COL)))
        if not permit_key:
            continue

        k = rep_row_key(r)
        if permit_key in best_key and k >= best_key[permit_key]:
            continue

        owner_org = as_text(r.get(OWNER_ORG_COL))
        owner_name = as_text(r.get(OWNER_NAME_COL))
        owner_identity = make_owner_identity(owner_org, owner_name)
        row_json = canonical_json(r)

        tidsbegrenset = tidsbegrenset_per_permit.get(permit_key)

        # Aggregert art-streng for permit (fra ALLE rader)
        art = ", ".join(sorted(art_map.get(permit_key, set())))

        best_key[permit_key] = k
        today[permit_key] = (owner_org, owner_name, owner_identity, row_json, tidsbegrenset, art)


    today_keys = set(today.keys())

    # 3) Hent forrige "current"
    cur = conn.cursor()
    cur.execute("SELECT permit_key, owner_orgnr, owner_name, owner_identity FROM permit_current;")
    current: Dict[str, Tuple[str, str, str]] = {
        k: (o or "", n or "", i or "") for (k, o, n, i) in cur.fetchall()
    }

    current_keys = set(current.keys())

    new_keys = today_keys - current_keys
    removed_keys = current_keys - today_keys
    common_keys = today_keys & current_keys

    # 4) Lagre permit_snapshot (kun hvis endret)
    snapshots_written = 0
    snapshots_skipped = 0

    for permit_key, (_org, _name, _ident, row_json, _tb, _art) in today.items():

        row_hash = sha256_text(row_json)
        prev_hash = latest_snapshot_hash(conn, permit_key)
        if prev_hash == row_hash:
            snapshots_skipped += 1
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO permit_snapshot(snapshot_date, permit_key, row_json, row_hash)
            VALUES (?, ?, ?, ?);
            """,
            (snapshot_date, permit_key, row_json, row_hash),
        )
        snapshots_written += 1

    # 5) Ownership history (SCD2 på eier, via owner_identity)
    close_date = prev_day(snapshot_date)

    def close_open_ownership(permit_key: str) -> None:
        conn.execute(
            """
            UPDATE ownership_history
            SET valid_to = ?
            WHERE permit_key = ?
              AND (valid_to IS NULL OR valid_to = '')
              AND date(valid_from) < date(?);
            """,
            (close_date, permit_key, snapshot_date),
        )

    def open_new_ownership(
        permit_key: str,
        owner_org: str,
        owner_name: str,
        owner_identity: str,
        tidsbegrenset: Optional[str],
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO ownership_history(
                permit_key, owner_orgnr, owner_name, owner_identity, valid_from, valid_to, tidsbegrenset
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?);
            """,
            (permit_key, owner_org or None, owner_name or None, owner_identity, snapshot_date, tidsbegrenset),
        )

    def update_open_tidsbegrenset(permit_key: str, tidsbegrenset: Optional[str]) -> None:
        if not tidsbegrenset:
            return
        conn.execute(
            """
            UPDATE ownership_history
            SET tidsbegrenset = COALESCE(tidsbegrenset, ?)
            WHERE permit_key = ?
              AND (valid_to IS NULL OR valid_to = '');
            """,
            (tidsbegrenset, permit_key),
        )

    # Nye tillatelser => åpne eierperiode
    for permit_key in sorted(new_keys):
        owner_org, owner_name, owner_identity, _row_json, tb, _art = today[permit_key]
        open_new_ownership(permit_key, owner_org, owner_name, owner_identity, tb)

    # Fjernede tillatelser => lukk åpen periode
    for permit_key in sorted(removed_keys):
        close_open_ownership(permit_key)

    # Eierskifte
    owner_changes = 0
    for permit_key in sorted(common_keys):
        _prev_owner_org, _prev_owner_name, prev_owner_identity = current[permit_key]
        owner_org, owner_name, owner_identity, _row_json, tb, _art = today[permit_key]

        if owner_identity != prev_owner_identity:
            owner_changes += 1
            close_open_ownership(permit_key)
            open_new_ownership(permit_key, owner_org, owner_name, owner_identity, tb)

            # Transfers-integrasjon (kun når orgnr finnes)
            if enable_transfers and as_text(owner_org):
                try:
                    from src.transfers import fetch_transfers, upsert_transfers, update_ownership_with_transfer

                    transfers_json = fetch_transfers(permit_key)
                    upsert_transfers(conn, permit_key, transfers_json)

                    update_ownership_with_transfer(
                        conn=conn,
                        permit_key=permit_key,
                        new_owner_orgnr=owner_org,
                        valid_from=snapshot_date,
                    )
                except Exception as e:
                    print(f"ADVARSEL: Transfers-integrasjon feilet for {permit_key}: {e}")

        # Samme eier: men kan ha fått tidsbegrenset i dag
        update_open_tidsbegrenset(permit_key, tb)

    # Generell pass (trygg) for alle tillatelser i dag
    for permit_key in sorted(today_keys):
        _o, _n, _i, _j, tb, _art = today[permit_key]
        update_open_tidsbegrenset(permit_key, tb)

    # 6) Oppdater permit_current
    for permit_key in sorted(removed_keys):
        conn.execute("DELETE FROM permit_current WHERE permit_key = ?;", (permit_key,))

    for permit_key in sorted(today_keys):
        owner_org, owner_name, owner_identity, row_json, _tb, art = today[permit_key]

        try:
            row_dict = json.loads(row_json) if row_json else {}
        except Exception:
            row_dict = {}

        grunn = 1 if is_grunnrente_pliktig(row_dict) else 0

        conn.execute(
            """
            INSERT INTO permit_current(
                permit_key, owner_orgnr, owner_name, owner_identity, snapshot_date, row_json, grunnrente_pliktig
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(permit_key) DO UPDATE SET
                owner_orgnr=excluded.owner_orgnr,
                owner_name=excluded.owner_name,
                owner_identity=excluded.owner_identity,
                snapshot_date=excluded.snapshot_date,
                row_json=excluded.row_json,
                grunnrente_pliktig=excluded.grunnrente_pliktig;
            """,
            (permit_key, owner_org or None, owner_name or None, owner_identity, snapshot_date, row_json, grunn),
        )

    # 7) Marker snapshot
    conn.execute(
        """
        INSERT OR REPLACE INTO snapshots(snapshot_date, filename, ingested_at)
        VALUES (?, ?, ?);
        """,
        (snapshot_date, csv_path.name, utc_now_iso()),
    )

    conn.commit()

    print(
        f"  Stats {snapshot_date}: "
        f"tillatelser={len(today_keys)} "
        f"new={len(new_keys)} "
        f"removed={len(removed_keys)} "
        f"owner_changes={owner_changes} "
        f"snapshots_written={snapshots_written} "
        f"skipped={snapshots_skipped}"
    )


def build_from_folder(
    run_preflight_first: bool = True,
    fail_on_warn: bool = False,
    strict_date_validation: bool = True,
    enable_transfers: bool = True,
) -> None:
    if run_preflight_first:
        pf = preflight(SNAPSHOT_DIR, fail_on_warn=fail_on_warn)
        print_preflight_report(pf)
        if not pf.ok:
            raise SystemExit("Preflight feilet. Rett feilene over før du bygger historikk.")

    conn = connect()
    init_db(conn)

    # START CLEAN
    conn.execute("DELETE FROM permit_current;")
    conn.execute("DELETE FROM ownership_history;")
    conn.execute("DELETE FROM permit_snapshot;")
    conn.execute("DELETE FROM snapshots;")
    conn.commit()

    files = sorted(SNAPSHOT_DIR.glob("*.csv"))
    if not files:
        raise SystemExit(f"Ingen CSV-filer funnet i {SNAPSHOT_DIR}")

    for p in files:
        d = date_from_filename(p)
        print(f"Prosesserer {d} - {p.name}")
        apply_snapshot(
            conn=conn,
            snapshot_date=d,
            csv_path=p,
            strict_date_validation=strict_date_validation,
            enable_transfers=enable_transfers,
        )

    print("\nFerdig. Historikk + snapshots er bygget.")


if __name__ == "__main__":
    build_from_folder(
        run_preflight_first=True,
        fail_on_warn=False,
        strict_date_validation=True,
        enable_transfers=True,
    )
