import json
import requests
from datetime import datetime, timezone
import re

def normalize_license_id(permit_key: str) -> str:
    """
    Gjør om f.eks. 'H F 0920' -> 'H-F-0920'
    - fjerner ekstra whitespace
    - erstatter mellomrom med '-'
    """
    s = (permit_key or "").strip()
    s = re.sub(r"\s+", " ", s)   # flere mellomrom -> ett
    s = s.replace(" ", "-")
    return s



BASE = "https://api.fiskeridir.no/pub-aqua/api/v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_transfers(permit_key: str, timeout: int = 60):
    license_id = normalize_license_id(permit_key)
    url = f"{BASE}/licenses/{license_id}/transfers"
    r = requests.get(url, headers={"accept": "application/json; charset=UTF-8"}, timeout=timeout)
    r.raise_for_status()
    return r.json()



def _pick(d: dict, keys):
    """Hjelper: returner første eksisterende felt."""
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def upsert_transfers(conn, permit_key: str, transfers_json):
    """
    Lagrer transfers-responsen fra API i license_transfers.

    Eksempel respons:
    {
      "ajourDate": "2026-01-07",
      "transfers": [
        {"identityNr":"...","journalDate":"...","journalNr":"...","officialName":"..."}
      ]
    }
    """
    fetched_at = utc_now_iso()

    if not isinstance(transfers_json, dict):
        return

    updated_at = transfers_json.get("ajourDate")  # YYYY-MM-DD
    items = transfers_json.get("transfers") or []

    for t in items:
        raw = json.dumps(t, ensure_ascii=False, default=str)

        transfer_key = t.get("journalNr")  # stabil id i ditt eksempel
        journal_date = t.get("journalDate")
        orgnr = t.get("identityNr")
        name = t.get("officialName")

        conn.execute("""
        INSERT INTO license_transfers(
        permit_key, transfer_key, journal_date, updated_at,
        current_owner_orgnr, current_owner_name, raw_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(permit_key, transfer_key) DO UPDATE SET
        journal_date=excluded.journal_date,
        updated_at=excluded.updated_at,
        current_owner_orgnr=excluded.current_owner_orgnr,
        current_owner_name=excluded.current_owner_name,
        raw_json=excluded.raw_json,
        fetched_at=excluded.fetched_at;
    """, (
        permit_key,
        str(transfer_key) if transfer_key is not None else None,
        str(journal_date) if journal_date is not None else None,
        str(updated_at) if updated_at is not None else None,
        str(orgnr) if orgnr is not None else None,
        str(name) if name is not None else None,
        raw,
        fetched_at
    ))


    conn.commit()



def update_ownership_with_transfer(conn, permit_key: str, new_owner_orgnr: str, valid_from: str):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, journal_date, updated_at
        FROM license_transfers
        WHERE permit_key = ?
          AND current_owner_orgnr = ?
        ORDER BY COALESCE(journal_date, updated_at) DESC
        LIMIT 1;
    """, (permit_key, new_owner_orgnr))

    row = cur.fetchone()
    if not row:
        return

    transfer_id, journal_date, updated_at = row
    registered_from = journal_date or updated_at

    conn.execute("""
        UPDATE ownership_history
        SET registered_from = ?, transfer_id = ?
        WHERE permit_key = ?
          AND valid_from = ?
          AND owner_orgnr = ?
          AND valid_to IS NULL;
    """, (registered_from, transfer_id, permit_key, valid_from, new_owner_orgnr))

    conn.commit()

