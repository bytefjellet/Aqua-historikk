# src/update_transfers_daily.py
from __future__ import annotations

import time
import random
import sqlite3
from typing import List, Optional

import requests

from src.db import connect, init_db
from src.transfers import fetch_transfers, upsert_transfers, update_ownership_with_transfer


def now_epoch() -> int:
    return int(time.time())


def sleep_jitter(seconds: float, jitter: float = 0.25) -> None:
    if seconds <= 0:
        return
    delta = seconds * jitter
    time.sleep(max(0.0, seconds + random.uniform(-delta, +delta)))


def ensure_state_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS transfers_fetch_state (
        permit_key TEXT PRIMARY KEY,
        last_fetched_at INTEGER,
        last_ajour_date TEXT,
        last_status TEXT,
        last_error TEXT,
        next_retry_at INTEGER
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tfs_next_retry ON transfers_fetch_state(next_retry_at);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tfs_last_fetched ON transfers_fetch_state(last_fetched_at);")
    conn.commit()


def pick_today_permits(
    conn: sqlite3.Connection,
    *,
    changed_since_days: int = 7,
    stale_after_days: int = 30,
    limit: int = 300,
) -> List[str]:
    """
    Velger et fornuftig dagsutvalg:
    1) permits med ownership-endring nylig
    2) permits som ikke er sjekket på en stund
    """
    t = now_epoch()
    changed_since_date = time.strftime("%Y-%m-%d", time.gmtime(t - changed_since_days * 86400))
    stale_before = t - stale_after_days * 86400

    # 1) nylige endringer
    recent = conn.execute(
        """
        SELECT DISTINCT oh.permit_key
        FROM ownership_history oh
        WHERE oh.valid_from >= ?
        """,
        (changed_since_date,),
    ).fetchall()
    recent_keys = [r["permit_key"] for r in recent]

    # 2) “stale” / aldri sjekket, og ikke i cooldown (next_retry_at)
    stale = conn.execute(
        """
        SELECT pc.permit_key
        FROM permit_current pc
        LEFT JOIN transfers_fetch_state s ON s.permit_key = pc.permit_key
        WHERE
          (s.last_fetched_at IS NULL OR s.last_fetched_at < ?)
          AND (s.next_retry_at IS NULL OR s.next_retry_at <= ?)
        ORDER BY COALESCE(s.last_fetched_at, 0) ASC, pc.permit_key
        LIMIT ?
        """,
        (stale_before, t, limit),
    ).fetchall()
    stale_keys = [r["permit_key"] for r in stale]

    # Kombiner, behold rekkefølge med recent først
    out = []
    seen = set()
    for k in recent_keys + stale_keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
        if len(out) >= limit:
            break
    return out


def update_current_ownership_period_from_transfers(conn: sqlite3.Connection, permit_key: str) -> None:
    """
    Oppdaterer nåværende ownership_history-periode (valid_to IS NULL) basert på transfers.
    """
    row = conn.execute(
        """
        SELECT owner_orgnr, valid_from
        FROM ownership_history
        WHERE permit_key = ? AND valid_to IS NULL
        ORDER BY valid_from DESC
        LIMIT 1
        """,
        (permit_key,),
    ).fetchone()

    if not row:
        return

    owner_orgnr = (row["owner_orgnr"] or "").strip()
    valid_from = row["valid_from"]
    if not owner_orgnr or not valid_from:
        return

    update_ownership_with_transfer(conn, permit_key, owner_orgnr, valid_from)


def mark_state(
    conn: sqlite3.Connection,
    permit_key: str,
    *,
    status: str,
    ajour_date: Optional[str],
    err: Optional[str],
    next_retry_at: Optional[int],
) -> None:
    t = now_epoch()
    conn.execute(
        """
        INSERT INTO transfers_fetch_state(permit_key, last_fetched_at, last_ajour_date, last_status, last_error, next_retry_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(permit_key) DO UPDATE SET
          last_fetched_at=excluded.last_fetched_at,
          last_ajour_date=excluded.last_ajour_date,
          last_status=excluded.last_status,
          last_error=excluded.last_error,
          next_retry_at=excluded.next_retry_at;
        """,
        (permit_key, t, ajour_date, status, (err or "")[:2000] if err else None, next_retry_at),
    )
    conn.commit()


def run_daily(
    *,
    limit: int = 300,
    pace_seconds: float = 0.2,
    timeout: int = 60,
    changed_since_days: int = 7,
    stale_after_days: int = 30,
    max_rate_limited: int = 50,
) -> None:

    conn = connect()
    init_db(conn)
    ensure_state_table(conn)

    keys = pick_today_permits(
        conn,
        changed_since_days=changed_since_days,
        stale_after_days=stale_after_days,
        limit=limit,
    )

    print(f"[daily] permits_to_check={len(keys)} limit={limit}")

    ok = 0
    rate_limited = 0
    failed = 0

    for i, k in enumerate(keys, start=1):
        try:
            data = fetch_transfers(k, timeout=timeout)  # bruker din eksisterende requests-kode
            upsert_transfers(conn, k, data)
            ajour = data.get("ajourDate") if isinstance(data, dict) else None

            # Oppdater kobling for nåværende periode
            try:
                update_current_ownership_period_from_transfers(conn, k)
            except Exception:
                # ikke stopp den daglige jobben for dette
                pass

            mark_state(conn, k, status="ok", ajour_date=ajour, err=None, next_retry_at=None)
            ok += 1

        except requests.exceptions.HTTPError as e:
            resp = getattr(e, "response", None)
            status_code = getattr(resp, "status_code", None)

            # 404 kan tolkes som “ingen transfers” → ok
            if status_code == 404:
                upsert_transfers(conn, k, {"ajourDate": None, "transfers": []})
                mark_state(conn, k, status="ok_404_no_transfers", ajour_date=None, err=None, next_retry_at=None)
                ok += 1

            # 429: sett cooldown (retry om litt) og gjør global cooldown
            elif status_code == 429:
                retry_after = None
                if resp is not None:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_after = int(ra.strip())
                        except Exception:
                            retry_after = None

                backoff = retry_after if retry_after is not None else 30
                next_retry_at = now_epoch() + backoff
                mark_state(conn, k, status="rate_limited", ajour_date=None, err=f"HTTP 429 for {k}", next_retry_at=next_retry_at)

                rate_limited += 1
                sleep_jitter(min(backoff, 10))  # global cooldown

            else:
                mark_state(conn, k, status="failed", ajour_date=None, err=f"HTTPError {status_code}: {e}", next_retry_at=None)
                failed += 1

        except requests.exceptions.Timeout as e:
            mark_state(conn, k, status="failed", ajour_date=None, err=f"Timeout: {e}", next_retry_at=None)
            failed += 1

        except Exception as e:
            mark_state(conn, k, status="failed", ajour_date=None, err=f"{type(e).__name__}: {e}", next_retry_at=None)
            failed += 1

        if i % 50 == 0:
            print(f"[daily] {i}/{len(keys)} ok={ok} rate_limited={rate_limited} failed={failed}")

        time.sleep(pace_seconds)

    
    print(f"[daily] done ok={ok} rate_limited={rate_limited} failed={failed}")

    # Optional: fail loudly if API is throttling heavily
    if rate_limited > max_rate_limited:
        raise SystemExit(2)



def parse_args():
    import argparse

    p = argparse.ArgumentParser(description="Daily transfers updater (rate-limit friendly).")
    p.add_argument("--limit", type=int, default=300, help="How many permits to check per run.")
    p.add_argument("--pace", type=float, default=0.2, help="Sleep seconds between permits.")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds.")
    p.add_argument("--changed-since-days", type=int, default=7, help="Always include permits with ownership changes since N days.")
    p.add_argument("--stale-after-days", type=int, default=30, help="Also include permits not checked for N days.")
    p.add_argument("--max-rate-limited", type=int, default=50, help="Fail run if rate-limited count exceeds this.")

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # plumb args into your selector + runner
    # Easiest: pass through to pick_today_permits via run_daily by adding params
    run_daily(
        limit=args.limit,
        pace_seconds=args.pace,
        timeout=args.timeout,
        changed_since_days=args.changed_since_days,
        stale_after_days=args.stale_after_days,
    )

