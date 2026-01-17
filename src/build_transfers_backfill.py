#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests

from src.db import connect, init_db
from src.transfers import fetch_transfers, upsert_transfers, update_ownership_with_transfer


# -----------------------------
# Små hjelpere / policy
# -----------------------------

@dataclass
class RetryPolicy:
    max_attempts: int = 12
    base_sleep: float = 1.0
    max_sleep: float = 120.0
    jitter: float = 0.25  # +- 25%


def now_epoch() -> int:
    return int(time.time())


def sleep_with_jitter(seconds: float, jitter: float) -> None:
    if seconds <= 0:
        return
    delta = seconds * jitter
    time.sleep(max(0.0, seconds + random.uniform(-delta, +delta)))


class RateLimitError(Exception):
    def __init__(self, retry_after_s: Optional[int], msg: str = "rate limited"):
        super().__init__(msg)
        self.retry_after_s = retry_after_s


class TransientAPIError(Exception):
    pass


def compute_backoff(policy: RetryPolicy, attempt_no: int, retry_after_s: Optional[int]) -> float:
    """
    attempt_no: 1..N (inne i én permit-jobb)
    """
    if retry_after_s is not None:
        return min(float(retry_after_s), policy.max_sleep)
    base = policy.base_sleep * (2 ** (attempt_no - 1))
    return min(base, policy.max_sleep)


# -----------------------------
# Queue-tabell (restartbarhet)
# -----------------------------

def ensure_queue_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transfers_backfill_queue (
            permit_key     TEXT PRIMARY KEY,
            status         TEXT NOT NULL CHECK(status IN ('pending','in_progress','done','failed')),
            attempts       INTEGER NOT NULL DEFAULT 0,
            next_retry_at  INTEGER,
            locked_at      INTEGER,
            lock_owner     TEXT,
            last_error     TEXT,
            updated_at     INTEGER NOT NULL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tbq_status ON transfers_backfill_queue(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tbq_next_retry ON transfers_backfill_queue(next_retry_at);")
    conn.commit()


def seed_queue(conn: sqlite3.Connection, limit: Optional[int] = None) -> int:
    sql = "SELECT permit_key FROM permit_current ORDER BY permit_key"
    rows = conn.execute(sql).fetchall()
    keys = [r["permit_key"] for r in rows]
    if limit is not None:
        keys = keys[:limit]

    t = now_epoch()
    inserted = 0
    for k in keys:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO transfers_backfill_queue
            (permit_key, status, attempts, next_retry_at, locked_at, lock_owner, last_error, updated_at)
            VALUES (?, 'pending', 0, NULL, NULL, NULL, NULL, ?)
            """,
            (k, t),
        )
        inserted += cur.rowcount

    conn.commit()
    return inserted


def queue_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT status, COUNT(*) AS c
        FROM transfers_backfill_queue
        GROUP BY status
        """
    ).fetchall()
    d = {r["status"]: r["c"] for r in rows}
    for k in ("pending", "in_progress", "done", "failed"):
        d.setdefault(k, 0)
    return d


def claim_one(conn: sqlite3.Connection, lock_owner: str, lock_timeout_s: int) -> Optional[str]:
    """
    Atomisk claim av én permit:
    - tar pending
    - eller failed som er klar for retry
    - eller stale in_progress (crash recovery)
    """
    t = now_epoch()
    stale_before = t - lock_timeout_s

    row = conn.execute(
        """
        SELECT permit_key
        FROM transfers_backfill_queue
        WHERE
            (
              status = 'pending'
              OR (status = 'failed' AND next_retry_at IS NOT NULL AND next_retry_at <= ?)
              OR (status = 'in_progress' AND (locked_at IS NULL OR locked_at <= ?))
            )
        ORDER BY
            CASE status
              WHEN 'pending' THEN 0
              WHEN 'failed' THEN 1
              WHEN 'in_progress' THEN 2
              ELSE 9
            END,
            COALESCE(next_retry_at, 0),
            updated_at
        LIMIT 1
        """,
        (t, stale_before),
    ).fetchone()

    if not row:
        return None

    permit_key = row["permit_key"]

    cur = conn.execute(
        """
        UPDATE transfers_backfill_queue
        SET status='in_progress', locked_at=?, lock_owner=?, updated_at=?
        WHERE permit_key=?
          AND (
              status='pending'
              OR (status='failed' AND next_retry_at IS NOT NULL AND next_retry_at <= ?)
              OR (status='in_progress' AND (locked_at IS NULL OR locked_at <= ?))
          )
        """,
        (t, lock_owner, t, permit_key, t, stale_before),
    )
    conn.commit()

    if cur.rowcount != 1:
        return None
    return permit_key


def mark_done(conn: sqlite3.Connection, permit_key: str) -> None:
    t = now_epoch()
    conn.execute(
        """
        UPDATE transfers_backfill_queue
        SET status='done', locked_at=NULL, lock_owner=NULL, last_error=NULL, updated_at=?
        WHERE permit_key=?
        """,
        (t, permit_key),
    )
    conn.commit()


def mark_failed(conn: sqlite3.Connection, permit_key: str, attempts: int, err: str, retry_in_s: Optional[int]) -> None:
    t = now_epoch()
    next_retry = (t + retry_in_s) if retry_in_s is not None else None
    conn.execute(
        """
        UPDATE transfers_backfill_queue
        SET status='failed',
            attempts=?,
            next_retry_at=?,
            locked_at=NULL,
            lock_owner=NULL,
            last_error=?,
            updated_at=?
        WHERE permit_key=?
        """,
        (attempts, next_retry, err[:2000], t, permit_key),
    )
    conn.commit()


# -----------------------------
# Din business-logikk
# -----------------------------

def fetch_transfers_with_handling(permit_key: str, timeout: int = 60) -> Dict[str, Any]:
    """
    Wrapper rundt din fetch_transfers() som oversetter requests-feil
    til RateLimitError / TransientAPIError.
    """
    try:
        return fetch_transfers(permit_key, timeout=timeout)

    except requests.exceptions.Timeout as e:
        raise TransientAPIError(f"Timeout: {e}") from e

    except requests.exceptions.ConnectionError as e:
        raise TransientAPIError(f"ConnectionError: {e}") from e

    except requests.exceptions.HTTPError as e:
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", None)
        if status == 404:
            return {"ajourDate": None, "transfers": []}


        # 429: rate limit
        if status == 429:
            retry_after = None
            if resp is not None:
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        retry_after = int(ra.strip())
                    except Exception:
                        retry_after = None
            raise RateLimitError(retry_after_s=retry_after, msg=f"HTTP 429 for {permit_key}") from e

        # 5xx: typisk transient
        if status is not None and 500 <= int(status) <= 599:
            raise TransientAPIError(f"HTTP {status} for {permit_key}") from e

        # andre 4xx: vanligvis “hard fail”
        raise

    except Exception:
        # ukjent feil: bubble opp som “hard fail”
        raise


def update_current_ownership_period_from_transfers(conn: sqlite3.Connection, permit_key: str) -> bool:
    """
    Finn nåværende ownership_history-periode (valid_to IS NULL) og
    bruk din update_ownership_with_transfer() for å sette registered_from + transfer_id.
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
        return False

    owner_orgnr = (row["owner_orgnr"] or "").strip()
    valid_from = row["valid_from"]

    if not owner_orgnr or not valid_from:
        return False

    update_ownership_with_transfer(conn, permit_key, owner_orgnr, valid_from)
    return True


def process_one_permit(conn: sqlite3.Connection, permit_key: str, policy: RetryPolicy, timeout: int) -> None:
    """
    1) Hent transfers
    2) Upsert til license_transfers
    3) Oppdater ownership_history (nåværende periode)
    4) Mark done / failed med backoff
    """
    qrow = conn.execute(
        "SELECT attempts FROM transfers_backfill_queue WHERE permit_key=?",
        (permit_key,),
    ).fetchone()
    attempts_total = int(qrow["attempts"]) if qrow else 0

    attempt_no = 0
    while True:
        attempt_no += 1
        attempts_total += 1

        try:
            transfers_json = fetch_transfers_with_handling(permit_key, timeout=timeout)
            upsert_transfers(conn, permit_key, transfers_json)

            # Prøv å koble mot ownership_history (ikke kritisk om den ikke finner noe)
            try:
                update_current_ownership_period_from_transfers(conn, permit_key)
            except Exception as e:
                # Ikke stopp backfillen av den grunn – men logg i køa om du vil
                # Her velger vi å la permit bli "done" uansett om ownership-update feiler,
                # siden transfers-dataene allerede er lagret.
                pass

            mark_done(conn, permit_key)
            return

        except RateLimitError as e:
            if attempts_total >= policy.max_attempts:
                mark_failed(conn, permit_key, attempts_total, f"RateLimitError: {e}", retry_in_s=None)
                return
            backoff = compute_backoff(policy, attempt_no, e.retry_after_s)
            mark_failed(conn, permit_key, attempts_total, f"RateLimitError: {e}", retry_in_s=int(backoff))

            # NYTT: global “cooldown” så vi ikke hammer API-et
            sleep_with_jitter(min(backoff, 10), policy.jitter)
            return


        except TransientAPIError as e:
            if attempts_total >= policy.max_attempts:
                mark_failed(conn, permit_key, attempts_total, f"TransientAPIError: {e}", retry_in_s=None)
                return
            backoff = compute_backoff(policy, attempt_no, None)
            mark_failed(conn, permit_key, attempts_total, f"TransientAPIError: {e}", retry_in_s=int(backoff))
            return

        except Exception as e:
            # Hard fail (f.eks. 404/400/ukjent schema)
            mark_failed(conn, permit_key, attempts_total, f"Unhandled: {type(e).__name__}: {e}", retry_in_s=None)
            return


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="One-time backfill of license transfers (restartable queue).")
    p.add_argument("--seed", action="store_true", help="Seed queue from permit_current (idempotent).")
    p.add_argument("--seed-limit", type=int, default=None, help="Seed only first N permits (testing).")
    p.add_argument("--max-items", type=int, default=0, help="Process at most N permits (0 = no limit).")
    p.add_argument("--stats-every", type=int, default=50, help="Print stats every N processed.")
    p.add_argument("--sleep-empty", type=float, default=2.0, help="Sleep when no work is available.")
    p.add_argument("--lock-timeout", type=int, default=1800, help="Seconds before in_progress is considered stale.")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds for fetch_transfers().")
    args = p.parse_args()

    lock_owner = f"{os.uname().nodename}:{os.getpid()}"
    policy = RetryPolicy()

    conn = connect()
    init_db(conn)
    ensure_queue_table(conn)

    if args.seed:
        inserted = seed_queue(conn, limit=args.seed_limit)
        print(f"[seed] inserted={inserted} stats={queue_stats(conn)}")

    processed = 0
    try:
        while True:
            if args.max_items and processed >= args.max_items:
                print(f"[done] reached --max-items={args.max_items}")
                print(f"[final] stats={queue_stats(conn)}")
                return
        
            
            permit_key = claim_one(conn, lock_owner=lock_owner, lock_timeout_s=args.lock_timeout)
            if not permit_key:
                st = queue_stats(conn)
                print(f"[idle] stats={st} sleeping {args.sleep_empty}s")
                time.sleep(args.sleep_empty)
                continue

            process_one_permit(conn, permit_key, policy=policy, timeout=args.timeout)
            processed += 1
            time.sleep(0.2)

            if processed % args.stats_every == 0:
                st = queue_stats(conn)
                total_transfers = conn.execute("SELECT COUNT(*) AS c FROM license_transfers").fetchone()["c"]
                print(f"[progress] processed={processed} stats={st} license_transfers_total={total_transfers}")
    
    except KeyboardInterrupt:
        print("\n[stopped] KeyboardInterrupt")
        print(f"[stopped] stats={queue_stats(conn)}")
        return

if __name__ == "__main__":
    main()
