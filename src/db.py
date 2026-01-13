# src/db.py
from __future__ import annotations

import sqlite3
from src.config import DB_PATH


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r["name"] == col for r in rows)


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    if not _has_column(conn, table, col):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type};")


def init_db(conn: sqlite3.Connection) -> None:
    # ----------------------------
    # snapshots
    # ----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_date TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        );
        """
    )

    # ----------------------------
    # permit_current (NOW-state)
    # ----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS permit_current (
            permit_key TEXT PRIMARY KEY,
            owner_orgnr TEXT,
            owner_name TEXT,
            owner_identity TEXT,              -- ALLTID satt i ingest
            snapshot_date TEXT NOT NULL,
            row_json TEXT NOT NULL,
            art TEXT,                         -- aggregert art-streng (kan inneholde flere arter)
            grunnrente_pliktig INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # migrasjoner for eksisterende DB
    _add_column_if_missing(conn, "permit_current", "owner_identity", "TEXT")
    _add_column_if_missing(conn, "permit_current", "grunnrente_pliktig", "INTEGER NOT NULL DEFAULT 0")
    _add_column_if_missing(conn, "permit_current", "art", "TEXT")

    # Backfill (sikkerhet)
    conn.execute(
        """
        UPDATE permit_current
        SET owner_identity = COALESCE(owner_identity, '')
        WHERE owner_identity IS NULL;
        """
    )

    # ----------------------------
    # permit_snapshot (daily sparse snapshot per permit)
    # ----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS permit_snapshot (
            snapshot_date TEXT NOT NULL,
            permit_key TEXT NOT NULL,
            row_json TEXT NOT NULL,
            row_hash TEXT NOT NULL,
            art TEXT,
            PRIMARY KEY (snapshot_date, permit_key)
        );
        """
    )

    # migrasjoner
    _add_column_if_missing(conn, "permit_snapshot", "row_hash", "TEXT")
    _add_column_if_missing(conn, "permit_snapshot", "art", "TEXT")

    # Backfill (sikkerhet)
    conn.execute(
        """
        UPDATE permit_snapshot
        SET row_hash = COALESCE(row_hash, '')
        WHERE row_hash IS NULL;
        """
    )

    # ----------------------------
    # ownership_history (SCD2)
    # ----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ownership_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_key TEXT NOT NULL,
            owner_orgnr TEXT,
            owner_name TEXT,
            owner_identity TEXT NOT NULL,     -- ALLTID satt (orgnr eller PN:<name>)
            valid_from TEXT NOT NULL,
            valid_to TEXT,
            tidsbegrenset TEXT,               -- dato (ISO) hvis tidsbegrenset
            registered_from TEXT,             -- faktisk registreringsdato (dagbokdato/ajour)
            registered_to TEXT,               -- valgfritt
            transfer_id INTEGER               -- peker til license_transfers.id (valgfritt)
        );
        """
    )

    # migrasjoner
    _add_column_if_missing(conn, "ownership_history", "owner_identity", "TEXT")
    _add_column_if_missing(conn, "ownership_history", "registered_from", "TEXT")
    _add_column_if_missing(conn, "ownership_history", "registered_to", "TEXT")
    _add_column_if_missing(conn, "ownership_history", "transfer_id", "INTEGER")
    _add_column_if_missing(conn, "ownership_history", "tidsbegrenset", "TEXT")

    # Backfill
    conn.execute(
        """
        UPDATE ownership_history
        SET owner_identity = COALESCE(owner_identity, '')
        WHERE owner_identity IS NULL;
        """
    )

    # ----------------------------
    # license_transfers (API cache)
    # ----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS license_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            permit_key TEXT NOT NULL,
            transfer_key TEXT,              -- hvis API har id/uuid
            journal_date TEXT,              -- dagbokdato
            updated_at TEXT,                -- ajourføring/oppdatert
            current_owner_orgnr TEXT,
            current_owner_name TEXT,
            raw_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );
        """
    )

    # ----------------------------
    # Indekser
    # ----------------------------
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_owner ON permit_current(owner_orgnr);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_current_owner_ident ON permit_current(owner_identity);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_key ON permit_snapshot(permit_key, snapshot_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_hash ON permit_snapshot(permit_key, row_hash);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ownerhist_key ON ownership_history(permit_key, valid_from);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ownerhist_owner ON ownership_history(owner_orgnr, valid_from);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ownerhist_ident ON ownership_history(owner_identity, valid_from);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_transfers_permit ON license_transfers(permit_key, journal_date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transfers_owner ON license_transfers(current_owner_orgnr, journal_date);")

    # Unik indeks: hindrer duplikat-start av samme eierperiode
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ownership_period_start
        ON ownership_history(permit_key, owner_identity, valid_from);
        """
    )

    # Hindrer flere aktive perioder per permit (partial unique index)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_ownership_per_permit
        ON ownership_history(permit_key)
        WHERE (valid_to IS NULL OR valid_to = '');
        """
    )

    conn.commit()
