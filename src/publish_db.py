from __future__ import annotations

from src.config import PROJECT_ROOT

def main() -> None:
    src_db = PROJECT_ROOT / "db" / "aqua.sqlite"
    dst_db = PROJECT_ROOT / "web" / "data" / "aqua.sqlite"

    if not src_db.exists():
        raise SystemExit(f"Fant ikke {src_db}. Kjør build_history først.")

    dst_db.parent.mkdir(parents=True, exist_ok=True)
    dst_db.write_bytes(src_db.read_bytes())
    print(f"Copied {src_db} -> {dst_db} ({dst_db.stat().st_size} bytes)")

if __name__ == "__main__":
    main()
