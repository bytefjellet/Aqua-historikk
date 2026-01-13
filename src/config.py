from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]

def _env_path(name: str, default: Path) -> Path:
    v = os.environ.get(name)
    return Path(v) if v else default

SNAPSHOT_DIR = _env_path("AQUA_SNAPSHOT_DIR", PROJECT_ROOT / "data" / "snapshots")
DB_PATH = _env_path("AQUA_DB_PATH", PROJECT_ROOT / "data" / "aqua.sqlite")

CSV_SEPARATOR = ";"
CSV_ENCODING = "utf-8-sig"
SKIP_FIRST_LINE = 1

KEY_COL = "TILL_NR"
OWNER_ORG_COL = "ORG.NR/PERS.NR"
OWNER_NAME_COL = "NAVN"
