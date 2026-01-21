from __future__ import annotations

from datetime import date
from pathlib import Path
import os
import subprocess

from src.config import SNAPSHOT_DIR

URL = "https://api.fiskeridir.no/pub-aqua/api/v1/dump/new-legacy-csv"


def _is_probably_complete_csv(path: Path) -> bool:
    """
    Lettvekts-sjekk for å fange typiske 'curl døde midt i nedlasting'-filer:
    - fila må ikke være tom / veldig liten
    - siste linje må ikke se avkuttet ut (f.eks. ende med '%')
    - siste linje må ha 'mange nok' semikolon (forventet mange felt i dette datasettet)

    Dette er heuristikk, men fanger problemet du så 21. januar.
    """
    try:
        st = path.stat()
    except FileNotFoundError:
        return False

    # Grov størrelse: dumpen er typisk flere MB. Sett lav terskel for å fange avbrutte filer.
    if st.st_size < 200_000:  # 200 KB
        return False

    try:
        with path.open("rb") as f:
            # Les siste ~8KB for å finne siste linje uten å lese hele fila
            f.seek(max(0, st.st_size - 8192))
            tail = f.read()
    except OSError:
        return False

    # Del på linjeskift og plukk siste ikke-tomme linje
    lines = [ln for ln in tail.splitlines() if ln.strip()]
    if not lines:
        return False

    last = lines[-1].decode("utf-8", errors="replace")

    # Klassisk symptom hos deg: avkuttet linje som ender med '%'
    if last.rstrip().endswith("%"):
        return False

    # Forvent mange felt i disse radene (mange ';'). Sett konservativ terskel.
    if last.count(";") < 10:
        return False

    return True


def _download_to_tmp(tmp: Path) -> None:
    tmp.parent.mkdir(parents=True, exist_ok=True)

    # curl: robust nedlasting
    # -sS: stille, men viser feil
    # --fail-with-body: 4xx/5xx gir exit!=0 og body i stderr
    # --retry-all-errors: retry også på nettfeil (inkl. 56)
    # timeouts for å unngå å henge i timevis
    cmd = [
        "curl",
        "-sS",
        "-L",
        "--fail-with-body",
        "--retry", "10",
        "--retry-all-errors",
        "--retry-delay", "5",
        "--connect-timeout", "20",
        "--max-time", "600",
        "-X", "GET", URL,
        "-H", "accept: text/plain;charset=UTF-8",
        "-o", str(tmp),
    ]

    subprocess.run(cmd, check=True)


def main() -> None:
    today = date.today().isoformat()
    out = SNAPSHOT_DIR / f"{today} - Uttrekk fra Akvakulturregisteret.csv"
    tmp = out.with_suffix(out.suffix + ".tmp")

    # Hvis ferdig fil finnes, valider at den ser hel ut. Hvis ikke: slett og last ned på nytt.
    if out.exists():
        if _is_probably_complete_csv(out):
            print(f"Finnes allerede: {out.name}")
            return
        print(f"Fant korrupt/avbrutt fil: {out.name} — laster ned på nytt")
        try:
            out.unlink()
        except OSError:
            # Hvis vi ikke får slettet, feile tydelig (heller enn å fortsette med korrupt fil)
            raise RuntimeError(f"Klarte ikke å slette korrupt fil: {out}")

    # Rydd opp eventuell gammel tmp-fil
    if tmp.exists():
        try:
            tmp.unlink()
        except OSError:
            pass

    try:
        _download_to_tmp(tmp)

        # Valider tmp før vi "committer" den som dagens fil
        if not _is_probably_complete_csv(tmp):
            raise RuntimeError(f"Nedlastet fil ser avbrutt/korrupt ut: {tmp}")

        # Atomisk rename: enten har du gammel fil, eller helt ny – aldri halv.
        os.replace(tmp, out)

        print(f"Lastet ned {out.name}")

    except Exception:
        # Sørg for at tmp ikke blir liggende
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        raise


if __name__ == "__main__":
    main()
