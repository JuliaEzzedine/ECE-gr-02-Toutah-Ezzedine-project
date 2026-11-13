"""Download the IMDb non-commercial dataset files.

Usage: python -m src.prep.download [--force]
"""
import argparse
import time

import requests

from src.common.config import IMDB_BASE_URL, RAW_DIR, TABLES

CHUNK_SIZE = 1 << 20  # 1 MiB


def download(table: str, force: bool = False, retries: int = 3) -> None:
    target = RAW_DIR / f"{table}.tsv.gz"
    if target.exists() and not force:
        print(f"[skip] {target.name} already present")
        return

    url = f"{IMDB_BASE_URL}/{table}.tsv.gz"
    tmp = target.with_suffix(".part")
    for attempt in range(1, retries + 1):
        try:
            print(f"[get ] {url}")
            with requests.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in resp.iter_content(CHUNK_SIZE):
                        fh.write(chunk)
            tmp.rename(target)
            size_mb = target.stat().st_size / 1e6
            print(f"[done] {target.name} ({size_mb:.1f} MB)")
            return
        except requests.RequestException as exc:
            print(f"[fail] attempt {attempt}/{retries}: {exc}")
            time.sleep(5 * attempt)
    raise RuntimeError(f"Could not download {url}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        download(table, force=args.force)


if __name__ == "__main__":
    main()
