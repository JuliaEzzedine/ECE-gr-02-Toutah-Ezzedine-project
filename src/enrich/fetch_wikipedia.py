"""Collect film synopses and biographies from Wikipedia (outside Spark).

Steps
  1. IMDb id -> Wikidata item -> English Wikipedia article, using the
     Wikidata property P345 (IMDb ID) through the Wikidata SPARQL endpoint.
  2. Films  : full plain text of the article, the "Plot" section is kept.
  3. People : lead section (introduction) of the article.

Every record keeps its lineage: Wikidata item URI, Wikipedia article URL,
retrieval time, licence, and a `status` explaining missing information
(no Wikidata item, no Wikipedia article, no plot section, ...).

Politeness
  - descriptive User-Agent with a contact address (Wikimedia policy)
  - requests are sent one at a time with a minimum delay (--delay)
  - `maxlag=5` on every Wikipedia API call
  - HTTP 429 / 5xx and maxlag errors are retried after Retry-After
    (or an exponential back-off)

The job is resumable: ids already present in the output files are skipped,
so it can be stopped with Ctrl+C and started again.

Outputs (JSON lines, appended):
  data/enrich/raw/wikidata_links.jsonl
  data/enrich/raw/film_synopsis.jsonl
  data/enrich/raw/person_bio.jsonl

Usage: python -m src.enrich.fetch_wikipedia [--delay 0.5] [--limit 100]
"""
import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, unquote

import requests

from src.common.config import ENRICH_RAW_DIR, ENRICH_TARGETS_DIR

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_PREFIX = "https://en.wikipedia.org/wiki/"
LICENSE = "CC BY-SA 4.0"
CONTACT = os.getenv("WIKI_CONTACT", "ece-big-data-project@example.com")
USER_AGENT = f"ECE-BigData-IMDb-Project/1.0 (student project; {CONTACT}) python-requests"

PLOT_SECTIONS = ["plot", "synopsis", "plot summary", "summary", "premise", "story"]
SPARQL_BATCH = 200
BIO_BATCH = 20          # Wikipedia returns at most 20 intro extracts per request
MAX_RETRIES = 6


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
class PoliteClient:
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.calls = 0

    def get_json(self, url: str, params: dict, method: str = "GET") -> dict:
        for attempt in range(MAX_RETRIES):
            wait = self.delay - (time.monotonic() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            self.last_call = time.monotonic()
            self.calls += 1
            try:
                if method == "POST":
                    resp = self.session.post(url, data=params, timeout=60)
                else:
                    resp = self.session.get(url, params=params, timeout=60)
            except requests.RequestException as exc:
                backoff = 2 ** attempt
                print(f"  [retry] {exc} -> sleeping {backoff}s")
                time.sleep(backoff)
                continue

            if resp.status_code == 429 or resp.status_code >= 500:
                backoff = int(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"  [retry] HTTP {resp.status_code} -> sleeping {backoff}s")
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and data.get("error", {}).get("code") == "maxlag":
                backoff = int(resp.headers.get("Retry-After", 5))
                print(f"  [retry] server lagged -> sleeping {backoff}s")
                time.sleep(backoff)
                continue
            return data
        raise RuntimeError(f"Giving up on {url} after {MAX_RETRIES} attempts")


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #
def read_targets(name: str) -> list[dict]:
    """Read the JSON lines written by Spark (one folder of part files)."""
    rows = []
    for part in sorted((ENRICH_TARGETS_DIR / name).glob("part-*.json")):
        with open(part, encoding="utf-8") as fh:
            rows += [json.loads(line) for line in fh if line.strip()]
    return rows


def done_ids(path: Path, key: str) -> set[str]:
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as fh:
        return {json.loads(line)[key] for line in fh if line.strip()}


def append(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def chunks(items: list, size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


# --------------------------------------------------------------------------- #
# Step 1: IMDb id -> Wikidata -> Wikipedia
# --------------------------------------------------------------------------- #
SPARQL_QUERY = """
SELECT ?imdb ?item ?article WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL {
    ?article schema:about ?item ;
             schema:isPartOf <https://en.wikipedia.org/> .
  }
}
"""


def resolve_links(client: PoliteClient, imdb_ids: list[str], out: Path) -> None:
    todo = sorted(set(imdb_ids) - done_ids(out, "imdb_id"))
    print(f"[links] {len(todo):,} ids to resolve")
    for batch in chunks(todo, SPARQL_BATCH):
        values = " ".join(f'"{i}"' for i in batch)
        data = client.get_json(
            WIKIDATA_SPARQL, {"query": SPARQL_QUERY % values, "format": "json"}, method="POST"
        )
        found: dict[str, dict] = {}
        for b in data["results"]["bindings"]:
            imdb = b["imdb"]["value"]
            article = b.get("article", {}).get("value")
            # keep the first item that has an English article
            if imdb not in found or (article and not found[imdb]["wikipedia_url"]):
                found[imdb] = {"wikidata_uri": b["item"]["value"], "wikipedia_url": article}
        records = []
        for imdb in batch:
            hit = found.get(imdb)
            if hit is None:
                status, hit = "no_wikidata_item", {"wikidata_uri": None, "wikipedia_url": None}
            elif hit["wikipedia_url"] is None:
                status = "no_english_wikipedia_article"
            else:
                status = "ok"
            records.append({"imdb_id": imdb, **hit, "status": status, "retrieved_at": now()})
        append(out, records)
        print(f"[links] +{len(batch)} ({client.calls} requests)")


def load_links(path: Path) -> dict[str, dict]:
    links = {}
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                rec = json.loads(line)
                links[rec["imdb_id"]] = rec
    return links


def title_from_url(url: str) -> str:
    return unquote(url[len(WIKIPEDIA_PREFIX):]).replace("_", " ")


def url_from_title(title: str) -> str:
    return WIKIPEDIA_PREFIX + quote(title.replace(" ", "_"))


# --------------------------------------------------------------------------- #
# Step 2: film synopsis
# --------------------------------------------------------------------------- #
HEADING = re.compile(r"^(={2,6})\s*(.+?)\s*\1\s*$", re.MULTILINE)


def extract_section(text: str, wanted: list[str]) -> tuple[str | None, str | None]:
    """Return (heading, body) of the first level-2 section whose title matches."""
    matches = list(HEADING.finditer(text))
    for idx, m in enumerate(matches):
        if len(m.group(1)) != 2 or m.group(2).strip().lower() not in wanted:
            continue
        end = len(text)
        for nxt in matches[idx + 1:]:
            if len(nxt.group(1)) == 2:
                end = nxt.start()
                break
        body = HEADING.sub("", text[m.end():end]).strip()
        return m.group(2).strip(), (body or None)
    return None, None


def fetch_full_text(client: PoliteClient, title: str) -> dict:
    data = client.get_json(WIKIPEDIA_API, {
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1",
        "prop": "extracts|info", "explaintext": "1", "exsectionformat": "wiki",
        "inprop": "url", "titles": title, "maxlag": "5",
    })
    pages = data.get("query", {}).get("pages", [])
    return pages[0] if pages else {"missing": True}


def fetch_synopses(client, films: list[dict], links: dict, out: Path, limit: int | None):
    done = done_ids(out, "tconst")
    todo = [f for f in films if f["tconst"] not in done]
    if limit:
        todo = todo[:limit]
    print(f"[films] {len(todo):,} synopses to fetch")
    for n, film in enumerate(todo, 1):
        link = links.get(film["tconst"], {})
        rec = {
            "tconst": film["tconst"], "wikidata_uri": link.get("wikidata_uri"),
            "source_url": link.get("wikipedia_url"), "license": LICENSE,
            "section": None, "synopsis": None, "retrieved_at": now(),
        }
        if not link.get("wikipedia_url"):
            rec["status"] = link.get("status", "not_resolved")
        else:
            page = fetch_full_text(client, title_from_url(link["wikipedia_url"]))
            if page.get("missing") or "extract" not in page:
                rec["status"] = "wikipedia_page_missing"
            else:
                rec["source_url"] = page.get("fullurl") or url_from_title(page["title"])
                rec["revision_id"] = page.get("lastrevid")
                section, body = extract_section(page["extract"], PLOT_SECTIONS)
                rec["section"], rec["synopsis"] = section, body
                rec["status"] = "ok" if body else "no_plot_section"
        append(out, [rec])
        if n % 50 == 0:
            print(f"[films] {n:,}/{len(todo):,} ({client.calls} requests)")


# --------------------------------------------------------------------------- #
# Step 3: biographies
# --------------------------------------------------------------------------- #
def fetch_intros(client: PoliteClient, titles: list[str]) -> dict[str, dict]:
    data = client.get_json(WIKIPEDIA_API, {
        "action": "query", "format": "json", "formatversion": "2", "redirects": "1",
        "prop": "extracts|info", "exintro": "1", "explaintext": "1", "exlimit": "max",
        "inprop": "url", "titles": "|".join(titles), "maxlag": "5",
    })
    query = data.get("query", {})
    # map requested title -> final title (normalisation and redirects)
    final = {t: t for t in titles}
    for step in ("normalized", "redirects"):
        mapping = {m["from"]: m["to"] for m in query.get(step, [])}
        final = {t: mapping.get(f, f) for t, f in final.items()}
    pages = {p["title"]: p for p in query.get("pages", [])}
    return {t: pages.get(f, {"missing": True}) for t, f in final.items()}


def fetch_bios(client, people: list[dict], links: dict, out: Path, limit: int | None):
    done = done_ids(out, "nconst")
    todo = [p for p in people if p["nconst"] not in done]
    if limit:
        todo = todo[:limit]
    print(f"[people] {len(todo):,} biographies to fetch")
    for n, batch in enumerate(chunks(todo, BIO_BATCH), 1):
        titles = {}
        for person in batch:
            url = links.get(person["nconst"], {}).get("wikipedia_url")
            if url:
                titles[person["nconst"]] = title_from_url(url)
        pages = fetch_intros(client, list(set(titles.values()))) if titles else {}
        records = []
        for person in batch:
            link = links.get(person["nconst"], {})
            rec = {
                "nconst": person["nconst"], "wikidata_uri": link.get("wikidata_uri"),
                "source_url": link.get("wikipedia_url"), "license": LICENSE,
                "biography": None, "retrieved_at": now(),
            }
            if person["nconst"] not in titles:
                rec["status"] = link.get("status", "not_resolved")
            else:
                page = pages.get(titles[person["nconst"]], {"missing": True})
                text = (page.get("extract") or "").strip()
                if page.get("missing"):
                    rec["status"] = "wikipedia_page_missing"
                else:
                    rec["source_url"] = page.get("fullurl") or rec["source_url"]
                    rec["revision_id"] = page.get("lastrevid")
                    rec["biography"] = text or None
                    rec["status"] = "ok" if text else "empty_introduction"
            records.append(rec)
        append(out, records)
        if n % 10 == 0:
            print(f"[people] {min(n * BIO_BATCH, len(todo)):,}/{len(todo):,} "
                  f"({client.calls} requests)")


# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=0.5,
                        help="minimum seconds between two requests")
    parser.add_argument("--limit", type=int, default=None,
                        help="only process N films / people (for a quick test)")
    parser.add_argument("--step", choices=["all", "links", "films", "people"], default="all")
    args = parser.parse_args()

    client = PoliteClient(args.delay)
    films = read_targets("films")
    people = read_targets("people")
    if args.limit:
        films, people = films[:args.limit], people[:args.limit]
    print(f"[targets] {len(films):,} films, {len(people):,} people")

    links_path = ENRICH_RAW_DIR / "wikidata_links.jsonl"
    if args.step in ("all", "links"):
        resolve_links(client, [f["tconst"] for f in films] + [p["nconst"] for p in people],
                      links_path)
    links = load_links(links_path)
    if args.step in ("all", "films"):
        fetch_synopses(client, films, links, ENRICH_RAW_DIR / "film_synopsis.jsonl", None)
    if args.step in ("all", "people"):
        fetch_bios(client, people, links, ENRICH_RAW_DIR / "person_bio.jsonl", None)
    print(f"[done] {client.calls} HTTP requests")


if __name__ == "__main__":
    main()
