#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import feedparser
import requests
import yaml


BASE = Path("/home/raspberry/iona")
DB_PATH = BASE / "data" / "inteldash.db"
RAW_DIR = BASE / "data" / "raw"
CONFIG_PATH = BASE / "config" / "sources.yaml"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_datetime(entry: dict[str, Any]) -> str | None:
    for key in ("published", "updated"):
        value = entry.get(key)
        if not value:
            continue
        try:
            dt = parsedate_to_datetime(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
        except Exception:
            continue
    return None


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def load_sources() -> list[dict[str, Any]]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return [s for s in data.get("sources", []) if s.get("enabled", False)]


def ensure_source(conn: sqlite3.Connection, source: dict[str, Any]) -> int:
    conn.execute(
        """
        INSERT INTO sources (name, type, url, reliability_score, enabled, region_focus, topic_focus)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            type=excluded.type,
            url=excluded.url,
            reliability_score=excluded.reliability_score,
            enabled=excluded.enabled,
            region_focus=excluded.region_focus,
            topic_focus=excluded.topic_focus
        """,
        (
            source["name"],
            source["type"],
            source["url"],
            float(source.get("reliability_score", 0.5)),
            1 if source.get("enabled", True) else 0,
            source.get("region_focus"),
            source.get("topic_focus"),
        ),
    )
    row = conn.execute("SELECT id FROM sources WHERE name = ?", (source["name"],)).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to create or retrieve source: {source['name']}")
    return int(row[0])


def save_raw_entry(source_name: str, entry: dict[str, Any], fetched_utc: str) -> str:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    safe_source = "".join(c if c.isalnum() else "_" for c in source_name.lower()).strip("_")
    url = entry.get("link", "")
    stem = sha256_text(url or json.dumps(entry, sort_keys=True))[:16]
    out_path = RAW_DIR / f"{safe_source}_{fetched_utc.replace(':', '').replace('-', '')}_{stem}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False, indent=2)
    return str(out_path)


def insert_article(conn: sqlite3.Connection, source_id: int, source_name: str, entry: dict[str, Any]) -> bool:
    title = (entry.get("title") or "").strip()
    url = (entry.get("link") or "").strip()
    snippet = (entry.get("summary") or entry.get("description") or "").strip()
    published_utc = parse_datetime(entry)
    fetched_utc = utc_now_iso()

    if not url:
        return False

    title_hash = sha256_text(title) if title else None
    content_hash = sha256_text(f"{title}\n{snippet}")
    raw_json_path = save_raw_entry(source_name, entry, fetched_utc)

    try:
        conn.execute(
            """
            INSERT INTO articles (
                source_id, title, url, published_utc, fetched_utc,
                snippet, body_text, title_hash, content_hash, language,
                is_duplicate, cluster_id, raw_json_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                title,
                url,
                published_utc,
                fetched_utc,
                snippet,
                None,
                title_hash,
                content_hash,
                None,
                0,
                None,
                raw_json_path,
            ),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def update_source_status(conn: sqlite3.Connection, source_id: int, status: str) -> None:
    conn.execute(
        """
        UPDATE sources
        SET last_fetch_utc = ?, last_status = ?
        WHERE id = ?
        """,
        (utc_now_iso(), status, source_id),
    )


def fetch_one_source(conn: sqlite3.Connection, source: dict[str, Any]) -> tuple[int, int]:
    source_id = ensure_source(conn, source)
    feed_url = source["url"]

    try:
        headers = {"User-Agent": "inteldash/1.0"}
        response = requests.get(feed_url, headers=headers, timeout=15)
        response.raise_for_status()

        parsed = feedparser.parse(response.content)

        if getattr(parsed, "bozo", 0):
            status = f"warning: bozo={getattr(parsed, 'bozo_exception', 'unknown')}"
        else:
            status = "ok"

        added = 0
        seen = 0

        for entry in parsed.entries:
            seen += 1
            if insert_article(conn, source_id, source["name"], dict(entry)):
                added += 1

        update_source_status(conn, source_id, f"{status}; entries={seen}; added={added}")
        return seen, added

    except requests.exceptions.Timeout:
        update_source_status(conn, source_id, "error: timeout")
        return 0, 0
    except requests.exceptions.RequestException as e:
        update_source_status(conn, source_id, f"error: request failed: {e}")
        return 0, 0
    except Exception as e:
        update_source_status(conn, source_id, f"error: {e}")
        return 0, 0


def main() -> None:
    sources = load_sources()
    conn = sqlite3.connect(DB_PATH)
    total_seen = 0
    total_added = 0

    try:
        for source in sources:
            seen, added = fetch_one_source(conn, source)
            total_seen += seen
            total_added += added
            conn.commit()
            print(f"{source['name']}: seen={seen}, added={added}")

        print(f"Done. total_seen={total_seen}, total_added={total_added}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
