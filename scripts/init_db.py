#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from pathlib import Path

BASE = Path("/home/raspberry/iona")
DB_PATH = BASE / "data" / "inteldash.db"


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    url TEXT NOT NULL,
    reliability_score REAL DEFAULT 0.5,
    enabled INTEGER DEFAULT 1,
    region_focus TEXT,
    topic_focus TEXT,
    last_fetch_utc TEXT,
    last_status TEXT
);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    title TEXT,
    url TEXT NOT NULL UNIQUE,
    published_utc TEXT,
    fetched_utc TEXT NOT NULL,
    snippet TEXT,
    body_text TEXT,
    title_hash TEXT,
    content_hash TEXT,
    language TEXT,
    is_duplicate INTEGER DEFAULT 0,
    cluster_id INTEGER,
    raw_json_path TEXT,
    FOREIGN KEY(source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS article_topics (
    article_id INTEGER NOT NULL,
    topic TEXT NOT NULL,
    score REAL DEFAULT 1.0,
    PRIMARY KEY (article_id, topic),
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS article_entities (
    article_id INTEGER NOT NULL,
    entity TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    PRIMARY KEY (article_id, entity, entity_type),
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS article_regions (
    article_id INTEGER NOT NULL,
    region TEXT NOT NULL,
    region_type TEXT NOT NULL,
    PRIMARY KEY (article_id, region, region_type),
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS clusters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    representative_title TEXT,
    summary_text TEXT,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    article_count INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 0,
    top_topic TEXT,
    top_region TEXT,
    importance_score REAL DEFAULT 0.0,
    novelty_score REAL DEFAULT 0.0,
    change_score REAL DEFAULT 0.0,
    status TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_utc TEXT NOT NULL,
    top_clusters_json TEXT,
    topic_counts_json TEXT,
    region_counts_json TEXT,
    change_log_json TEXT,
    brief_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_articles_source_id ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_published_utc ON articles(published_utc);
CREATE INDEX IF NOT EXISTS idx_articles_fetched_utc ON articles(fetched_utc);
CREATE INDEX IF NOT EXISTS idx_articles_cluster_id ON articles(cluster_id);
"""


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
        print(f"Database initialised at: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
