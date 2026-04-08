#!/usr/bin/env python3
from __future__ import annotations

import re
import sqlite3
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path

BASE = Path("/home/raspberry/iona")
DB_PATH = BASE / "data" / "inteldash.db"


def normalise_title(title: str | None) -> str:
    text = (title or "").lower()
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def title_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def get_top_topic(conn: sqlite3.Connection, article_ids: list[int]) -> str | None:
    if not article_ids:
        return None
    placeholders = ",".join("?" for _ in article_ids)
    rows = conn.execute(
        f"SELECT topic FROM article_topics WHERE article_id IN ({placeholders})",
        article_ids
    ).fetchall()
    if not rows:
        return None
    counts = Counter(r[0] for r in rows)
    return counts.most_common(1)[0][0]


def get_top_region(conn: sqlite3.Connection, article_ids: list[int]) -> str | None:
    if not article_ids:
        return None
    placeholders = ",".join("?" for _ in article_ids)
    rows = conn.execute(
        f"SELECT region FROM article_regions WHERE article_id IN ({placeholders})",
        article_ids
    ).fetchall()
    if not rows:
        return None
    counts = Counter(r[0] for r in rows)
    return counts.most_common(1)[0][0]


def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    conn.execute("DELETE FROM clusters")
    conn.execute("UPDATE articles SET cluster_id = NULL")

    rows = conn.execute(
        "SELECT id, title, published_utc FROM articles ORDER BY id"
    ).fetchall()

    clusters: list[dict] = []

    for article_id, title, published_utc in rows:
        norm_title = normalise_title(title)
        if not norm_title:
            continue

        matched_cluster = None

        for cluster in clusters:
            sim = title_similarity(norm_title, cluster["norm_title"])
            if sim >= 0.72:
                matched_cluster = cluster
                break

        if matched_cluster is None:
            clusters.append({
                "rep_title": title,
                "norm_title": norm_title,
                "article_ids": [article_id],
                "first_seen": published_utc,
                "last_seen": published_utc,
            })
        else:
            matched_cluster["article_ids"].append(article_id)
            matched_cluster["last_seen"] = published_utc or matched_cluster["last_seen"]

    cluster_rows = 0
    linked_articles = 0

    for cluster in clusters:
        article_ids = cluster["article_ids"]
        top_topic = get_top_topic(conn, article_ids)
        top_region = get_top_region(conn, article_ids)

        conn.execute(
            """
            INSERT INTO clusters (
                representative_title, summary_text, first_seen_utc, last_seen_utc,
                article_count, source_count, top_topic, top_region,
                importance_score, novelty_score, change_score, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cluster["rep_title"],
                None,
                cluster["first_seen"],
                cluster["last_seen"],
                len(article_ids),
                len(article_ids),
                top_topic,
                top_region,
                float(len(article_ids)),
                0.0,
                0.0,
                "active",
            )
        )

        cluster_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        for article_id in article_ids:
            conn.execute(
                "UPDATE articles SET cluster_id = ? WHERE id = ?",
                (cluster_id, article_id)
            )
            linked_articles += 1

        cluster_rows += 1

    conn.commit()
    conn.close()

    print(f"Clusters created: {cluster_rows}")
    print(f"Articles linked to clusters: {linked_articles}")


if __name__ == "__main__":
    main()
