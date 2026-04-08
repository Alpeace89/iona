#!/usr/bin/env python3
from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

BASE = Path("/home/raspberry/iona")
DB_PATH = BASE / "data" / "inteldash.db"
WEB_DIR = BASE / "docs"
OUT_PATH = WEB_DIR / "index.html"


def q(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[tuple]:
    return conn.execute(sql, params).fetchall()


def esc(text) -> str:
    return html.escape("" if text is None else str(text))


def make_table(headers: list[str], rows: list[tuple]) -> str:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{esc(cell)}</td>" for cell in row)
        body_rows.append(f"<tr>{cells}</tr>")
    body = "\n".join(body_rows) if body_rows else '<tr><td colspan="%d">No data</td></tr>' % len(headers)
    return f"""
    <table>
      <thead><tr>{head}</tr></thead>
      <tbody>
        {body}
      </tbody>
    </table>
    """


def main() -> None:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    article_count = q(conn, "SELECT COUNT(*) FROM articles")[0][0]
    cluster_count = q(conn, "SELECT COUNT(*) FROM clusters")[0][0]

    top_topics = q(
        conn,
        """
        SELECT COALESCE(top_topic, 'untagged') AS topic, COUNT(*)
        FROM clusters
        GROUP BY topic
        ORDER BY COUNT(*) DESC, topic ASC
        LIMIT 10
        """
    )

    top_regions = q(
        conn,
        """
        SELECT COALESCE(top_region, 'unassigned') AS region, COUNT(*)
        FROM clusters
        GROUP BY region
        ORDER BY COUNT(*) DESC, region ASC
        LIMIT 10
        """
    )

    top_clusters = q(
        conn,
        """
        SELECT id,
               representative_title,
               article_count,
               COALESCE(top_topic, ''),
               COALESCE(top_region, '')
        FROM clusters
        ORDER BY article_count DESC, id ASC
        LIMIT 20
        """
    )

    recent_clusters = q(
        conn,
        """
        SELECT id,
               representative_title,
               COALESCE(last_seen_utc, ''),
               COALESCE(top_topic, ''),
               COALESCE(top_region, '')
        FROM clusters
        ORDER BY id DESC
        LIMIT 20
        """
    )

    middle_east_clusters = q(
        conn,
        """
        SELECT representative_title,
               COALESCE(top_topic, ''),
               article_count
        FROM clusters
        WHERE top_region = 'middle_east'
        ORDER BY article_count DESC, id ASC
        LIMIT 10
        """
    )

    conn.close()

    updated = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IONA</title>
  <style>
    :root {{
      --bg: #0f172a;
      --panel: #111827;
      --panel2: #1f2937;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #60a5fa;
      --border: #374151;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.4;
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 16px;
    }}
    h1, h2 {{
      margin-top: 0;
    }}
    .sub {{
      color: var(--muted);
      margin-bottom: 16px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 16px;
      margin-bottom: 16px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 4px 18px rgba(0,0,0,0.2);
    }}
    .stat {{
      font-size: 2rem;
      font-weight: bold;
      color: var(--accent);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    th, td {{
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid var(--border);
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      background: var(--panel2);
      position: sticky;
      top: 0;
    }}
    .section {{
      margin-bottom: 16px;
    }}
    .titlecell {{
      max-width: 700px;
      word-wrap: break-word;
    }}
    .footer {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-top: 24px;
      text-align: centre;
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>IONA</h1>
<p style="font-size: 0.9em; color: #666;">Last updated: {now}</p>
    <div class="sub">Intelligence On News Aggregation</div>

    <div class="grid section">
      <div class="card">
        <h2>Articles</h2>
        <div class="stat">{article_count}</div>
      </div>
      <div class="card">
        <h2>Clusters</h2>
        <div class="stat">{cluster_count}</div>
      </div>
      <div class="card">
        <h2>Last Updated</h2>
        <div>{esc(updated)}</div>
      </div>
    </div>

    <div class="grid section">
      <div class="card">
        <h2>Top Topics</h2>
        {make_table(["Topic", "Clusters"], top_topics)}
      </div>
      <div class="card">
        <h2>Top Regions</h2>
        {make_table(["Region", "Clusters"], top_regions)}
      </div>
    </div>

    <div class="card section">
      <h2>Top Clusters</h2>
      {make_table(["ID", "Title", "Articles", "Topic", "Region"], top_clusters)}
    </div>

    <div class="card section">
      <h2>Recent Clusters</h2>
      {make_table(["ID", "Title", "Last Seen", "Topic", "Region"], recent_clusters)}
    </div>

    <div class="card section">
      <h2>Middle East Watch</h2>
      {make_table(["Title", "Topic", "Articles"], middle_east_clusters)}
    </div>

    <div class="footer">
      IONA · Static local page
    </div>
  </div>
</body>
</html>
"""
    OUT_PATH.write_text(html_doc, encoding="utf-8")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
