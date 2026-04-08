#!/usr/bin/env python3
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

BASE = Path("/home/raspberry/iona")
DB_PATH = BASE / "data" / "inteldash.db"

TOPIC_RULES = {
    "geopolitics": ["sanction","diplomat","embassy","border","ceasefire","conflict","military","troops","defence","defense","missile","airstrike","invasion","navy","nato","summit"],
    "cyber": ["cyber","ransomware","hack","hacker","breach","malware","phishing","ddos"],
    "natural_hazards": ["earthquake","quake","tsunami","volcano","wildfire","flood","storm","hurricane"],
    "energy": ["oil","gas","lng","pipeline","refinery","electricity","energy","nuclear"],
    "space": ["nasa","spacex","rocket","launch","satellite","iss","artemis"],
    "economy": ["inflation","tariff","trade","economy","market","stocks","bank","recession"]
}

REGION_RULES = {
    "canada": ["canada","ottawa","ontario","quebec","alberta"],
    "united_states": ["united states","usa","washington"],
    "china": ["china","beijing"],
    "russia": ["russia","moscow","kremlin"],
    "ukraine": ["ukraine","kyiv"],
    "middle_east": ["israel","gaza","iran","iraq","syria"],
    "europe": ["europe","france","germany","italy","spain","uk"],
    "asia_pacific": ["japan","korea","taiwan","australia"],
    "africa": ["africa","sudan","ethiopia","nigeria"],
    "latin_america": ["mexico","brazil","argentina","chile"]
}

def normalise(text):
    text = text.lower()
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def main():
    conn = sqlite3.connect(DB_PATH)

    rows = conn.execute(
        "SELECT id, title, snippet FROM articles"
    ).fetchall()

    topic_count = 0
    region_count = 0

    for article_id, title, snippet in rows:
        text = normalise(f"{title or ''} {snippet or ''}")

        for topic, kws in TOPIC_RULES.items():
            if any(k in text for k in kws):
                conn.execute(
                    "INSERT OR IGNORE INTO article_topics (article_id, topic, score) VALUES (?, ?, 1)",
                    (article_id, topic)
                )
                topic_count += 1

        for region, kws in REGION_RULES.items():
            if any(k in text for k in kws):
                conn.execute(
                    "INSERT OR IGNORE INTO article_regions (article_id, region, region_type) VALUES (?, ?, 'rule')",
                    (article_id, region)
                )
                region_count += 1

    conn.commit()
    conn.close()

    print(f"Topics added: {topic_count}")
    print(f"Regions added: {region_count}")

if __name__ == "__main__":
    main()
