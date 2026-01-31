#!/usr/bin/env python3
"""
Full sync pipeline: scrape Songs by Level, build CSV, upsert into songs table.
No image downloading or uploading.

Credentials from env: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (required for writes).
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from urllib.parse import unquote

from supabase import create_client, Client  # pylint: disable=import-error
from scraper import scrape_songs_by_level, fetch_song

# -----------------------------------------------------------------------------
# Env & Config
# -----------------------------------------------------------------------------

def _load_env() -> None:
    """Load .env via python-dotenv when available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

def _get_supabase_credentials() -> tuple[str, str]:
    """Return (url, key) from env."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set."
        )
    return url, key

_load_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

SONGS_BY_LEVEL_CSV = "songs_by_level.csv"
EXPORT_CSV = "songs_export.csv"

# Manually specified pages to always check/add
MANUAL_SONG_URLS = [
    "https://arcaea.fandom.com/wiki/OMAJINAI",
    "https://arcaea.fandom.com/wiki/CHAIN2NITE",
    "https://arcaea.fandom.com/wiki/One_Step_Closer",
    "https://arcaea.fandom.com/wiki/My_life_is_mine_alone!",
    "https://arcaea.fandom.com/wiki/Melty_Rhapsody",
    "https://arcaea.fandom.com/wiki/Signal",
    "https://arcaea.fandom.com/wiki/The_%27Raft%27_taught_me:_your_heart_will_always_find_a_way.",
]


def get_supabase_client() -> Client:
    """Create and return Supabase client using env credentials."""
    url, key = _get_supabase_credentials()
    return create_client(url, key)


def run_pipeline(skip_scrape: bool = False) -> None:
    """Run sync: scrape songs by level, upsert to DB (metadata only)."""
    supabase = get_supabase_client()
    project_root = Path(__file__).resolve().parent
    csv_path = project_root / SONGS_BY_LEVEL_CSV

    # 1. Scrape Songs by Level
    if skip_scrape and csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)
            rows = list(reader)
        logger.info("Using existing %s (%d rows).", SONGS_BY_LEVEL_CSV, len(rows))
    else:
        rows = scrape_songs_by_level(save_path=str(csv_path))
        if not rows:
            logger.error("No rows from scrape. Exiting.")
            return

    # 1b. Gap Check (Manual URLs vs Songs_by_Level)
    # Check if specific songs from the manual list (URLs) are missing.
    
    logger.info("Performing gap check against Manual URLs...")
    try:
        # Normalize titles for comparison
        existing_titles_csv = set((r.get("song") or "").strip().lower() for r in rows)
        
        # Parse titles from URLs
        manual_candidates = []
        for url in MANUAL_SONG_URLS:
            # Extract title part: .../wiki/Title_Of_Song
            if "/wiki/" in url:
                raw_title = url.split("/wiki/")[-1]
                # Decode (e.g. %27 -> ') and replace underscores
                title_decoded = unquote(raw_title).replace("_", " ")
                manual_candidates.append(title_decoded)
        
        if not manual_candidates:
             logger.info("No manual candidates found.")
        else:
            missing_titles = []
            for t in manual_candidates:
                # We compare loosely
                norm_t = t.strip()
                lower_t = norm_t.lower()
                
                # If it's in the CSV scrape, we good (it will be upserted/updated)
                if lower_t in existing_titles_csv:
                    continue
                    
                missing_titles.append(t)
            
            logger.info(f"Checking {len(missing_titles)} missing songs from Manual List...")
            
            # Fetch missing
            fetched_count = 0
            for i, m_title in enumerate(missing_titles):
                new_entries = fetch_song(m_title)
                if new_entries:
                    logger.info(f"Found missing song: {m_title}")
                    rows.extend(new_entries)
                    fetched_count += 1
                else:
                    logger.warning(f"Could not parse data for {m_title}")
                    
            logger.info(f"Added {fetched_count} confirmed missing songs.")
        
    except Exception as e:
        logger.error(f"Gap check failed: {e}")
        # We continue with what we have


    # 2. Build rows for export/upsert (Metadata Only)
    unique_rows = {} # (title, artist, difficulty) -> row_dict
    export_rows = []
    
    for row in rows:
        # Standardize fields
        const_val = row.get("chart_constant")
        if const_val in [None, "", "-"]:
             const_val = None
        else:
             try:
                 const_val = float(const_val)
             except (ValueError, TypeError):
                 const_val = None

        # Exclude songs with constant > 13 per user request
        if const_val is not None and const_val > 13:
            continue

        r = {
            "title": (row.get("song") or "").strip(),
            "artist": (row.get("artist") or "").strip(),
            "difficulty": (row.get("difficulty") or "").strip(),
            "constant": const_val,
            "level": (row.get("level") or "").strip(),
            "version": (row.get("version") or "").strip(),
            # imageUrl removed
        }
        # Unique key for deduplication
        key = (r["title"], r["artist"], r["difficulty"])
        unique_rows[key] = r # Latest entry wins

    db_rows = list(unique_rows.values())
    
    # Rebuild export rows from the unique set to match DB
    export_rows = []
    for r in db_rows:
        export_rows.append({
            "song": r["title"],
            "artist": r["artist"],
            "difficulty": r["difficulty"],
            "chart_constant": r["constant"],
            "level": r["level"],
            "version": r["version"]
        })

    # 3. Write export CSV
    export_path = project_root / EXPORT_CSV
    fieldnames = [
        "song", "artist", "difficulty",
        "chart_constant", "level", "version",
    ]
    with open(export_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(export_rows)
    logger.info("Wrote %d rows to %s.", len(export_rows), EXPORT_CSV)

    # 4. Upsert into Supabase
    batch_size = 100
    total = 0
    for i in range(0, len(db_rows), batch_size):
        batch = db_rows[i : i + batch_size]
        supabase.table("songs").upsert(
            batch,
            on_conflict="title,artist,difficulty",
            ignore_duplicates=False, # Update existing
        ).execute()
        total += len(batch)
        logger.info("Upserted rows %d-%d", i + 1, total)
    logger.info("Done. Upserted %d rows into songs table.", total)


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Sync songs metadata to Supabase (no images)."
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Reuse existing songs_by_level.csv",
    )
    args = parser.parse_args()
    try:
        run_pipeline(skip_scrape=args.skip_scrape)
        return 0
    except Exception as err:
        logger.error("Pipeline failed: %s", err)
        raise SystemExit(1) from err


if __name__ == "__main__":
    raise SystemExit(main())
