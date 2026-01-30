#!/usr/bin/env python3
"""
Full sync pipeline: scrape Songs by Level, download wiki images, upload to Supabase Storage,
build CSV with Supabase image URLs, upsert into songs table.

Credentials from env: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (required for writes; anon key is blocked by RLS).
Loads .env via python-dotenv when available.

Usage: python pipeline.py [--skip-scrape] [--skip-wiki-images]
"""

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path

from supabase import create_client, Client  # pylint: disable=import-error

from scraper import scrape_songs_by_level, download_wiki_images
from update_image_urls import build_title_to_file_map, find_image_for_title

# -----------------------------------------------------------------------------
# Env loading (optional dotenv)
# -----------------------------------------------------------------------------


def _load_env() -> None:  # pylint: disable=import-outside-toplevel
    """Load .env via python-dotenv when available."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def _get_supabase_credentials() -> tuple[str, str]:
    """Return (url, key) from env. Requires service role key (bypasses RLS) for writes."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set. "
            "The pipeline writes to Storage and the songs table; the anon key is blocked by RLS."
        )
    return url, key


_load_env()

# -----------------------------------------------------------------------------
# Config & Logging
# -----------------------------------------------------------------------------

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

STORAGE_BUCKET = "song-images"
SONGS_BY_LEVEL_CSV = "songs_by_level.csv"
WIKI_IMAGES_DIR = Path("wiki_images")
EXPORT_CSV = "songs_export.csv"


def get_supabase_client() -> Client:
    """Create and return Supabase client using env credentials."""
    url, key = _get_supabase_credentials()
    return create_client(url, key)


def safe_title(title: str) -> str:
    """Non-alnum -> '_', lower, max 50 chars for storage filename."""
    cleaned = re.sub(r"[^a-zA-Z0-9]", "_", title or "")
    return cleaned.lower()[:50]


def get_content_type_from_ext(ext: str) -> str:
    """Return MIME type for image extension."""
    mime_map = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "webp": "image/webp",
    }
    return mime_map.get(ext.lower(), "image/jpeg")


def image_url_without_revision(url: str) -> str:
    """Strip /revision/latest from Supabase storage URL."""
    if not url:
        return url
    return url.replace("/revision/latest", "")


def file_exists_in_bucket(supabase: Client, file_path: str) -> bool:
    """Return True if file_path exists in the storage bucket."""
    try:
        res = supabase.storage.from_(STORAGE_BUCKET).list(path="")
        if res:
            for item in res:
                name = (
                    item.get("name")
                    if isinstance(item, dict)
                    else getattr(item, "name", None)
                )
                if name == file_path:
                    return True
        return False
    except (OSError, ValueError):
        return False


def upload_local_file_to_storage(
    supabase: Client,
    local_path: Path,
    storage_filename: str,
) -> str | None:
    """Upload local file to Supabase Storage; return public URL or None."""
    if not local_path.is_file():
        return None
    file_bytes = local_path.read_bytes()
    ext = local_path.suffix.lstrip(".").lower() or "jpg"
    content_type = get_content_type_from_ext(ext)

    try:
        supabase.storage.from_(STORAGE_BUCKET).upload(
            storage_filename,
            file_bytes,
            file_options={"content-type": content_type},
        )
    except Exception as err:  # pylint: disable=broad-exception-caught
        err_str = str(err).lower()
        if "already exists" in err_str or "duplicate" in err_str:
            public_url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(
                storage_filename
            )
            return image_url_without_revision(public_url)
        logger.error("Upload failed for %s: %s", storage_filename, err)
        return None

    public_url = supabase.storage.from_(STORAGE_BUCKET).get_public_url(storage_filename)
    return image_url_without_revision(public_url)


def run_pipeline(  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    skip_scrape: bool = False, skip_wiki_images: bool = False
) -> None:
    """Run full sync: scrape, wiki images, upload to storage, upsert songs table."""
    supabase = get_supabase_client()
    project_root = Path(__file__).resolve().parent
    images_dir = project_root / WIKI_IMAGES_DIR
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

    # 2. Download wiki images
    if not skip_wiki_images:
        download_wiki_images(download_dir=images_dir)
    else:
        logger.info("Skipping wiki-images download.")
    if not images_dir.is_dir():
        logger.error("wiki_images/ not found. Exiting.")
        return

    # 3. Build title -> local file path
    title_to_path = build_title_to_file_map(images_dir)
    # For each row we need Path from song name: find_image_for_title returns rel path string
    # We need Path to read bytes. Build song -> Path (same as title_to_path but keyed by song from CSV).
    song_to_path: dict[str, Path] = {}
    for song in {r.get("song", "").strip() for r in rows if r.get("song")}:
        if song in song_to_path:
            continue
        # find_image_for_title returns e.g. "wiki_images/File.jpg"; we need Path
        rel = find_image_for_title(song, title_to_path, images_dir)
        if rel:
            song_to_path[song] = project_root / rel

    # 4. For each unique song: ensure file in storage, get public URL
    song_to_image_url: dict[str, str] = {}
    for song, local_path in song_to_path.items():
        storage_name = safe_title(song) + local_path.suffix.lower()
        if not storage_name.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
            storage_name = safe_title(song) + ".jpg"
        if file_exists_in_bucket(supabase, storage_name):
            url = image_url_without_revision(
                supabase.storage.from_(STORAGE_BUCKET).get_public_url(storage_name)
            )
            song_to_image_url[song] = url
        else:
            url = upload_local_file_to_storage(supabase, local_path, storage_name)
            if url:
                song_to_image_url[song] = url
                logger.info("Uploaded %s", storage_name)

    # 5. Build rows with imageUrl
    export_rows = []
    for row in rows:
        song = (row.get("song") or "").strip()
        image_url = song_to_image_url.get(song) or ""
        export_rows.append({
            "imageUrl": image_url,
            "song": song,
            "artist": row.get("artist", ""),
            "difficulty": row.get("difficulty", ""),
            "chart_constant": row.get("chart_constant", ""),
            "level": row.get("level", ""),
            "version": row.get("version", ""),
        })

    # 6. Write export CSV
    export_path = project_root / EXPORT_CSV
    fieldnames = [
        "imageUrl", "song", "artist", "difficulty",
        "chart_constant", "level", "version",
    ]
    with open(export_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(export_rows)
    logger.info("Wrote %d rows to %s.", len(export_rows), EXPORT_CSV)

    # 7. Upsert into Supabase songs (map song -> title, chart_constant -> constant)
    db_rows = []
    for row in export_rows:
        db_rows.append({
            "imageUrl": row["imageUrl"],
            "title": row["song"],
            "artist": row["artist"],
            "difficulty": row["difficulty"],
            "constant": row["chart_constant"],
            "level": row["level"],
            "version": row["version"],
        })

    # Batch upsert (PostgREST has limits; 100–500 per batch is safe)
    batch_size = 100
    total = 0
    for i in range(0, len(db_rows), batch_size):
        batch = db_rows[i : i + batch_size]
        supabase.table("songs").upsert(
            batch,
            on_conflict="title,artist,difficulty",
            ignore_duplicates=True,
        ).execute()
        total += len(batch)
        logger.info("Upserted rows %d-%d", i + 1, total)
    logger.info("Done. Upserted %d rows into songs table.", total)


def main() -> int:
    """CLI entry point; run pipeline and exit with 0 on success, 1 on error."""
    parser = argparse.ArgumentParser(
        description="Full sync: scrape, wiki images, Supabase storage, songs table"
    )
    parser.add_argument(
        "--skip-scrape", action="store_true",
        help="Reuse existing songs_by_level.csv",
    )
    parser.add_argument(
        "--skip-wiki-images", action="store_true",
        help="Skip downloading wiki images",
    )
    args = parser.parse_args()
    try:
        run_pipeline(
            skip_scrape=args.skip_scrape,
            skip_wiki_images=args.skip_wiki_images,
        )
        return 0
    except Exception as err:
        logger.error("Pipeline failed: %s", err)
        raise SystemExit(1) from err


if __name__ == "__main__":
    raise SystemExit(main())
