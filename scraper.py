"""
Arcaea Fandom scraper — single entry point.

- Songs by Level: scrape the Songs_by_Level wiki page (table: Song, Artist, Difficulty, Chart Constant, Level, Version).
- Song pages: fetch individual song pages via MediaWiki API and parse chart info + jacket (incl. BYD).
- Wiki images: download all song jacket images from Category:Songs.

Uses MediaWiki API only (no direct HTML scraping) to avoid Fandom blocks.
"""

import argparse
import csv
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

API_URL = "https://arcaea.fandom.com/api.php"
HEADERS = {
    "User-Agent": "ArcaeaChartsFetcher/1.0 (https://github.com/your-repo; gentle bot)",
    "Accept": "application/json",
}

SONGS_BY_LEVEL_PAGE = "Songs_by_Level"
DOWNLOAD_DIR = Path("wiki_images")
DELAY_BETWEEN_PAGES = 1.5  # seconds
REQUEST_TIMEOUT = 30


# -----------------------------------------------------------------------------
# CSV
# -----------------------------------------------------------------------------

def save_to_csv(data, filename):
    """Save list of dicts to a CSV file."""
    if not data:
        print("No data to save.")
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows to {filename}")


# -----------------------------------------------------------------------------
# MediaWiki API
# -----------------------------------------------------------------------------

def fetch_page_via_api(page_title):
    """Fetch parsed HTML for a wiki page using the MediaWiki API."""
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "text",
        "format": "json",
        "redirects": "1",
    }
    r = requests.get(API_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise ValueError(data["error"].get("info", str(data["error"])))
    return data["parse"]["text"]["*"]


# -----------------------------------------------------------------------------
# Songs by Level (Songs_by_Level page)
# -----------------------------------------------------------------------------

def parse_songs_by_level_html(html):
    """Parse the Songs by Level wiki page HTML into a list of row dicts.

    Table columns: Song, Artist, Difficulty, Chart Constant, Level, Version.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    # Fandom can use wikitable sortable, article-table sortable, or plain wikitable
    selectors = [
        "table.wikitable.sortable",
        "table.article-table.sortable",
        "table.wikitable",
        "table.sortable",
    ]
    tables = []
    for sel in selectors:
        tables = soup.select(sel)
        if tables:
            break
    if not tables:
        # Fallback: any table with 6+ columns in first data row
        for table in soup.select("table"):
            for tr in table.select("tbody tr"):
                tds = tr.select("td")
                if len(tds) >= 6:
                    tables = [table]
                    break
            if tables:
                break
    for table in tables:
        for tr in table.select("tbody tr"):
            tds = tr.select("td")
            if len(tds) < 6:
                continue
            # Song: often <a href="/wiki/...">Display name</a>
            song_cell = tds[0]
            song_link = song_cell.select_one("a")
            if song_link:
                song_title = song_link.get_text(strip=True)
            else:
                song_title = song_cell.get_text(strip=True)
            artist = tds[1].get_text(strip=True)
            difficulty = tds[2].get_text(strip=True)
            chart_constant = tds[3].get_text(strip=True)
            level = tds[4].get_text(strip=True)
            version = tds[5].get_text(strip=True)
            if not song_title:
                continue
            rows.append({
                "song": song_title,
                "artist": artist,
                "difficulty": difficulty,
                "chart_constant": chart_constant,
                "level": level,
                "version": version,
            })
    return rows


def scrape_songs_by_level(save_path=None):
    """Scrape the Songs by Level page via API and return (and optionally save) rows.

    Args:
        save_path: If set, save rows to this CSV file.

    Returns:
        List of dicts with keys: song, artist, difficulty, chart_constant, level, version.
    """
    print(f"Fetching {SONGS_BY_LEVEL_PAGE} via API...")
    html = fetch_page_via_api(SONGS_BY_LEVEL_PAGE)
    rows = parse_songs_by_level_html(html)
    print(f"Parsed {len(rows)} rows from Songs by Level.")
    if save_path:
        save_to_csv(rows, save_path)
    return rows


# -----------------------------------------------------------------------------
# Individual song pages (chart info + jacket, BYD support)
# -----------------------------------------------------------------------------

def parse_song_soup(soup, fallback_title=""):
    """Parse song data from a BeautifulSoup object. Handles BYD and Beyond jacket."""
    title = ""
    title_elem = soup.select_one(".mw-page-title-main")
    if title_elem:
        title = title_elem.get_text(strip=True)
    if not title:
        for selector in ["h1.page-header__title", "h1#firstHeading", ".song-template-title", "h1"]:
            el = soup.select_one(selector)
            if el:
                title = el.get_text(strip=True)
                break
    if not title and fallback_title:
        title = fallback_title.replace("_", " ")

    jacket_url = ""
    byd_jacket_url = ""
    jacket_imgs = soup.select(".pi-image img")
    if jacket_imgs:
        if jacket_imgs[0].has_attr("src"):
            jacket_url = jacket_imgs[0]["src"]
        if len(jacket_imgs) >= 2 and jacket_imgs[1].has_attr("src"):
            byd_jacket_url = jacket_imgs[1]["src"]

    artist = ""
    artist_elem = soup.select_one(".song-template-artist")
    if artist_elem:
        artist = re.sub(r"\([^)]+\)", "", artist_elem.get_text()).strip()

    songs_data = []
    chart_tables = soup.select("table.pi-horizontal-group")
    if not chart_tables:
        return songs_data

    # First table: default tab (PST/PRS/FTR/ETR, sometimes BYD)
    default_table = chart_tables[0]
    data_cells = default_table.select("tbody td")
    if len(data_cells) >= 3:
        level_cell = data_cells[0]
        constant_cell = data_cells[2]
        difficulties = [
            ("Past", "pst"),
            ("Present", "prs"),
            ("Future", "ftr"),
            ("Eternal", "etr"),
            ("Beyond", "byd"),
        ]
        for difficulty_name, class_key in difficulties:
            level_span = level_cell.select_one(f'span[class*="{class_key}"]')
            constant_span = constant_cell.select_one(f'span[class*="{class_key}"]')
            if not level_span or not constant_span:
                continue
            level_str = level_span.get_text(strip=True)
            constant_str = constant_span.get_text(strip=True)
            if not level_str or not constant_str or constant_str == "-":
                continue
            image_url = byd_jacket_url if difficulty_name == "Beyond" and byd_jacket_url else jacket_url
            songs_data.append({
                "imageUrl": image_url,
                "title": title,
                "artist": artist,
                "difficulty": difficulty_name,
                "constant": constant_str,
                "level": level_str,
                "version": "",
                "id": "",
            })

    # Second table: Beyond tab only
    if len(chart_tables) >= 2:
        byd_table = chart_tables[1]
        byd_cells = byd_table.select("tbody td")
        if len(byd_cells) >= 3:
            level_str = byd_cells[0].get_text(strip=True)
            constant_str = byd_cells[2].get_text(strip=True)
            if level_str and constant_str and constant_str != "-":
                if not any(s.get("difficulty") == "Beyond" for s in songs_data):
                    image_url = byd_jacket_url or jacket_url
                    songs_data.append({
                        "imageUrl": image_url,
                        "title": title,
                        "artist": artist,
                        "difficulty": "Beyond",
                        "constant": constant_str,
                        "level": level_str,
                        "version": "",
                        "id": "",
                    })

    return songs_data


def fetch_song(page_title):
    """Fetch and parse one song page via API; returns list of song entries."""
    html = fetch_page_via_api(page_title)
    soup = BeautifulSoup(html, "html.parser")
    return parse_song_soup(soup, fallback_title=page_title.replace("_", " "))


def fetch_songs(page_titles, delay_seconds=2, output_csv="individual_songs.csv"):
    """Fetch multiple song pages via API and save to CSV."""
    all_songs = []
    for i, title in enumerate(page_titles):
        print(f"Fetching: {title}...")
        try:
            entries = fetch_song(title)
            for e in entries:
                e["id"] = len(all_songs) + 1
                all_songs.append(e)
        except Exception as e:
            print(f"Error: {e}")
        if i < len(page_titles) - 1:
            time.sleep(delay_seconds)
    save_to_csv(all_songs, output_csv)
    print(f"Saved {len(all_songs)} entries to {output_csv}")
    return all_songs


# -----------------------------------------------------------------------------
# Category:Songs + wiki image downloader
# -----------------------------------------------------------------------------

def get_all_song_titles():
    """Get all page titles in Category:Songs via MediaWiki API (with pagination)."""
    titles = []
    cmcontinue = None
    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": "Category:Songs",
            "cmlimit": "500",
            "cmtype": "page",
            "format": "json",
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        r = requests.get(API_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        for m in data.get("query", {}).get("categorymembers", []):
            titles.append(m["title"])
        cont = data.get("continue", {})
        cmcontinue = cont.get("cmcontinue")
        if not cmcontinue:
            break
        time.sleep(0.3)
    return titles


def image_url_to_filename(url, fallback="image"):
    """Derive a safe filename from an image URL."""
    if not url:
        return f"{fallback}.jpg"
    path = url.split("?")[0].rstrip("/")
    parts = path.split("/")
    for part in reversed(parts):
        if "." in part and part not in (".", ".."):
            safe = re.sub(r"[^\w.\-]", "_", part)
            if safe:
                return safe
    ext = "jpg"
    if ".png" in url.lower():
        ext = "png"
    elif ".webp" in url.lower():
        ext = "webp"
    return f"{fallback}.{ext}"


def download_image(url, filepath):
    """Download url to filepath; return True on success."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        r.raise_for_status()
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"    Download failed: {e}")
        return False


def download_wiki_images(download_dir=None):
    """Download all song jacket images from Category:Songs.

    Uses API to fetch each song page, parses jacket (and Beyond) URLs, downloads
    each unique image to download_dir (default: wiki_images/).
    """
    download_dir = download_dir or DOWNLOAD_DIR
    print("Fetching song list from Category:Songs...")
    song_titles = get_all_song_titles()
    print(f"Found {len(song_titles)} song pages.")

    download_dir = Path(download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)
    seen_urls = set()
    downloaded = 0
    skipped = 0

    for i, page_title in enumerate(song_titles):
        print(f"[{i + 1}/{len(song_titles)}] {page_title}...")
        try:
            html = fetch_page_via_api(page_title)
            soup = BeautifulSoup(html, "html.parser")
            entries = parse_song_soup(soup, fallback_title=page_title.replace("_", " "))
        except Exception as e:
            print(f"    Skip: {e}")
            skipped += 1
            continue

        for entry in entries:
            url = (entry.get("imageUrl") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            title_slug = re.sub(r"[^\w\-]", "_", entry.get("title", page_title))[:50]
            fname = image_url_to_filename(url, fallback=title_slug)
            filepath = download_dir / fname
            if filepath.exists():
                continue
            if download_image(url, filepath):
                downloaded += 1
                print(f"    Saved {fname}")
        time.sleep(DELAY_BETWEEN_PAGES)

    print(f"\nDone. Downloaded {downloaded} new images (skipped {skipped} pages). Saved to {download_dir}/")
    return downloaded


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Arcaea Fandom scraper (Songs by Level, song pages, wiki images)")
    parser.add_argument(
        "mode",
        choices=["songs-by-level", "song-pages", "wiki-images"],
        help="songs-by-level: scrape Songs_by_Level page; song-pages: fetch listed song pages; wiki-images: download all jackets from Category:Songs",
    )
    parser.add_argument("--output", "-o", help="Output CSV (songs-by-level: default songs_by_level.csv; song-pages: default individual_songs.csv)")
    parser.add_argument("--delay", type=float, default=2.0, help="Delay between API requests (seconds)")
    parser.add_argument("--dir", dest="download_dir", default=None, help="Download directory for wiki-images (default: wiki_images)")
    args = parser.parse_args()

    if args.mode == "songs-by-level":
        scrape_songs_by_level(save_path=args.output or "songs_by_level.csv")
    elif args.mode == "song-pages":
        pages = [
            "Xterfusion",
            "Fracture_Ray",
            "World_Ender",
            "Singularity",
            "Tempestissimo",
        ]
        fetch_songs(pages, delay_seconds=args.delay, output_csv=args.output or "individual_songs.csv")
    elif args.mode == "wiki-images":
        download_wiki_images(download_dir=args.dir)


if __name__ == "__main__":
    main()
