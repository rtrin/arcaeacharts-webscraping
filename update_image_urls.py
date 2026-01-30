"""
Update song rows' imageUrl to local wiki_images paths by matching song title to filename.

Handles:
- Normal names: "Fracture Ray" -> Fracture_Ray.jpg, Xterfusion.jpg
- Encoded names: "ΟΔΥΣΣΕΙΑ" -> _CE_9F_CE_94_CE_A5_CE_A3_CE_A3_CE_95_CE_99_CE_91.jpg
  (Fandom stores some filenames as UTF-8 bytes in underscore-hex: _XX_XX_XX)

Usage:
  python update_image_urls.py [--csv individual_songs.csv] [--images wiki_images] [--output ...]
"""

import argparse
import csv
import re
from pathlib import Path

# Unicode subscript/superscript digit → ASCII digit (e.g. ₀→0 for INCARNATOR₀₀)
_SUBSUP_DIGITS = str.maketrans(
    "⁰¹²³⁴⁵⁶⁷⁸⁹₀₁₂₃₄₅₆₇₈₉",
    "01234567890123456789",
)


def _normalize_digits(s: str) -> str:
    """Replace subscript/superscript digits with ASCII digits for matching."""
    return (s or "").translate(_SUBSUP_DIGITS)


def slug(s: str) -> str:
    """Normalize for matching: alphanumeric + underscore, lower. Subscript/superscript digits → ASCII."""
    s = (s or "").strip()
    s = _normalize_digits(s)
    s = re.sub(r"[^\w\s\-]", " ", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s.lower()


def title_to_encoded_stem(title: str) -> str:
    """Encode title as UTF-8 bytes in underscore-hex form (e.g. ΟΔΥΣΣΕΙΑ -> _CE_9F_CE_94_...)."""
    if not title:
        return ""
    raw = title.encode("utf-8")
    return "_" + "_".join(f"{b:02X}" for b in raw)


def stem_to_title(stem: str) -> str | None:
    """Decode stem that looks like _XX_XX_XX (UTF-8 hex bytes) to string. Else return None."""
    if not stem:
        return None
    s = stem[1:] if stem.startswith("_") else stem
    parts = s.split("_")
    try:
        b = bytes(
            int(p, 16)
            for p in parts
            if len(p) == 2 and all(c in "0123456789ABCDEFabcdef" for c in p)
        )
        if not b:
            return None
        return b.decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None


def build_title_to_file_map(images_dir: Path) -> dict[str, Path]:
    """
    Map song title (normalized and decoded) -> first matching file path.
    - For stems like _CE_9F_..., decode to title and register.
    - For normal stems, register slug(stem) and also stem-as-title (with spaces).
    """
    title_to_path: dict[str, Path] = {}
    if not images_dir.is_dir():
        return title_to_path

    for path in images_dir.iterdir():
        if path.is_dir():
            continue
        stem = path.stem
        ext = path.suffix.lower()
        if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
            continue

        # Encoded form: _XX_XX_XX -> decode to title
        decoded = stem_to_title(stem)
        if decoded is not None:
            key = decoded.strip().lower()
            if key and key not in title_to_path:
                title_to_path[key] = path
            # Also register normalized (e.g. with collapsed spaces)
            key2 = " ".join(decoded.split()).lower()
            if key2 and key2 not in title_to_path:
                title_to_path[key2] = path
            continue

        # Normal stem: use as slug and as "title" (underscores -> spaces)
        normal_title = stem.replace("_", " ").strip()
        slug_normal = slug(normal_title)
        if slug_normal and slug_normal not in title_to_path:
            title_to_path[slug_normal] = path
        key_lower = normal_title.lower()
        if key_lower and key_lower not in title_to_path:
            title_to_path[key_lower] = path
        # Compact (no spaces/underscores) so INCARNATOR₀₀ matches INCARNATOR_00.jpg
        key_compact = normal_title.lower().replace(" ", "").replace("_", "")
        if key_compact and key_compact not in title_to_path:
            title_to_path[key_compact] = path
        # Exact stem (lower) for URLs that use stem as filename
        if stem.lower() not in title_to_path:
            title_to_path[stem.lower()] = path

    return title_to_path


def _rel(path: Path, images_dir: Path) -> str:
    """Path relative to project root (parent of images_dir)."""
    base = images_dir.parent.resolve()
    return str(path.resolve().relative_to(base))


def find_image_for_title(title: str, title_to_path: dict[str, Path], images_dir: Path) -> str | None:
    """
    Return relative path (e.g. wiki_images/File.jpg) for the song title, or None.
    """
    if not title or not title.strip():
        return None
    t = title.strip()
    k = _normalize_digits(t).lower()
    s = slug(t)
    # 1) Exact match (case-insensitive) – covers decoded Greek etc.
    if k in title_to_path:
        return _rel(title_to_path[k], images_dir)
    # 2) Slug match
    if s in title_to_path:
        return _rel(title_to_path[s], images_dir)
    # 3) Encoded stem / normalized digits: e.g. INCARNATOR₀₀ → incarnator00
    key2 = " ".join(t.split()).lower()
    key2 = _normalize_digits(key2)
    if key2 in title_to_path:
        return _rel(title_to_path[key2], images_dir)
    # 4) Substring / prefix: e.g. "~ +" might match a file key
    for key, path in title_to_path.items():
        if s and (key.startswith(s) or s.startswith(key)):
            return _rel(path, images_dir)
    return None


def update_csv_image_urls(
    csv_path: Path,
    images_dir: Path,
    output_path: Path | None = None,
    title_column: str = "title",
    image_url_column: str = "imageUrl",
) -> tuple[int, int]:
    """
    Read CSV, set imageUrl to wiki_images/... for each row by matching title.
    Returns (rows_updated, rows_total).
    """
    output_path = output_path or csv_path
    title_to_path = build_title_to_file_map(images_dir)
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            rows.append(row)
    if not rows:
        return 0, 0

    updated = 0
    for row in rows:
        title = row.get(title_column, "").strip()
        rel = find_image_for_title(title, title_to_path, images_dir)
        if rel:
            row[image_url_column] = rel
            updated += 1

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return updated, len(rows)


def main():
    parser = argparse.ArgumentParser(description="Update song CSV imageUrl from wiki_images by title match")
    parser.add_argument("--csv", default="individual_songs.csv", help="Input CSV path")
    parser.add_argument("--images", default="wiki_images", help="Directory of jacket images")
    parser.add_argument("--output", "-o", default=None, help="Output CSV (default: overwrite input)")
    parser.add_argument("--title-column", default="title", help="Column name for song title")
    parser.add_argument("--image-column", default="imageUrl", help="Column name for image URL")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    images_dir = Path(args.images)
    output_path = Path(args.output) if args.output else csv_path

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 1
    if not images_dir.is_dir():
        print(f"Images dir not found: {images_dir}")
        return 1

    updated, total = update_csv_image_urls(
        csv_path,
        images_dir,
        output_path=output_path,
        title_column=args.title_column,
        image_url_column=args.image_column,
    )
    print(f"Updated {updated}/{total} rows with image paths. Output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
