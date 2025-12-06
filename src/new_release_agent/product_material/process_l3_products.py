#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple transformer to flatten l3_products_with_kwd.json into a CSV with columns:
- category_display_name_1, category_display_name_2, category_display_name_3
- category_id_1, category_id_2, category_id_3
- keywords (joined string)
- asin, product_name, image_url, price, rating, rating_count, new_release_count

Usage:
    python process_l3_products.py \
        --input /Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/l3_products_with_kwd.json \
        --output /Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/l3_products_with_kwd_flattened.csv

If --output is not provided, the script will create a CSV next to the input file
with suffix "_flattened.csv".
"""

import argparse
import csv
import json
import os
from typing import Any, Dict, List


HEADERS = [
    "category_display_name_1",
    "category_display_name_2",
    "category_display_name_3",
    "category_id_1",
    "category_id_2",
    "category_id_3",
    "keywords",
    "asin",
    "product_name",
    "image_url",
    "price",
    "rating",
    "rating_count",
    "new_release_count",
]


def _safe_get(lst: List[Any], idx: int) -> str:
    try:
        val = lst[idx]
    except Exception:
        val = ""
    return "" if val is None else str(val)


def _format_keywords(keywords: Any) -> str:
    """Convert the keywords field into a single string.

    Supports formats like:
    - [[id, text], [id, text], ...]
    - ["text1", "text2", ...]
    - [{"id": ..., "text": ...}, ...]
    - any other structure -> str(value)
    """
    if keywords is None:
        return ""

    out: List[str] = []
    if isinstance(keywords, list):
        for kw in keywords:
            if isinstance(kw, (list, tuple)):
                # Prefer second element as keyword text if present
                if len(kw) >= 2:
                    out.append(str(kw[1]))
                elif len(kw) == 1:
                    out.append(str(kw[0]))
                else:
                    out.append("")
            elif isinstance(kw, dict):
                # Common keys that might store keyword text
                if "text" in kw:
                    out.append(str(kw["text"]))
                elif "keyword" in kw:
                    out.append(str(kw["keyword"]))
                elif "name" in kw:
                    out.append(str(kw["name"]))
                else:
                    out.append(str(kw))
            else:
                out.append(str(kw))
        return "; ".join(out)
    # Fallback to string for non-list types
    return str(keywords)


def flatten_to_rows(dataset: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for cat in dataset:
        category_display_name = cat.get("category_display_name") or []
        category_id = cat.get("category_id") or []
        products = cat.get("products") or []

        c1 = _safe_get(category_display_name, 0)
        c2 = _safe_get(category_display_name, 1)
        c3 = _safe_get(category_display_name, 2)
        i1 = _safe_get(category_id, 0)
        i2 = _safe_get(category_id, 1)
        i3 = _safe_get(category_id, 2)

        for p in products:
            row = {
                "category_display_name_1": c1,
                "category_display_name_2": c2,
                "category_display_name_3": c3,
                "category_id_1": i1,
                "category_id_2": i2,
                "category_id_3": i3,
                "keywords": _format_keywords(p.get("keywords")),
                "asin": p.get("asin", ""),
                "product_name": p.get("product_name", ""),
                "image_url": p.get("image_url", ""),
                "price": p.get("price", ""),
                "rating": p.get("rating", ""),
                "rating_count": p.get("rating_count", ""),
                "new_release_count": p.get("new_release_count", ""),
            }
            rows.append(row)
    return rows


def write_csv(rows: List[Dict[str, Any]], output_path: str) -> None:
    # Ensure parent dir exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    default_input = \
        "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/BS_l3_products_with_kwd_20251120.json"

    parser = argparse.ArgumentParser(description="Flatten l3 products JSON to CSV")
    parser.add_argument("--input", type=str, default=default_input,
                        help="Path to input JSON file")
    parser.add_argument("--output", type=str, default="",
                        help="Path to output CSV file (optional)")
    args = parser.parse_args()

    input_path = args.input
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if args.output:
        output_path = args.output
    else:
        base, _ = os.path.splitext(input_path)
        output_path = base + "_flattened.csv"

    # Load JSON
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("Input JSON must be a list of category objects")

    rows = flatten_to_rows(data)
    write_csv(rows, output_path)

    print(f"Done. Categories: {len(data)}, Rows: {len(rows)}")
    print(f"CSV saved to: {output_path}")


if __name__ == "__main__":
    main()