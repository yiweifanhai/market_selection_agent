#!/usr/bin/env python3
import json
import csv
import sys
from pathlib import Path


def safe_get(d, path, default=None):
    cur = d
    for key in path:
        if isinstance(cur, dict):
            if key in cur:
                cur = cur[key]
            else:
                return default
        elif isinstance(cur, list):
            if isinstance(key, int):
                try:
                    cur = cur[key]
                except (IndexError, TypeError):
                    return default
            else:
                return default
        else:
            return default
    return cur


def parse_jsonl_to_csv(input_path: Path, output_path: Path):
    columns = [
        "id",
        "custom_id",
        "request_id",
        "status_code",
        "model",
        "created",
        "service_tier",
        "error",
        "status",
        "comparison_mode",
        "root_product_keyword",
        "baseline_summary",
        "innovation_analysis",
        "primary_dimension",
        "secondary_dimensions",
        "innovation_score",
        "one_sentence_reason",
    ]

    rows = []
    with input_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                top = json.loads(line)
                # extract meta fields first
                meta_id = top.get("id", "")
                meta_custom_id = top.get("custom_id", "")
                meta_error = top.get("error", "")
                meta_request_id = safe_get(top, ["response", "request_id"], "")
                meta_status_code = safe_get(top, ["response", "status_code"], "")
                meta_model = safe_get(top, ["response", "body", "model"], None) or top.get("model", "")
                meta_created = safe_get(top, ["response", "body", "created"], None) or top.get("created", "")
                meta_service_tier = safe_get(top, ["response", "body", "service_tier"], None) or top.get("service_tier", "")

                content_str = safe_get(top, ["response", "body", "choices", 0, "message", "content"], None)
                if content_str is None:
                    content_str = safe_get(top, ["choices", 0, "message", "content"], None)

                payload = {}
                if isinstance(content_str, str):
                    try:
                        payload = json.loads(content_str)
                    except json.JSONDecodeError:
                        cleaned = content_str.strip()
                        # try strip code fences
                        if cleaned.startswith("```"):
                            # remove leading fence and possible language tag
                            cleaned = cleaned.split("\n", 1)[-1]
                            if cleaned.endswith("```"):
                                cleaned = cleaned[:-3]
                        # try extract JSON substring
                        start = cleaned.find("{")
                        end = cleaned.rfind("}")
                        if start != -1 and end != -1 and end > start:
                            try:
                                payload = json.loads(cleaned[start:end+1])
                            except Exception:
                                payload = {}
                elif isinstance(content_str, dict):
                    payload = content_str

                sec_dims = payload.get("secondary_dimensions")
                if isinstance(sec_dims, list):
                    sec_dims_str = " | ".join(map(str, sec_dims))
                elif sec_dims is None:
                    sec_dims_str = ""
                else:
                    sec_dims_str = str(sec_dims)

                row = [
                    meta_custom_id,
                    meta_status_code,
                    meta_service_tier,
                    meta_error,
                    payload.get("status", ""),
                    payload.get("comparison_mode", ""),
                    payload.get("root_product_keyword", ""),
                    payload.get("baseline_summary", ""),
                    payload.get("innovation_analysis", ""),
                    payload.get("primary_dimension", ""),
                    sec_dims_str,
                    payload.get("innovation_score", ""),
                    payload.get("one_sentence_reason", ""),
                ]
                rows.append(row)
            except Exception:
                # Even if parsing the top-level fails, continue to next line
                continue

    # Write CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)


if __name__ == "__main__":
    # Allow CLI args: input_jsonl output_csv
    if len(sys.argv) >= 3:
        input_file = Path(sys.argv[1]).expanduser()
        output_file = Path(sys.argv[2]).expanduser()
    else:
        # Defaults
        input_file = Path("/Users/shuxin/projects/market_selection_agent/src/new_release_agent/llm_parser/nr_nothinking_result_all.jsonl")
        output_file = Path("/Users/shuxin/projects/market_selection_agent/src/new_release_agent/llm_parser/nr_nothinking_results_all.csv")

    parse_jsonl_to_csv(input_file, output_file)
    print(f"Wrote CSV: {output_file}")