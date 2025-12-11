import json
import os
from typing import List, Dict, Any

# Absolute paths for prompts and input data
import inspect
current_file_path = inspect.getfile(inspect.currentframe())
current_file_dir = os.path.dirname(current_file_path)

SYSTEM_PROMPT_PATH = f'{current_file_dir}/system_prompt.md'
USER_PROMPT_PATH = f'{current_file_dir}/user_prompt.md'

# SYSTEM_PROMPT_PATH = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/llm_inference/system_prompt.md"
# USER_PROMPT_PATH = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/llm_inference/user_prompt.md"

PAIR_DATA_PATH = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/pair_data/nr_bs_pair_1120_2.json"
OUTPUT_JSONL_PATH = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/llm_inference/innovation_requests_thinking.jsonl"


def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def format_benchmark_text(bench: List[Dict[str, Any]]) -> str:
    items = bench or []
    if not items:
        return "NONE"
    lines = []
    # Use the exact benchmark subset we will include images for
    for i, item in enumerate(items, start=1):
        name = item.get("product_name", "")
        asin = item.get("asin", "")
        lines.append(f"- [{i}] {name} (ASIN: {asin})")
    return "\n".join(lines)


def format_new_product_text(new: Dict[str, Any]) -> str:
    name = new.get("product_name", "")
    asin = new.get("asin", "")
    return f"- {name} (ASIN: {asin})"


def split_user_template(user_template: str, benchmark_text: str, new_text: str) -> Dict[str, str]:
    """Fill placeholders, then split the template into: header, PART A/B section, steps."""
    filled = (
        user_template
        .replace("{{BENCHMARK_DATA_HERE}}", benchmark_text)
        .replace("{{NEW_PRODUCT_DATA_HERE}}", new_text)
    )
    parts = filled.split("\n---\n")
    if len(parts) >= 3:
        header = parts[0].strip()
        a_section = parts[1].strip()
        b_section = parts[2].strip()
        steps = "\n---\n".join(parts[3:]).strip()
    else:
        # Fallback: put everything into header
        header = filled.strip()
        ab_section = ""
        steps = ""
    return {"header": header, "a_section": a_section, "b_section": b_section, "steps": steps}


def build_image_contents(new: Dict[str, Any], bench_subset: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Return image-only content blocks for PART A (benchmark) and PART B (new product)."""
    part_a_images: List[Dict[str, Any]] = []
    for item in bench_subset:
        url = item.get("image_url")
        if url:
            part_a_images.append({"type": "image_url", "image_url": {"url": str(url).strip()}})

    part_b_images: List[Dict[str, Any]] = []
    new_img = new.get("image_url")
    if new_img:
        part_b_images.append({"type": "image_url", "image_url": {"url": str(new_img).strip()}})

    return {"part_a": part_a_images, "part_b": part_b_images}


def create_multimodal_messages(system_prompt: str, segments: Dict[str, str], image_contents: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Compose messages following user_prompt.md structure without duplicating titles.
    Order: header text -> PART A text -> PART A images -> PART B text -> PART B images -> steps text.
    """
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
    ]

    header = segments.get("header", "")
    a_section = segments.get("a_section", "")
    b_section = segments.get("b_section", "")
    steps = segments.get("steps", "")

    if header:
        messages.append({"role": "user", "content": [{"type": "text", "text": header}]})
    if a_section:
        messages.append({"role": "user", "content": [{"type": "text", "text": a_section}]})
    if image_contents.get("part_a"):
        messages.append({"role": "user", "content": image_contents["part_a"]})
    # PART B section is included in ab_section (it contains both PART A/B text)
    if b_section:
        messages.append({"role": "user", "content": [{"type": "text", "text": b_section}]})
    if image_contents.get("part_b"):
        messages.append({"role": "user", "content": image_contents["part_b"]})
    if steps:
        messages.append({"role": "user", "content": [{"type": "text", "text": steps}]})

    return {"messages": messages}


def generate_requests(
    pair_data_path: str,
    output_jsonl_path: str,
):
    system_prompt = load_text(SYSTEM_PROMPT_PATH)
    user_template = load_text(USER_PROMPT_PATH)

    records_written = 0
    done_asin = set()
    with open(pair_data_path, "r", encoding="utf-8") as fin, open(output_jsonl_path, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # Skip malformed lines
                continue

            new = obj.get("new", {})
            higher = obj.get("higher_old_products", [])
            lower = obj.get("lower_old_products", [])

            # Choose benchmark subset (prefer higher, fallback lower), limited to 3 to keep message concise
            bench_all = higher or lower or []
            bench_subset = bench_all[:5]

            benchmark_text = format_benchmark_text(bench_subset)
            new_text = format_new_product_text(new)
            segments = split_user_template(user_template, benchmark_text, new_text)

            image_contents = build_image_contents(new, bench_subset)

            body = create_multimodal_messages(system_prompt, segments, image_contents)
            # 仅使用 asin 作为 custom_id，若 asin 缺失则跳过该条数据
            asin = new.get("asin")
            if not asin or asin in done_asin:
                continue
            custom_id = asin

            jsonl_record = {
                "custom_id": str(custom_id),
                "body": body,
            }
            fout.write(json.dumps(jsonl_record, ensure_ascii=False) + "\n")
            records_written += 1
            done_asin.add(asin)

    print(f"Generated {records_written} requests -> {output_jsonl_path}")


if __name__ == "__main__":
    # Allow overriding paths via env vars if needed, but keep defaults for simplicity
    PAIR = os.environ.get("PAIR_DATA_PATH", PAIR_DATA_PATH)
    OUT = os.environ.get("OUTPUT_JSONL_PATH", OUTPUT_JSONL_PATH)
    PAIR_DATA_PATH = PAIR
    OUTPUT_JSONL_PATH = OUT
    generate_requests(
        PAIR_DATA_PATH,
        OUTPUT_JSONL_PATH,
    )