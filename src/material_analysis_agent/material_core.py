#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
材质分析核心模块（新文件）
- 提供函数以便根据 CSV 的前5张图片与标题进行材质与可改造性分析
- 不修改原有代码文件，独立、易维护
"""

import base64
import io
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
from tqdm import tqdm
import pandas as pd
import requests
from PIL import Image
from pydantic import BaseModel

# 可选导入：方舟 SDK
try:
    from volcenginesdkarkruntime import Ark
except Exception:
    Ark = None

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]  # .../market_selection_agent
PROMPT_PATH = PROJECT_ROOT / "src" / "prompts" / "material_analysis.txt"

class ProductMaterialAnalysis(BaseModel):
    easy_to_develop_reason: str
    easy_to_develop_conclusion: str
    material_reason: str
    material_conclusion: str


DEFAULT_PROMPT = (
    "你是一位专业的产品材质分析师，现在需要你根据一个关键词对应的市场下的几个产品的图片，以及产品的标题判断材质以及改造生产难度\n\n"
    "# 输入数据\n"
    "    1. 关键词\n"
    "    2. 前5个产品的标题\n"
    "    3. 关键词对应的市场的一级分类\n"
    "    4. 二级分类\n"
    "    5. 三级分类\n"
    "    6. 5个产品的图片\n\n"
    "# 主要工作\n"
    "1. 判断该市场的产品的材质。材质需要为金属、陶瓷、玻璃、塑料、橡胶、布料纤维、纸张、骨头、木材、皮革、硅胶、石头珠宝、液体、碳/石墨材料中的一种，如果不属于其中的材质，请输出你认为的材质，如果是电子电器产品不需要判断材质，结果判断为电子产品。\n"
    "2. 判断产品是否容易进行差异化改造的，从生产的角度判断是否容易进行低成本的差异化改造和生产。如3C电子电器产品需要开模会比较贵，但是陶瓷、玻璃、木材、纸张等材质的产品容易进行低成本的差异化改造。\n"
    "3. 给出最终的判断结果。\n\n"
    "# 输出\n"
    "easy_to_develop_reason # 是否容易开发该产品，不需要复杂的生产流程就可以低成本快速改造的理由\n"
    "easy_to_develop_conclusion # 是否容易开发该产品的结论\n"
    "material_reason # 材质判断的理由\n"
    "material_conclusion # 最终的材质判断结果\n\n"
    "所有输出必须为中文。"
)


def load_prompt() -> str:
    if PROMPT_PATH.exists():
        txt = PROMPT_PATH.read_text(encoding="utf-8").strip()
        if txt:
            print(f"使用自定义提示词: {PROMPT_PATH}")
            return txt
    print("使用默认提示词")
    return DEFAULT_PROMPT


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_filename(name: str) -> str:
    return "".join([c if c.isalnum() or c in (" ", "_") else "_" for c in str(name)])


def split_titles(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [t.strip() for t in str(s).split(";;") if t and t.strip()][:5]


def split_images(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [u.strip() for u in str(s).split(";;") if u and u.strip()][:5]


def download_and_save_image(url: str, dest_path: Path, retry: int = 3, timeout: int = 10) -> bool:
    for attempt in range(retry):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                img = Image.open(io.BytesIO(resp.content)).convert("RGB")
                img = img.resize((512, 512))
                ensure_dir(dest_path.parent)
                img.save(dest_path, format="JPEG")
                return True
        except Exception:
            if attempt < retry - 1:
                continue
            return False
    return False


def download_images_for_keyword(keyword: str, image_urls: List[str], save_dir: Path) -> List[Path]:
    paths: List[Path] = []
    for idx, url in enumerate(image_urls, start=1):
        filename = f"{safe_filename(keyword)}_{idx}.jpg"
        dest = save_dir / filename
        ok = download_and_save_image(url, dest)
        if ok:
            paths.append(dest)
    return paths


def encode_image_to_data_uri(image_path: Path) -> str:
    import imghdr

    data = image_path.read_bytes()
    img_type = imghdr.what(None, data) or image_path.suffix.lstrip(".")
    b64 = base64.b64encode(data).decode("utf-8")
    return f"data:image/{img_type};base64,{b64}"


def build_user_message(item: Dict[str, str], images_data_uris: List[str]) -> List[Dict]:
    keyword = str(item.get("keyword", ""))
    top5_title = split_titles(item.get("top5_title"))
    c1 = str(item.get("common_category_l1", ""))
    c2 = str(item.get("common_category_l2", ""))
    c3 = str(item.get("common_category_l3", ""))

    user_msg: List[Dict] = [
        {
            "type": "text",
            "text": (
                f"关键词：{keyword}\n前5个产品的标题分别为：{top5_title}\n"
                f"关键词对应的市场的一级分类：{c1}\n二级分类：{c2}\n三级分类：{c3} 其中5个产品的图片分别为："
            ),
        }
    ]
    for data_uri in images_data_uris:
        user_msg.append({
            "type": "image_url",
            "image_url": {"url": data_uri},
        })
    user_msg.append({
        "type": "text",
        "text": "请根据以上图片和文字，判断该市场的产品的材质以及理由。先给出理由，才给出最终的材质结果。",
    })
    return user_msg


def call_ark(user_msg: List[Dict], prompt: str, api_key: str, model: str) -> ProductMaterialAnalysis:
    if Ark is None:
        raise RuntimeError("未安装 volcenginesdkarkruntime，请先在 requirements.txt 中添加并安装")
    client = Ark(api_key=api_key)
    completion = client.beta.chat.completions.parse(
        model=model,
        messages=[{"role": "system", "content": prompt}, {"role": "user", "content": user_msg}],
        response_format=ProductMaterialAnalysis,
        extra_body={"thinking": {"type": "disabled"}},
    )
    return completion.choices[0].message.parsed


def save_checkpoint(result_df: pd.DataFrame, checkpoint_path: Path) -> None:
    """将当前进度保存为 checkpoint"""
    ensure_dir(checkpoint_path.parent)
    result_df.to_csv(checkpoint_path, index=False)
    print(f"已保存中间结果到：{checkpoint_path}")


def load_checkpoint_if_exists(checkpoint_path: Path, items: List[Dict]) -> pd.DataFrame:
    """如果存在 checkpoint，则加载并返回已处理部分的结果"""
    kept_cols = ["keyword", "top5_images", "top5_title", "common_category_l1", "common_category_l2", "common_category_l3"]
    result_df = pd.DataFrame(items)[kept_cols].copy()
    result_df["easy_to_develop_reason"] = ""
    result_df["easy_to_develop_conclusion"] = ""
    result_df["material_reason"] = ""
    result_df["material_conclusion"] = ""

    if checkpoint_path.exists():
        print(f"检测到中间结果文件：{checkpoint_path}，尝试恢复进度...")
        try:
            checkpoint = pd.read_csv(checkpoint_path)
            # 只合并非空列，避免覆盖空值
            for col in ["easy_to_develop_reason", "easy_to_develop_conclusion", "material_reason", "material_conclusion"]:
                result_df[col] = checkpoint[col].combine_first(result_df[col])
            print("已成功恢复中间进度")
        except Exception as e:
            print(f"加载中间结果失败：{e}，将重新开始")
    return result_df


def analyze_items(
    items: List[Dict],
    image_dir: Path,
    api_key: str,
    model: str,
    save_interval: int = 20,
    max_workers: int = 8,
    checkpoint_path: Optional[Path] = None,
) -> pd.DataFrame:
    kept_cols = ["keyword", "top5_images", "top5_title", "common_category_l1", "common_category_l2", "common_category_l3"]
    result_df = load_checkpoint_if_exists(checkpoint_path or Path("checkpoint_material.csv"), items)

    prompt = load_prompt()
    ensure_dir(image_dir)

    def process_one(item: Dict, idx: int) -> Dict:
        # 如果已经处理过，跳过
        if result_df.loc[idx, "easy_to_develop_reason"] and result_df.loc[idx, "easy_to_develop_reason"] != "Error":
            return {
                "index": idx,
                "easy_to_develop_reason": result_df.loc[idx, "easy_to_develop_reason"],
                "easy_to_develop_conclusion": result_df.loc[idx, "easy_to_develop_conclusion"],
                "material_reason": result_df.loc[idx, "material_reason"],
                "material_conclusion": result_df.loc[idx, "material_conclusion"],
            }

        keyword = str(item.get("keyword", ""))
        images = split_images(item.get("top5_images"))
        saved = download_images_for_keyword(keyword, images, image_dir)
        data_uris = [encode_image_to_data_uri(p) for p in saved]
        user_msg = build_user_message(item, data_uris)
        try:
            resp = call_ark(user_msg, prompt, api_key, model)
            return {
                "index": idx,
                "easy_to_develop_reason": resp.easy_to_develop_reason,
                "easy_to_develop_conclusion": resp.easy_to_develop_conclusion,
                "material_reason": resp.material_reason,
                "material_conclusion": resp.material_conclusion,
            }
        except Exception as e:
            print(f"处理第 {idx} 行失败：{e}")
            return {
                "index": idx,
                "easy_to_develop_reason": "Error",
                "easy_to_develop_conclusion": "Error",
                "material_reason": "Error",
                "material_conclusion": "Error",
            }

    total = len(items)
    processed = 0
    # 并发执行
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, item, i): i for i, item in enumerate(items)}
        for future in tqdm(as_completed(futures), total=len(items), desc="材质分析进度"):
            r = future.result()
            idx = r["index"]
            result_df.loc[idx, "easy_to_develop_reason"] = r["easy_to_develop_reason"]
            result_df.loc[idx, "easy_to_develop_conclusion"] = r["easy_to_develop_conclusion"]
            result_df.loc[idx, "material_reason"] = r["material_reason"]
            result_df.loc[idx, "material_conclusion"] = r["material_conclusion"]
            processed += 1
            # 每 save_interval 条保存一次中间结果
            if processed % save_interval == 0:
                save_checkpoint(result_df, checkpoint_path or Path("checkpoint_material.csv"))
    # 最终再保存一次
    save_checkpoint(result_df, checkpoint_path or Path("checkpoint_material.csv"))
    return result_df


def analyze_csv(
    input_csv: Path,
    output_csv: Path,
    image_dir: Path,
    api_key: str,
    model: str,
    max_workers: int = 4,
    save_interval: int = 20,
    checkpoint_path: Optional[Path] = None,
) -> None:
    df = pd.read_csv(input_csv)
    required = ["keyword", "top5_images", "top5_title", "common_category_l1", "common_category_l2", "common_category_l3"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"输入CSV缺少必需列: {c}")
    items = list(df[required].to_dict(orient="records"))

    result_df = analyze_items(
        items=items,
        image_dir=image_dir,
        api_key=api_key,
        model=model,
        save_interval=save_interval,
        max_workers=max_workers,
        checkpoint_path=checkpoint_path,
    )

    ensure_dir(output_csv.parent)
    result_df.to_csv(output_csv, index=False)
    print(f"最终结果已保存：{output_csv}")
