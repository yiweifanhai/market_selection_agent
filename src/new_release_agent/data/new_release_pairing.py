import pandas as pd 

NR = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/1120_NR_material_easy.csv"
nr_df = pd.read_csv(NR)

BS = "/Users/shuxin/projects/market_selection_agent/src/new_release_agent/data/1120_BS.csv" 
bs_df = pd.read_csv(BS)

def compute_rank_in_category(bs_df: pd.DataFrame) -> pd.DataFrame:
    """Compute rank within each (c1,c2,c3) group based on file order."""
    # Preserve original order and assign incremental rank starting from 1 within each category group
    bs_df = bs_df.copy()
    bs_df["_row_index"] = range(len(bs_df))
    # Sort by original index to be explicit
    bs_df = bs_df.sort_values("_row_index")
    bs_df["rank_in_category"] = (
        bs_df.groupby([
            "category_display_name_1",
            "category_display_name_2",
            "category_display_name_3",
        ], sort=False).cumcount() + 1
    )
    return bs_df

bs_df_ranked = compute_rank_in_category(bs_df)


import argparse
import json
import os
from typing import Dict, List, Tuple

grouped: Dict[Tuple[str, str, str], pd.DataFrame] = {}
for cat_tuple, g in bs_df_ranked.groupby(
    [
        "category_display_name_1",
        "category_display_name_2",
        "category_display_name_3",
    ], sort=False
):
    grouped[cat_tuple] = g

def _normalize_kw(s: str) -> str:
    return str(s or "").strip().lower()

def _contains_main_keyword(bs_product_name: str, nr_product_name: str) -> bool:
    # 如果两个产品的前10个单词中有两个连续的单词相同，则认为是包含的
    # 补充大小写等文本处理逻辑，例如转换为小写
    tokens1 = [_normalize_kw(w) for w in bs_product_name.split()[:10]]
    bs_chunk = ' '.join(tokens1)
    tokens2 = [_normalize_kw(w) for w in nr_product_name.split()[:10]]

    # 检查两个产品名在前10个单词中是否存在连续两个单词相同
    for i in range(len(tokens2) - 1):
        words = tokens2[i] + ' ' + tokens2[i + 1]
        if words in bs_chunk:
            return True
    return False

def _keyword_same(bs_keyword, nr_keyword):
    # 检查两个产品的关键词是否相同
    return _normalize_kw(bs_keyword) == _normalize_kw(nr_keyword)

results: List[Dict] = []

for _, row in nr_df.iterrows():
    c1 = row["category_display_name_1"]
    c2 = row["category_display_name_2"]
    c3 = row["category_display_name_3"]
    asin_nr = row["asin"]
    product_name = row['product_name']
    cat_key = (c1, c2, c3)
    bs_cat = grouped.get(cat_key, pd.DataFrame(columns=bs_df_ranked.columns))
    # 满足_contains_main_keyword or _keyword_same
    bs_same_kw = bs_cat[
        bs_cat["product_name"].apply(lambda s: _contains_main_keyword(s, product_name)) 
    ]
    # Locate NR asin in the BS category list (full list, not only same keyword)
    new_rank = None
    if not bs_cat.empty:
        matches = bs_cat[bs_cat["asin"] == asin_nr]
        if len(matches) > 0:
            new_rank = int(matches.iloc[0]["rank_in_category"])

    higher_old: List[Dict] = []
    lower_old: List[Dict] = []
    if new_rank is None:
        # 则所有包含关键词的BS都作为 higher_old_products
        higher_old = [
            {
                "asin": r["asin"],
                "image_url": r["image_url"],
                "product_name": r["product_name"],
            }
            for _, r in bs_same_kw.iterrows()
        ]

    if not bs_same_kw.empty and new_rank is not None:
        for _, r in bs_same_kw.iterrows():
            if r["asin"] == asin_nr:
                # 排除新品自身
                continue
            rec = {
                "asin": r["asin"],
                "image_url": r["image_url"],
                "product_name": r["product_name"],
            }
            if int(r["rank_in_category"]) < new_rank:
                higher_old.append(rec)
            elif int(r["rank_in_category"]) > new_rank:
                lower_old.append(rec)
            else:
                # 同 rank 理论不会出现，但保持健壮性
                pass

    results.append({
        "new": {
            "asin": asin_nr,
            "image_url": row["image_url"],
            "product_name": row["product_name"],
            "category_display_name_1": c1,
            "category_display_name_2": c2,
            "category_display_name_3": c3,
        },
        "higher_old_products": higher_old,
        "lower_old_products": lower_old,
    })

len([ i for i in results if len(i['lower_old_products'])>0])
len([ i for i in results if len(i['higher_old_products'])>0])

with open('nr_bs_pair_1120_2', "w", encoding="utf-8") as f:
    for rec in results:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")