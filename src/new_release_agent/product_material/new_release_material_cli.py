#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新品材质与可开发性分析 CLI
- 复用 new_release_core.analyze_csv
- 自动根据输入路径推导输出与图片目录
- 与 material_analysis_agent 的 CLI 行为一致
"""
import argparse
import os
from pathlib import Path
import sys
import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from new_release_core import analyze_csv  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="新品材质与可开发性分析：根据图片与标题进行判断")
    parser.add_argument("--input", required=True, help="输入 CSV 路径，如 /src/new_release_agent/data/1120_NR.csv")
    parser.add_argument("--model", default="ep-20251109005945-gk8z8", help="Ark 模型名称或端点标识")
    parser.add_argument("--api-key-env", default="ARK_API_KEY", help="读取 API Key 的环境变量名，默认 ARK_API_KEY")
    return parser.parse_args()


def main():
    args = parse_args()
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"未在环境变量 {args.api_key_env} 中找到 API Key，请先导出该环境变量")

    input_csv = Path(args.input)
    output_csv = input_csv.with_name(f"{input_csv.stem}_material.csv")
    final_output_csv = input_csv.with_name(f"{input_csv.stem}_material_merge.csv")
    image_dir = input_csv.parent / f"{input_csv.stem}_images"

    analyze_csv(
        input_csv=input_csv,
        output_csv=output_csv,
        image_dir=image_dir,
        api_key=api_key,
        model=args.model,
        checkpoint_path=output_csv.with_name(f"{input_csv.stem}_checkpoint.csv"),
    )

    # # 将 material 列拼接回原始 NR 文件，保持一致输出风格
    # df_material = pd.read_csv(output_csv)
    # # 与 material_cli 的做法保持一致：所有列名增加 _material 后缀
    # df_material.columns = [col + "_material" for col in df_material.columns]
    # df_nr = pd.read_csv(input_csv)
    # # 按 main_keyword 进行左连接
    # if "main_keyword" not in df_nr.columns:
    #     raise RuntimeError("原始 NR CSV 中找不到 main_keyword 列")
    # merged = pd.merge(df_material, df_nr, left_on="main_keyword_material", right_on="main_keyword", how="left")
    # merged.to_csv(final_output_csv, index=False)
    # print(f"最终合并结果已保存：{final_output_csv}")


if __name__ == "__main__":
    main()