#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
材质分析 CLI（新文件）
- 读取指定 CSV，使用 material_core 进行材质与可改造性分析
- 仅依赖环境变量中的 API Key，不修改任何原有文件
- 自动根据 input 路径确定输出文件与图片保存目录，减少参数设置
"""
import pandas as pd
import argparse
import os
from pathlib import Path
import sys

# 使得可以本地导入同目录下的 material_core
CURRENT_DIR = Path(__file__).resolve().parent
sys.path.append(str(CURRENT_DIR))

from material_core import analyze_csv  # noqa: E402

def parse_args():
    parser = argparse.ArgumentParser(description="材质分析：根据前5张图片与标题进行材质与改造性判断")
    parser.add_argument("--input", required=True, help="输入 CSV 路径")
    # 简化参数：自动根据 input 推导 output 与 image-dir
    parser.add_argument("--model", default="ep-20250618020820-t2x6m", help="Ark 模型名称或端点标识")
    parser.add_argument("--api-key-env", default="ARK_API_KEY", help="读取 API Key 的环境变量名，默认 ARK_API_KEY")
    return parser.parse_args()

def main():
    args = parse_args()
    # export ARK_API_KEY=
    api_key = os.environ.get(args.api_key_env)
    if not api_key:
        raise RuntimeError(f"未在环境变量 {args.api_key_env} 中找到 API Key，请先导出该环境变量")

    input_csv = Path(args.input)
    # 自动推导输出路径与图片目录
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
    # 这里增加material文件中的列拼接到原始的文件的逻辑
    df = pd.read_csv(output_csv)
    # 所有列名增加 _material 后缀
    df.columns = [col + "_material" for col in df.columns]
    df = pd.merge(df, pd.read_csv(input_csv), on='keyword', how='left')
    df.to_csv(final_output_csv, index=False)

if __name__ == "__main__":
    main()