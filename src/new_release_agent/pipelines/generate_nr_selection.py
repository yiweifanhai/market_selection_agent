import json
import os
import inspect
import dataclasses
import sys

import pandas as pd

# common config
# 不怎么改的
API_KEY = "29549de0-26ea-4e17-b73f-09ecdf08b678"
MODEL_ENDPOINT = "ep-20251109005945-gk8z8"

# import signal
# signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))

@dataclasses.dataclass
class PipelineArgs():
    new_release_json_path: str
    best_seller_json_path: str

def pipeline(
    args: PipelineArgs
):
    current_file_name = inspect.getframeinfo(inspect.currentframe()).filename
    
    src_path = f'{current_file_name}/../../../'
    src_path = os.path.abspath(src_path)
    sys.path.append(src_path)

    temp_dir = f'{current_file_name}/../../../../temp'
    temp_dir = os.path.abspath(temp_dir)

    temp_dir = f'{temp_dir}/{TASK_PREFIX}'

    print('temp dir path:', temp_dir)
    os.makedirs(temp_dir, exist_ok=True)

    # step1
    if not os.path.exists(f'{temp_dir}/new_release_products.csv'):
        step1_unwrap_new_release_json(
            new_release_json_path=args.new_release_json_path,
            output_path=f'{temp_dir}/new_release_products.csv',
        )
    
    if not os.path.exists(f'{temp_dir}/new_release_products_with_materials.csv'):
        nr_material_easy_path = step2_unwrap_product_materials(
            new_release_csv_path=f'{temp_dir}/new_release_products.csv',
            output_path=f'{temp_dir}/new_release_products_with_materials.csv',
            temp_dir=temp_dir,
        )

    if not os.path.exists(f'{temp_dir}/best_seller_products.csv'):
        step3_process_best_seller_json(
            best_seller_json_path=args.best_seller_json_path,
            output_path=f'{temp_dir}/best_seller_products.csv',
        )

    if not os.path.exists(f'{temp_dir}/new_release_best_seller_pairs.jsonl'):
        step4_generate_nr_bs_pairs(
            # new_release_csv_path=f'{temp_dir}/new_release_products_with_materials.csv',
            new_release_csv_path=nr_material_easy_path,
            best_seller_csv_path=f'{temp_dir}/best_seller_products.csv',
            output_path=f'{temp_dir}/new_release_best_seller_pairs.jsonl',
        )

    if not os.path.exists(f'{temp_dir}/llm_requests.jsonl'):
        step5_generate_llm_requests(
            temp_dir=temp_dir,
        )

    if not os.path.exists(f'{temp_dir}/llm_results.jsonl'):
        step6_schedule_batch_inference(
            temp_dir=temp_dir,
        )

    if not os.path.exists(f'{temp_dir}/llm_results.csv'):
        step7_parse_nr_results_to_csv(
            temp_dir=temp_dir,
        )

    if not os.path.exists(f'{temp_dir}/fixed_keywords_{TASK_PREFIX}.csv'):
        step8_fetch_ss_details(
            temp_dir=temp_dir,
        )

    if not os.path.exists(f'{temp_dir}/llm_results_featured_output.xlsx'):
        step9_generate_xlsx(
            temp_dir=temp_dir,
        )

def step1_unwrap_new_release_json(
    new_release_json_path: str,
    output_path: str,
):
    from new_release_agent.product_material.process_l3_products import flatten_to_rows, write_csv

    f = open(new_release_json_path, "r", encoding="utf-8")
    data = json.load(f)

    rows = flatten_to_rows(data)
    write_csv(rows, output_path)

    from new_release_agent.data.main_keyword import get_main_keyword
    x = pd.read_csv(output_path)
    x = x[(x.price < 80) & (x.price >= 16) & (x.rating >= 4.0) & (x.rating_count <= 100)]
    x = x.apply(get_main_keyword, axis=1)
    x = x.to_csv(output_path, index=False)

    print('step1 output: ', output_path)
def step2_unwrap_product_materials(
    new_release_csv_path: str,
    output_path: str,
    temp_dir: str,
):
    from new_release_agent.product_material.new_release_material_core import analyze_csv

    analyze_csv(
        input_csv=new_release_csv_path,
        output_csv=output_path,
        image_dir=f'{temp_dir}/images',
        api_key=API_KEY,
        model=MODEL_ENDPOINT,
        max_workers=6,
        save_interval=20,
        checkpoint_path=f'{temp_dir}/checkpoint_nr_material.csv',
    )

    material = pd.read_csv(output_path)

    valid_keys = [
        '容易开发', '容易进行低成本差异化改造', '容易', '容易低成本差异化改造', '容易进行低成本差异化改造。', '是',
        '容易进行低成本的差异化改造和生产', '相对容易进行低成本差异化改造', '容易进行低成本差异化改造和生产','较容易进行低成本差异化改造',
    ]
    # material = material[(material.easy_to_develop_conclusion.isin(valid_keys)) & (~material['material_conclusion'].str.contains('液体', na=False))]
    
    output_path = f'{temp_dir}/new_release_material_easy.csv'
    material.to_csv(output_path, index=False)

    print('step2 output: ', output_path)

    return output_path
def step3_process_best_seller_json(
    best_seller_json_path: str,
    output_path: str,
):
    from new_release_agent.product_material.process_l3_products import flatten_to_rows, write_csv

    f = open(best_seller_json_path, "r", encoding="utf-8")
    data = json.load(f)

    rows = flatten_to_rows(data)
    write_csv(rows, output_path)

    from new_release_agent.data.main_keyword import get_main_keyword
    x = pd.read_csv(output_path)
    # x = x[(x.price < 80) & (x.price >= 16) & (x.rating >= 4.0) & (x.rating_count <= 100)]
    x = x.apply(get_main_keyword, axis=1)
    x = x.to_csv(output_path, index=False)

    print('step3 output: ', output_path)

def step4_generate_nr_bs_pairs(
    new_release_csv_path: str,
    best_seller_csv_path: str,
    output_path: str,
):
    from new_release_agent.data.new_release_pairing import process_pair

    process_pair(
        new_release_csv=new_release_csv_path,
        best_seller_csv=best_seller_csv_path,
        output_path=output_path,
    )

    print('step4 output: ', output_path)

def step5_generate_llm_requests(
    temp_dir: str
):
    from new_release_agent.llm_inference.innovation_jsonl_generator import generate_requests

    generate_requests(
        pair_data_path=f'{temp_dir}/new_release_best_seller_pairs.jsonl',
        output_jsonl_path=f'{temp_dir}/llm_requests.jsonl',
    )

    print('step5 output: ', f'{temp_dir}/llm_requests.jsonl')

def step6_schedule_batch_inference(
    temp_dir: str
):
    from new_release_agent.llm_inference.batch_inference import batch_inference
    
    checkpoint_path = f'{temp_dir}/checkpoint_llm/'
    os.makedirs(checkpoint_path, exist_ok=True)

    batch_inference(
        input_path=f'{temp_dir}/llm_requests.jsonl',
        output_path=f'{temp_dir}/llm_results.jsonl',
        checkpoint_path=f'{checkpoint_path}',
        api_key=API_KEY,
        model=MODEL_ENDPOINT,
    )

    print('step6 output: ', f'{temp_dir}/llm_results.jsonl')

def step7_parse_nr_results_to_csv(
    temp_dir: str
):
    from new_release_agent.llm_inference.parse_nr_results_to_csv import parse_jsonl_to_csv

    parse_jsonl_to_csv(
        input_path=f'{temp_dir}/llm_results.jsonl',
        output_path=f'{temp_dir}/llm_results.csv',
    )

    print('step7 output: ', f'{temp_dir}/llm_results.csv')

# todo: mig from amz_spider
def step8_fetch_ss_details(
    temp_dir: str
):
    # keyword --> multiple asins
    df = pd.read_csv(f'{temp_dir}/llm_results.csv')
    df = df[df['innovation_score'] >= 6]
    df = df[['root_product_keyword', 'custom_id']]

    # rename to keyword / asin
    df = df.rename(columns={'root_product_keyword': 'keyword', 'custom_id': 'asin'})

    df.to_csv(f'{temp_dir}/fixed_keywords_{TASK_PREFIX}.csv', index=False)

    print('step8 output: ', f'{temp_dir}/fixed_keywords_{TASK_PREFIX}.csv')

def step9_generate_xlsx(
    temp_dir: str,
):
    llm_results_df = pd.read_csv(f'{temp_dir}/llm_results.csv')
    fixed_keywords_output_df = pd.read_csv(f'{temp_dir}/fixed_keywords_output.csv')

    llm_results_df = llm_results_df.set_index('custom_id')
    fixed_keywords_output_df = fixed_keywords_output_df.set_index('asin')

    llm_results_joined_df = llm_results_df.join(fixed_keywords_output_df, how='left')

    llm_results_joined_df.to_excel(f'{temp_dir}/llm_results_featured_output.xlsx', index=False)

if __name__ == "__main__":
    TASK_PREFIX = 'dev_task'

    pwd = os.getcwd()

    args = PipelineArgs(
        new_release_json_path='./data/l3_products_with_kwd.json',
        best_seller_json_path='./data/bs_l3_products_with_kwd.json',
    )

    # join abs path
    for k, v in vars(args).items():
        vars(args)[k] = os.path.abspath(v)

    pipeline(args)
