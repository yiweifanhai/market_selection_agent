import pandas as pd 

# df = pd.read_csv('/Users/shuxin/projects/market_analysis/LLM_market_selection/V2/data/20250802_all_10w_with_rank_ratio.csv')
# df.rename(columns={df.columns[0]: 'keyword'}, inplace=True)
# new_df = df[(df['price_avg'] >= 20) & (df['m1_mid'] >= 0.50)]
# print(f"主数据筛选后数量: {len(new_df)}")

# df2 = pd.read_csv('/Users/shuxin/projects/market_analysis/LLM_market_selection/NR/new_release_20250830.csv')
# new_df2 = df2[(df2['price_avg'] >= 15) & (df2['m1_mid'] >= 0.45)]

# 按照要求筛选数据：price_avg >= 18 且 m1_mid > 0.55
# new_df = df[((df['price_avg'] >= 20) & (df['m1_mid'] >= 0.50)) | (df['relative_available_date_days_mid'] < 365)]

folder_path = '/Users/shuxin/projects/market_analysis/LLM_market_selection/V2/'
file_path = 'data/20250913_part_1_with_rank_ratio.csv'

def get_df():
    df= all_word = pd.read_csv(folder_path + file_path)
    df = df.rename(columns={all_word.columns[0]: 'keyword'})
    new_df = df[((df['price_avg'] >= 20) 
              & (df['m1_mid'] >= 0.50) 
              # & (df['cr_by_product_cr5'] <= 0.8) 
                # & (df['score'] >= 6.5) 
              )]
    print(len(new_df))
    return new_df

merged_df = get_df() 

# merged_df = pd.concat([
#     new_df[~new_df['keyword'].isin(new_df2['keyword'])],
#     new_df2
# ], ignore_index=True)
print(f"合并去重后总数量: {len(merged_df)}")

import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from PIL import Image
import io
import time
from tqdm import tqdm

# 保存图片的文件夹
# 从file_path中提取文件名作为保存目录
current_dir = os.path.dirname(os.path.abspath(__file__))

file_name = os.path.splitext(os.path.basename(file_path))[0]
save_dir = current_dir + f"/downloaded_images_{file_name}"
os.makedirs(save_dir, exist_ok=True)

def download_and_save_image(url, keyword, idx, img_idx, retry=3, timeout=10):
    """
    下载图片并保存到本地，文件名为 keyword_idx_imgidx.jpg
    """
    for attempt in range(retry):
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 200:
                img = Image.open(io.BytesIO(response.content)).convert('RGB')
                img = img.resize((512, 512))
                # 文件名处理，防止特殊字符
                safe_keyword = "".join([c if c.isalnum() or c in (' ', '_') else '_' for c in str(keyword)])
                filename = f"{safe_keyword}_{idx}_{img_idx+1}.jpg"
                filepath = os.path.join(save_dir, filename)
                img.save(filepath, format="JPEG")
                return True
        except Exception as e:
            if attempt < retry - 1:
                time.sleep(1)
            else:
                return False
    return False

def prepare_download_tasks(df):
    """
    生成所有图片下载任务
    """
    tasks = []
    for idx, row in df.iterrows():
        keyword = row['keyword']
        urls = str(row['top5_images']).split(';;')
        for img_idx, url in enumerate(urls[:5]):
            if url and url.strip():
                tasks.append((url, keyword, idx, img_idx))
    return tasks

download_tasks = prepare_download_tasks(merged_df)
total_success = 0
total_fail = 0

max_workers = 10  # 超级多线程

# tqdm的position参数和手动刷新，确保进度条显示正常
with tqdm(total=len(download_tasks), desc="下载图片", position=0, leave=True, dynamic_ncols=True) as pbar:
    def task_wrapper(args):
        url, keyword, idx, img_idx = args
        return download_and_save_image(url, keyword, idx, img_idx)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {executor.submit(task_wrapper, task): task for task in download_tasks}
        for i, future in enumerate(as_completed(future_to_task), 1):
            success = future.result()
            if success:
                total_success += 1
            else:
                total_fail += 1
            # 只用refresh刷新，防止进度条卡住
            pbar.set_postfix({"成功数": total_success, "失败数": total_fail})
            pbar.update(1)
            pbar.refresh()

print(f"图片下载完成。成功数: {total_success}，失败数: {total_fail}")
