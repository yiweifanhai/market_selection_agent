import pandas as pd 

# x = pd.read_csv('BS_l3_products_with_kwd_20251120_flattened.csv')

'''筛选条件
19 < price < 80
rating >= 4.0 
rating_count <= 100 
'''

import re 

def get_main_keyword(row):
    if type(row['keywords']) is not str or type(row['product_name']) is not str:
        return row
    aba_keywords = [ i.lower().strip() for i in row['keywords'].split(';')]
    title = row['product_name'].lower()
    # 截取每一段由特殊字符分隔的子字符串，从左到右开始，看第一个被完全包含的aba_keywords
    # 按除了字母、空格、数字以外的所有字符切分标题
    segments = re.split(r'[^a-zA-Z0-9\s]+', title)
    keywords = []
    for seg in segments:
        seg = seg.strip()
        if seg:  # 非空片段
            # 任意一个aba_keywords是否出现在该片段中
            for kw in aba_keywords:
                if kw.lower() in seg.lower():
                    keywords.append(kw)
            if keywords:  # 一旦找到就跳出外层循环
                break
    if not keywords:  # 如果都没匹配，兜底用第一个aba_keywords
        keywords = None 
    row['main_keyword'] = max(keywords, key=len) if keywords else None
    return row 

# filtered = x[(x.price < 80) & (x.price >= 16) & (x.rating >= 4.0) & (x.rating_count <= 100)]
# filtered_main_keyword = filtered.apply(get_main_keyword, axis=1)
# filtered_main_keyword.to_csv('1120_NR.csv', index=False)