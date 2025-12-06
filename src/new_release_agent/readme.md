整体流程
1. 把new Release的json转csv展开
    product_material/process_l3_products.py 
2. 从csv分析产品对应的material 
    product_material/new_release_material_cli 
3. 处理BS的json
    /data/BS_data_processer.ipynb
4. 构造new release和BS的pair数据
     /data/new_release_BS_pairing.py
5. 构造批量推理任务
    llm_inference/innovation_jsonl_generator.py
6. 解析批量推理的结果
    llm_parser/parse_nr_results_to_csv.py


-- 可视化
    上传pair数据，查看new release产品对应的BS竞品