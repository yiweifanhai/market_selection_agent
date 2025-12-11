import os
import json
from typing import Optional, List, Dict, Any, Literal
from volcenginesdkarkruntime import Ark

from pydantic import BaseModel
from enum import Enum

from pqdm.threads import pqdm

'''
{
  "status": "VALID",
  "comparison_mode": "REAL_BENCHMARK" 或 "INTERNAL_KNOWLEDGE", 
  "root_product_keyword": "例如：Cutting Board (仅在内部常识模式下必填)",
  "baseline_summary": "描述你用来对比的标准（是基于竞品数据总结的，还是基于常识构建的）...",
  "innovation_analysis": "分析新品哪里不一样...",
  "primary_dimension": "核心维度",
  "secondary_dimensions": ["其他维度"],
  "innovation_score": 打分0-10分,
  "one_sentence_reason": "简短的几句话理由..."
}
'''
class UserPromptResponse(BaseModel):
    status: str
    comparison_mode: str
    root_product_keyword: str
    baseline_summary: str
    innovation_analysis: str
    primary_dimension: str
    secondary_dimensions: List[str]
    innovation_score: int
    one_sentence_reason: str

def batch_inference(
    input_path: str,
    output_path: str,
    checkpoint_path: Optional[str] = None,

    max_workers: int = 6,
    api_key: str = '',
    model: str = '',
):
    client = Ark(api_key=api_key)

    def call_ark(
        messages: List[Dict],
    ) -> Dict:
        resp = client.beta.chat.completions.parse(
            model=model,
            messages=messages,
            response_format=UserPromptResponse,
            extra_body={'thinking': {'type': 'disabled'}, 'reasoning_effort': 'minimal'}
        )

        return resp.choices[0].message.parsed.model_dump()
    
    def handle_line(line: str):
        line = json.loads(line)

        task_id = line['custom_id']

        checkpoint_file = os.path.join(checkpoint_path, f'{task_id}.json')
        if os.path.exists(checkpoint_file):
            resp = json.load(open(checkpoint_file, 'r', encoding='utf-8'))
        else:
            resp = call_ark(line['body']['messages'])
            json.dump(resp, open(checkpoint_file, 'w', encoding='utf-8'), ensure_ascii=False)

        wrapped_resp = {
            'custom_id': task_id,
            'error': None,
            'response': {
                'status_code': 200,
                'body': {
                    'service_tier': 'default',
                    'choices': [{
                        'message': {
                            'content': json.dumps(resp, ensure_ascii=False),
                            'role': 'assistant',
                        }
                    }]
                }
            }
        }

        return wrapped_resp
    
    # lines = open(input_path, 'r', encoding='utf-8')
    # lines = lines.readlines()
    with open(input_path, 'r', encoding='utf-8') as fin:
        lines = fin.readlines()
        lines = [line.strip() for line in lines]

        results = pqdm(
            array=lines,
            function=handle_line,
            n_jobs=max_workers,
            exception_behaviour='immediate',
        )

        with open(output_path, 'w', encoding='utf-8') as f_out:
            for rec in results:
                f_out.write(json.dumps(rec, ensure_ascii=False) + '\n')
