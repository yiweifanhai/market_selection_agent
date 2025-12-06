请分析以下产品数据。
注意：[PART A] 可能为空。如果为空，请触发“内部常识判断模式”。

---
**[PART A: Benchmark Group (老爆款/参照系)]**
这是产品标题 {{BENCHMARK_DATA_HERE}} 以下是产品图片

---
**[PART B: New Product (待验证新品)]**
这是产品标题 {{NEW_PRODUCT_DATA_HERE}} 以下是产品图片
---

请严格按照以下逻辑步骤进行分析并输出 JSON：

**Step 1: 确定基准 (Establish Baseline)**
- **情况 A (如果 [PART A] 有数据):** 基于 [PART A] 的内容，总结该品类的“平庸标准”。
- **情况 B (如果 [PART A] 为 "NONE" 或数据无效):** 1. 首先提取 [PART B] 的**核心产品词 (Root Keyword)**（例如把 "Cute Dinosaur Taco Holder" 还原为 "Taco Holder"）。
  2. 调取你的**内部常识库**，构建该产品在亚马逊市场上的“大众化画像”（通常是什么材质？什么形状？有什么标配功能？）。这将作为你的“虚拟基准”。

**Step 2: 差异识别 (Identify Deviation)**
将 [PART B] 与 Step 1 确定的基准（无论是真实的还是虚拟的）进行对比。
- 新品是否在形态、场景、功能或材质上，显著背离了你认知的“标准品”？
- *警告：* 如果你使用的是“内部常识”，请采取保守策略。不要把现代标配（如 Type-C 接口、普通防水）误判为颠覆性创新。在使用“内部常识模式”时，请务必自我质疑： "这个所谓的创新点（例如无线、便携），在最近 2 年的亚马逊市场上是否已经泛滥？" 只有当你确信这是一个极少见的设计时，才给予 7 分以上的高分。如果不确定，请给予 4-6 分的“中度优化”评价。

**Step 3: 维度匹配 (Categorize)**
(同上：匹配 10 大创新维度，从强到弱排序)

**Step 4: 评分 (Scoring)**
(同上：0-10分打分)

**Output Format (JSON Only):**
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