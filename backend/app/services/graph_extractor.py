"""
图谱抽取服务
使用 LLM 从文本中抽取实体和关系，写入 Neo4j 前不依赖任何第三方图谱 API
"""

import json
import re
from typing import Dict, Any, List, Optional

from ..config import Config
from ..utils.logger import get_logger
from ..utils.llm_client import LLMClient

logger = get_logger("mirofish.graph_extractor")


def _build_ontology_prompt(ontology: Dict[str, Any]) -> str:
    """将本体格式化为 LLM 可读说明"""
    parts = []
    et = ontology.get("entity_types") or []
    if et:
        parts.append("实体类型：")
        for e in et:
            name = e.get("name", "")
            desc = e.get("description", "")
            attrs = e.get("attributes") or []
            attr_str = ", ".join(a.get("name", "") for a in attrs)
            parts.append(f"  - {name}: {desc}" + (f" 属性: {attr_str}" if attr_str else ""))
    edge_types = ontology.get("edge_types") or []
    if edge_types:
        parts.append("关系类型（源->目标）：")
        for ed in edge_types:
            name = ed.get("name", "")
            desc = ed.get("description", "")
            st = ed.get("source_targets") or []
            st_str = "; ".join(f"{s.get('source','')}->{s.get('target','')}" for s in st)
            parts.append(f"  - {name}: {desc}" + (f" ({st_str})" if st_str else ""))
    return "\n".join(parts) if parts else "无预定义本体，请从文本中合理推断实体与关系。"


def extract_entities_and_relations(
    text: str,
    ontology: Dict[str, Any],
    llm_client: Optional[LLMClient] = None,
) -> Dict[str, Any]:
    """
    从一段文本中抽取实体和关系。

    Args:
        text: 原始文本
        ontology: 本体定义（entity_types, edge_types）
        llm_client: LLM 客户端，不传则使用默认（阿里百炼等）

    Returns:
        {
            "entities": [ {"type": "Person", "name": "张三", "summary": "...", "attributes": {}}, ... ],
            "relations": [ {"type": "KNOWS", "source_name": "张三", "target_name": "李四", "fact": "..."}, ... ]
        }
    """
    if not text or not text.strip():
        return {"entities": [], "relations": []}

    client = llm_client or LLMClient()
    ontology_desc = _build_ontology_prompt(ontology)

    system = """你是一个知识图谱抽取专家。根据用户给出的「本体定义」和「文本内容」，从文本中抽取实体和关系。
要求：
1. 只输出一个 JSON 对象，不要其他解释。
2. 格式为：{"entities": [...], "relations": [...]}
3. entities 中每项：type（实体类型，必须在本体中出现）、name（实体名称）、summary（一句话摘要）、attributes（对象，可选）
4. relations 中每项：type（关系类型，如 KNOWS、WORKS_AT）、source_name、target_name（与某实体的 name 一致）、fact（关系描述句子）
5. 实体名称在同一段文本内保持一致，以便 relation 的 source_name/target_name 能对应到 entities 的 name。
6. 若文本中无相关内容，返回 {"entities": [], "relations": []}。"""

    user = f"""本体定义：
{ontology_desc}

文本内容：
{text[:8000]}

请抽取实体和关系，直接输出 JSON。"""

    try:
        raw = client.chat(
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            max_tokens=4096,
        )
    except Exception as e:
        logger.warning(f"图谱抽取 LLM 调用失败: {e}")
        return {"entities": [], "relations": []}

    cleaned = raw.strip()
    cleaned = re.sub(r"^```(?:json)?\s*\n?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\n?```\s*$", "", cleaned).strip()

    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning(f"图谱抽取返回非 JSON: {cleaned[:200]}")
        return {"entities": [], "relations": []}

    entities = data.get("entities") or []
    relations = data.get("relations") or []
    if not isinstance(entities, list):
        entities = []
    if not isinstance(relations, list):
        relations = []

    return {"entities": entities, "relations": relations}
