"""
Neo4j 图谱服务
本地图存储：创建图、设置本体、写入抽取结果、分页读取、关键词搜索、删除图
"""

import uuid
import json
from typing import Dict, Any, List, Optional

from ..db.neo4j_driver import get_driver
from ..utils.logger import get_logger

logger = get_logger("mirofish.neo4j_graph")

# 存储关系使用统一类型，关系类型名放在属性 rel_type 中
REL_TYPE = "REL"


def _sanitize_label(s: str) -> str:
    """Neo4j 标签：只保留字母数字下划线"""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in (s or "Entity"))


def _ensure_indexes(session):
    """确保常用查询有索引"""
    session.run(
        "CREATE INDEX entity_graph_id IF NOT EXISTS FOR (n:Entity) ON (n.graph_id)"
    )
    session.run(
        "CREATE INDEX graph_meta_id IF NOT EXISTS FOR (g:GraphMeta) ON (g.graph_id)"
    )


def create_graph(graph_id: str, name: str, description: str = "") -> str:
    """创建图谱：写入 GraphMeta 节点。"""
    driver = get_driver()
    with driver.session() as session:
        _ensure_indexes(session)
        session.run(
            """
            MERGE (g:GraphMeta {graph_id: $graph_id})
            SET g.name = $name, g.description = $description, g.ontology = null
            """,
            graph_id=graph_id,
            name=name or graph_id,
            description=description or "",
        )
    logger.info(f"创建图谱: {graph_id}")
    return graph_id


def set_ontology(graph_id: str, ontology: Dict[str, Any]) -> None:
    """将本体保存到图谱元数据节点。"""
    driver = get_driver()
    ontology_str = json.dumps(ontology, ensure_ascii=False)
    with driver.session() as session:
        session.run(
            """
            MERGE (g:GraphMeta {graph_id: $graph_id})
            SET g.ontology = $ontology
            """,
            graph_id=graph_id,
            ontology=ontology_str,
        )
    logger.info(f"设置本体: graph_id={graph_id}")


def get_ontology(graph_id: str) -> Optional[Dict[str, Any]]:
    """读取图谱本体。"""
    driver = get_driver()
    with driver.session() as session:
        r = session.run(
            "MATCH (g:GraphMeta {graph_id: $graph_id}) RETURN g.ontology AS ontology",
            graph_id=graph_id,
        )
        rec = r.single()
        if not rec or not rec["ontology"]:
            return None
        return json.loads(rec["ontology"])


def _merge_entities_and_relations(
    session,
    graph_id: str,
    entities: List[Dict],
    relations: List[Dict],
) -> None:
    """
    将一批实体与关系写入 Neo4j。
    实体按 (graph_id, name) 合并；关系用 REL 类型，rel_type 存关系类型名。
    """
    name_to_uuid: Dict[str, str] = {}

    for e in entities:
        if not e or not e.get("name"):
            continue
        entity_type = _sanitize_label(e.get("type") or "Entity")
        name = (e.get("name") or "").strip()
        if not name:
            continue
        node_uuid = str(uuid.uuid4())
        summary = e.get("summary") or ""
        attrs = e.get("attributes")
        if not isinstance(attrs, dict):
            attrs = {}
        attrs_str = json.dumps(attrs, ensure_ascii=False)

        session.run(
            """
            MERGE (n:Entity {graph_id: $graph_id, name: $name})
            ON CREATE SET n.uuid = $uuid, n.summary = $summary, n.attributes = $attributes,
                          n.entity_type = $entity_type, n.created_at = toString(datetime())
            ON MATCH SET n.summary = CASE WHEN n.summary IS NULL OR n.summary = '' THEN $summary ELSE n.summary END,
                          n.attributes = CASE WHEN n.attributes IS NULL OR n.attributes = '' THEN $attributes ELSE n.attributes END,
                          n.entity_type = COALESCE(n.entity_type, $entity_type)
            """,
            graph_id=graph_id,
            name=name,
            uuid=node_uuid,
            summary=summary,
            attributes=attrs_str,
            entity_type=entity_type,
        )
        name_to_uuid[name] = session.run(
            "MATCH (n:Entity {graph_id: $graph_id, name: $name}) RETURN n.uuid AS u",
            graph_id=graph_id,
            name=name,
        ).single()["u"]

    for r in relations:
        if not r:
            continue
        rel_type = (r.get("type") or "RELATED").replace(" ", "_").upper()[:64]
        source_name = (r.get("source_name") or "").strip()
        target_name = (r.get("target_name") or "").strip()
        fact = r.get("fact") or ""
        if not source_name or not target_name:
            continue
        su = name_to_uuid.get(source_name)
        tu = name_to_uuid.get(target_name)
        if not su:
            rec = session.run(
                "MATCH (n:Entity {graph_id: $graph_id, name: $name}) RETURN n.uuid AS u",
                graph_id=graph_id,
                name=source_name,
            ).single()
            if rec and rec["u"]:
                su = rec["u"]
                name_to_uuid[source_name] = su
        if not tu:
            rec = session.run(
                "MATCH (n:Entity {graph_id: $graph_id, name: $name}) RETURN n.uuid AS u",
                graph_id=graph_id,
                name=target_name,
            ).single()
            if rec and rec["u"]:
                tu = rec["u"]
                name_to_uuid[target_name] = tu
        if not su or not tu:
            continue
        rel_uuid = str(uuid.uuid4())
        session.run(
            """
            MATCH (a:Entity {graph_id: $graph_id, uuid: $su})
            MATCH (b:Entity {graph_id: $graph_id, uuid: $tu})
            CREATE (a)-[r:REL]->(b)
            SET r.graph_id = $graph_id, r.uuid = $rel_uuid, r.rel_type = $rel_type,
                r.fact = $fact, r.name = $rel_type, r.created_at = toString(datetime())
            """,
            graph_id=graph_id,
            su=su,
            tu=tu,
            rel_uuid=rel_uuid,
            rel_type=rel_type,
            fact=fact,
        )


def add_extracted(
    graph_id: str,
    entities: List[Dict],
    relations: List[Dict],
) -> None:
    """将抽取结果写入图谱。"""
    if not entities and not relations:
        return
    driver = get_driver()
    with driver.session() as session:
        _merge_entities_and_relations(session, graph_id, entities, relations)
    logger.debug(f"写入抽取结果: graph_id={graph_id}, entities={len(entities)}, relations={len(relations)}")


def get_all_nodes(graph_id: str, max_items: int = 2000) -> List[Dict[str, Any]]:
    """获取图谱下所有实体节点。"""
    driver = get_driver()
    with driver.session() as session:
        r = session.run(
            """
            MATCH (n:Entity)
            WHERE n.graph_id = $graph_id AND n.uuid IS NOT NULL
            RETURN n.uuid AS uuid, n.name AS name, n.summary AS summary, n.attributes AS attributes,
                   n.created_at AS created_at, n.entity_type AS entity_type
            LIMIT $limit
            """,
            graph_id=graph_id,
            limit=max_items,
        )
        rows = list(r)
    out = []
    for row in rows:
        attrs = row["attributes"]
        if isinstance(attrs, str):
            try:
                attrs = json.loads(attrs) if attrs else {}
            except Exception:
                attrs = {}
        et = row.get("entity_type") or "Entity"
        labels = ["Entity", et] if et != "Entity" else ["Entity"]
        out.append({
            "uuid": row["uuid"],
            "name": row["name"] or "",
            "labels": labels,
            "summary": row["summary"] or "",
            "attributes": attrs if isinstance(attrs, dict) else {},
            "created_at": row["created_at"],
        })
    return out


def get_all_edges(graph_id: str, max_items: int = 5000) -> List[Dict[str, Any]]:
    """获取图谱下所有边。"""
    driver = get_driver()
    with driver.session() as session:
        r = session.run(
            """
            MATCH (a:Entity)-[r:REL]->(b:Entity)
            WHERE r.graph_id = $graph_id
            RETURN r.uuid AS uuid, r.name AS name, r.fact AS fact, r.rel_type AS rel_type,
                   r.created_at AS created_at,
                   a.uuid AS source_node_uuid, b.uuid AS target_node_uuid,
                   a.name AS source_name, b.name AS target_name
            LIMIT $limit
            """,
            graph_id=graph_id,
            limit=max_items,
        )
        rows = list(r)
    return [
        {
            "uuid": row["uuid"],
            "name": row["name"] or row["rel_type"] or "",
            "fact": row["fact"] or "",
            "source_node_uuid": row["source_node_uuid"],
            "target_node_uuid": row["target_node_uuid"],
            "source_node_name": row["source_name"],
            "target_node_name": row["target_name"],
            "created_at": row["created_at"],
            "valid_at": None,
            "invalid_at": None,
            "expired_at": None,
        }
        for row in rows
    ]


def get_node_by_uuid(node_uuid: str, graph_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """按 uuid 查节点，可选限定 graph_id。"""
    driver = get_driver()
    with driver.session() as session:
        if graph_id:
            r = session.run(
                "MATCH (n:Entity {graph_id: $graph_id, uuid: $uuid}) RETURN n",
                graph_id=graph_id,
                uuid=node_uuid,
            )
        else:
            r = session.run(
                "MATCH (n:Entity {uuid: $uuid}) RETURN n",
                uuid=node_uuid,
            )
        rec = r.single()
        if not rec:
            return None
        n = rec["n"]
        attrs = n.get("attributes")
        if isinstance(attrs, str):
            try:
                attrs = json.loads(attrs) if attrs else {}
            except Exception:
                attrs = {}
        et = n.get("entity_type") or "Entity"
        labels = ["Entity", et] if et != "Entity" else ["Entity"]
        return {
            "uuid": n.get("uuid"),
            "name": n.get("name") or "",
            "labels": labels,
            "summary": n.get("summary") or "",
            "attributes": attrs if isinstance(attrs, dict) else {},
            "created_at": n.get("created_at"),
        }


def get_edges_for_node(graph_id: str, node_uuid: str) -> List[Dict[str, Any]]:
    """获取与某节点相连的所有边。"""
    driver = get_driver()
    with driver.session() as session:
        r = session.run(
            """
            MATCH (a:Entity)-[r:REL]-(b:Entity)
            WHERE r.graph_id = $graph_id AND (a.uuid = $node_uuid OR b.uuid = $node_uuid)
            RETURN r.uuid AS uuid, r.name AS name, r.fact AS fact,
                   a.uuid AS source_node_uuid, b.uuid AS target_node_uuid
            """,
            graph_id=graph_id,
            node_uuid=node_uuid,
        )
        rows = list(r)
    return [
        {
            "uuid": row["uuid"],
            "name": row["name"] or "",
            "fact": row["fact"] or "",
            "source_node_uuid": row["source_node_uuid"],
            "target_node_uuid": row["target_node_uuid"],
        }
        for row in rows
    ]


def search_graph(
    graph_id: str,
    query: str,
    limit: int = 10,
    scope: str = "edges",
) -> tuple[List[str], List[Dict], List[Dict]]:
    """
    关键词搜索。返回 (facts, edges, nodes)。
    """
    q = (query or "").strip().lower()
    if not q:
        return [], [], []

    keywords = [w for w in q.replace(",", " ").replace("，", " ").split() if len(w) > 1]
    facts = []
    edges_out = []
    nodes_out = []

    driver = get_driver()
    with driver.session() as session:
        if scope in ("edges", "both"):
            r = session.run(
                """
                MATCH (a:Entity)-[r:REL]->(b:Entity)
                WHERE r.graph_id = $graph_id
                  AND (toLower(r.fact) CONTAINS $q OR toLower(toString(r.name)) CONTAINS $q)
                RETURN r.uuid AS uuid, r.name AS name, r.fact AS fact,
                       a.uuid AS su, b.uuid AS tu, a.name AS sn, b.name AS tn
                LIMIT $limit
                """,
                graph_id=graph_id,
                q=q,
                limit=limit,
            )
            for row in r:
                if row["fact"]:
                    facts.append(row["fact"])
                edges_out.append({
                    "uuid": row["uuid"],
                    "name": row["name"] or "",
                    "fact": row["fact"] or "",
                    "source_node_uuid": row["su"],
                    "target_node_uuid": row["tu"],
                    "source_node_name": row["sn"],
                    "target_node_name": row["tn"],
                })
        if scope in ("nodes", "both"):
            r = session.run(
                """
                MATCH (n:Entity)
                WHERE n.graph_id = $graph_id AND n.uuid IS NOT NULL
                  AND (toLower(n.name) CONTAINS $q OR toLower(n.summary) CONTAINS $q)
                RETURN n.uuid AS uuid, n.name AS name, n.summary AS summary, n.entity_type AS entity_type
                LIMIT $limit
                """,
                graph_id=graph_id,
                q=q,
                limit=limit,
            )
            for row in r:
                if row.get("summary"):
                    facts.append(f"[{row['name']}]: {row['summary']}")
                et = row.get("entity_type") or "Entity"
                labels = ["Entity", et] if et != "Entity" else ["Entity"]
                nodes_out.append({
                    "uuid": row["uuid"],
                    "name": row["name"] or "",
                    "labels": labels,
                    "summary": row.get("summary") or "",
                })
    return facts, edges_out, nodes_out


def delete_graph(graph_id: str) -> None:
    """删除图谱：删除该 graph_id 下所有边、节点及 GraphMeta。"""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            "MATCH (a:Entity)-[r:REL]->(b:Entity) WHERE r.graph_id = $graph_id DELETE r",
            graph_id=graph_id,
        )
        session.run(
            "MATCH (n:Entity) WHERE n.graph_id = $graph_id DELETE n",
            graph_id=graph_id,
        )
        session.run(
            "MATCH (g:GraphMeta {graph_id: $graph_id}) DELETE g",
            graph_id=graph_id,
        )
    logger.info(f"已删除图谱: {graph_id}")
