"""
图谱实体读取与过滤服务（Neo4j 本地实现）
从 Neo4j 图谱中读取节点，筛选出符合预定义实体类型的节点
"""

import time
from typing import Dict, Any, List, Optional, Set, Callable, TypeVar

from ..config import Config
from ..utils.logger import get_logger
from . import neo4j_graph

logger = get_logger("mirofish.zep_entity_reader")

T = TypeVar("T")


class EntityNode:
    """实体节点数据结构（与原有 Zep 接口兼容）"""

    def __init__(
        self,
        uuid: str,
        name: str,
        labels: List[str],
        summary: str,
        attributes: Dict[str, Any],
        related_edges: Optional[List[Dict[str, Any]]] = None,
        related_nodes: Optional[List[Dict[str, Any]]] = None,
    ):
        self.uuid = uuid
        self.name = name
        self.labels = labels or []
        self.summary = summary or ""
        self.attributes = attributes or {}
        self.related_edges = related_edges or []
        self.related_nodes = related_nodes or []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "uuid": self.uuid,
            "name": self.name,
            "labels": self.labels,
            "summary": self.summary,
            "attributes": self.attributes,
            "related_edges": self.related_edges,
            "related_nodes": self.related_nodes,
        }

    def get_entity_type(self) -> Optional[str]:
        for label in self.labels:
            if label not in ("Entity", "Node"):
                return label
        return None


class FilteredEntities:
    """过滤后的实体集合"""

    def __init__(
        self,
        entities: List[EntityNode],
        entity_types: Set[str],
        total_count: int,
        filtered_count: int,
    ):
        self.entities = entities
        self.entity_types = entity_types
        self.total_count = total_count
        self.filtered_count = filtered_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entities": [e.to_dict() for e in self.entities],
            "entity_types": list(self.entity_types),
            "total_count": self.total_count,
            "filtered_count": self.filtered_count,
        }


class ZepEntityReader:
    """
    图谱实体读取服务（Neo4j 实现）
    接口与原有 ZepEntityReader 一致，便于调用方无感替换
    """

    def __init__(self, api_key: Optional[str] = None):
        if not Config.NEO4J_PASSWORD:
            raise ValueError("NEO4J_PASSWORD 未配置")
        self._api_key = api_key

    def _call_with_retry(
        self,
        func: Callable[[], T],
        operation_name: str,
        max_retries: int = 3,
        initial_delay: float = 2.0,
    ) -> T:
        last_exception = None
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Neo4j {operation_name} 第 {attempt + 1} 次尝试失败: {str(e)[:100]}, "
                        f"{delay:.1f}秒后重试..."
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Neo4j {operation_name} 在 {max_retries} 次尝试后仍失败: {str(e)}")
        raise last_exception

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        def _get():
            return neo4j_graph.get_all_nodes(graph_id)

        nodes = self._call_with_retry(_get, f"get_all_nodes(graph={graph_id})")
        logger.info(f"共获取 {len(nodes)} 个节点")
        return nodes

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        def _get():
            return neo4j_graph.get_all_edges(graph_id)

        edges = self._call_with_retry(_get, f"get_all_edges(graph={graph_id})")
        logger.info(f"共获取 {len(edges)} 条边")
        return edges

    def get_node_edges(self, node_uuid: str) -> List[Dict[str, Any]]:
        try:
            # 需要 graph_id；这里通过任意包含该节点的图查询（通常调用方会先有 graph_id）
            # 若仅按 node_uuid 查，需在 neo4j_graph 中提供 get_edges_for_node_by_uuid(node_uuid)
            return []
        except Exception as e:
            logger.warning(f"获取节点 {node_uuid} 的边失败: {str(e)}")
            return []

    def get_node_edges_for_graph(self, graph_id: str, node_uuid: str) -> List[Dict[str, Any]]:
        return neo4j_graph.get_edges_for_node(graph_id, node_uuid)

    def filter_defined_entities(
        self,
        graph_id: str,
        defined_entity_types: Optional[List[str]] = None,
        enrich_with_edges: bool = True,
    ) -> FilteredEntities:
        all_nodes = self.get_all_nodes(graph_id)
        total_count = len(all_nodes)
        all_edges = self.get_all_edges(graph_id) if enrich_with_edges else []
        node_map = {n["uuid"]: n for n in all_nodes}
        filtered_entities = []
        entity_types_found: Set[str] = set()

        for node in all_nodes:
            labels = node.get("labels") or []
            custom_labels = [l for l in labels if l not in ("Entity", "Node")]
            if not custom_labels:
                continue
            if defined_entity_types:
                matching = [l for l in custom_labels if l in defined_entity_types]
                if not matching:
                    continue
                entity_type = matching[0]
            else:
                entity_type = custom_labels[0]
            entity_types_found.add(entity_type)
            entity = EntityNode(
                uuid=node["uuid"],
                name=node["name"] or "",
                labels=labels,
                summary=node.get("summary") or "",
                attributes=node.get("attributes") or {},
            )
            if enrich_with_edges:
                related_edges = []
                related_node_uuids = set()
                for edge in all_edges:
                    if edge.get("source_node_uuid") == node["uuid"]:
                        related_edges.append({
                            "direction": "outgoing",
                            "edge_name": edge.get("name", ""),
                            "fact": edge.get("fact", ""),
                            "target_node_uuid": edge.get("target_node_uuid"),
                        })
                        related_node_uuids.add(edge["target_node_uuid"])
                    elif edge.get("target_node_uuid") == node["uuid"]:
                        related_edges.append({
                            "direction": "incoming",
                            "edge_name": edge.get("name", ""),
                            "fact": edge.get("fact", ""),
                            "source_node_uuid": edge.get("source_node_uuid"),
                        })
                        related_node_uuids.add(edge["source_node_uuid"])
                entity.related_edges = related_edges
                entity.related_nodes = [
                    {
                        "uuid": node_map[n]["uuid"],
                        "name": node_map[n]["name"],
                        "labels": node_map[n].get("labels", []),
                        "summary": node_map[n].get("summary", ""),
                    }
                    for n in related_node_uuids
                    if n in node_map
                ]
            filtered_entities.append(entity)

        logger.info(
            f"筛选完成: 总节点 {total_count}, 符合条件 {len(filtered_entities)}, 实体类型: {entity_types_found}"
        )
        return FilteredEntities(
            entities=filtered_entities,
            entity_types=entity_types_found,
            total_count=total_count,
            filtered_count=len(filtered_entities),
        )

    def get_entity_with_context(self, graph_id: str, entity_uuid: str) -> Optional[EntityNode]:
        def _get():
            return neo4j_graph.get_node_by_uuid(entity_uuid, graph_id)

        try:
            node = self._call_with_retry(_get, f"get_node(uuid={entity_uuid[:8]}...)")
        except Exception as e:
            logger.error(f"获取实体 {entity_uuid} 失败: {str(e)}")
            return None
        if not node:
            return None
        edges = neo4j_graph.get_edges_for_node(graph_id, entity_uuid)
        all_nodes = self.get_all_nodes(graph_id)
        node_map = {n["uuid"]: n for n in all_nodes}
        related_edges = []
        related_node_uuids = set()
        for edge in edges:
            if edge.get("source_node_uuid") == entity_uuid:
                related_edges.append({
                    "direction": "outgoing",
                    "edge_name": edge.get("name", ""),
                    "fact": edge.get("fact", ""),
                    "target_node_uuid": edge.get("target_node_uuid"),
                })
                related_node_uuids.add(edge["target_node_uuid"])
            else:
                related_edges.append({
                    "direction": "incoming",
                    "edge_name": edge.get("name", ""),
                    "fact": edge.get("fact", ""),
                    "source_node_uuid": edge.get("source_node_uuid"),
                })
                related_node_uuids.add(edge["source_node_uuid"])
        related_nodes = [
            {
                "uuid": node_map[u]["uuid"],
                "name": node_map[u]["name"],
                "labels": node_map[u].get("labels", []),
                "summary": node_map[u].get("summary", ""),
            }
            for u in related_node_uuids
            if u in node_map
        ]
        return EntityNode(
            uuid=node["uuid"],
            name=node["name"] or "",
            labels=node.get("labels") or [],
            summary=node.get("summary") or "",
            attributes=node.get("attributes") or {},
            related_edges=related_edges,
            related_nodes=related_nodes,
        )

    def get_entities_by_type(
        self,
        graph_id: str,
        entity_type: str,
        enrich_with_edges: bool = True,
    ) -> List[EntityNode]:
        result = self.filter_defined_entities(
            graph_id=graph_id,
            defined_entity_types=[entity_type],
            enrich_with_edges=enrich_with_edges,
        )
        return result.entities
