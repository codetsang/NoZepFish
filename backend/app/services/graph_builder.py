"""
图谱构建服务
使用 Neo4j 本地存储 + LLM 抽取（仅大模型调用阿里百炼），无第三方图谱 API
"""

import uuid
import time
import threading
from typing import Dict, Any, List, Optional, Callable

from ..config import Config
from ..models.task import TaskManager, TaskStatus
from .text_processor import TextProcessor
from .graph_extractor import extract_entities_and_relations
from . import neo4j_graph


def _graph_info(graph_id: str) -> "GraphInfo":
    """从 Neo4j 统计得到 GraphInfo"""
    nodes = neo4j_graph.get_all_nodes(graph_id)
    edges = neo4j_graph.get_all_edges(graph_id)
    entity_types = set()
    for n in nodes:
        for label in n.get("labels") or []:
            if label not in ("Entity", "Node"):
                entity_types.add(label)
    return GraphInfo(
        graph_id=graph_id,
        node_count=len(nodes),
        edge_count=len(edges),
        entity_types=list(entity_types),
    )


class GraphInfo:
    """图谱信息"""

    def __init__(self, graph_id: str, node_count: int, edge_count: int, entity_types: List[str]):
        self.graph_id = graph_id
        self.node_count = node_count
        self.edge_count = edge_count
        self.entity_types = entity_types or []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "node_count": self.node_count,
            "edge_count": self.edge_count,
            "entity_types": self.entity_types,
        }


class GraphBuilderService:
    """
    图谱构建服务
    使用 Neo4j + LLM 抽取，完全本地化（仅 LLM 走阿里百炼）
    """

    def __init__(self, api_key: Optional[str] = None):
        if not Config.NEO4J_PASSWORD:
            raise ValueError("NEO4J_PASSWORD 未配置")
        self.task_manager = TaskManager()

    def build_graph_async(
        self,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str = "MiroFish Graph",
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        batch_size: int = 3,
    ) -> str:
        """异步构建图谱，返回任务 ID。"""
        task_id = self.task_manager.create_task(
            task_type="graph_build",
            metadata={"graph_name": graph_name, "chunk_size": chunk_size, "text_length": len(text)},
        )
        thread = threading.Thread(
            target=self._build_graph_worker,
            args=(task_id, text, ontology, graph_name, chunk_size, chunk_overlap, batch_size),
        )
        thread.daemon = True
        thread.start()
        return task_id

    def _build_graph_worker(
        self,
        task_id: str,
        text: str,
        ontology: Dict[str, Any],
        graph_name: str,
        chunk_size: int,
        chunk_overlap: int,
        batch_size: int,
    ):
        try:
            self.task_manager.update_task(task_id, status=TaskStatus.PROCESSING, progress=5, message="开始构建图谱...")
            graph_id = self.create_graph(graph_name)
            self.task_manager.update_task(task_id, progress=10, message=f"图谱已创建: {graph_id}")
            self.set_ontology(graph_id, ontology)
            self.task_manager.update_task(task_id, progress=15, message="本体已设置")
            chunks = TextProcessor.split_text(text, chunk_size, chunk_overlap)
            total_chunks = len(chunks)
            self.task_manager.update_task(
                task_id, progress=20, message=f"文本已分割为 {total_chunks} 个块"
            )
            self.add_text_batches(
                graph_id,
                chunks,
                batch_size,
                lambda msg, prog: self.task_manager.update_task(
                    task_id, progress=20 + int(prog * 0.7), message=msg
                ),
            )
            self.task_manager.update_task(task_id, progress=90, message="获取图谱信息...")
            graph_info = _graph_info(graph_id)
            self.task_manager.complete_task(
                task_id,
                {
                    "graph_id": graph_id,
                    "graph_info": graph_info.to_dict(),
                    "chunks_processed": total_chunks,
                },
            )
        except Exception as e:
            import traceback
            self.task_manager.fail_task(task_id, f"{str(e)}\n{traceback.format_exc()}")

    def create_graph(self, name: str) -> str:
        graph_id = f"mirofish_{uuid.uuid4().hex[:16]}"
        neo4j_graph.create_graph(
            graph_id=graph_id,
            name=name or "MiroFish Graph",
            description="MiroFish Social Simulation Graph",
        )
        return graph_id

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]) -> None:
        neo4j_graph.set_ontology(graph_id, ontology)

    def add_text_batches(
        self,
        graph_id: str,
        chunks: List[str],
        batch_size: int = 3,
        progress_callback: Optional[Callable[[str, float], None]] = None,
    ) -> List[str]:
        """分批抽取并写入 Neo4j，不再返回 episode uuid 列表（本地无需等待）。"""
        total = len(chunks)
        for i in range(0, total, batch_size):
            batch = chunks[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total + batch_size - 1) // batch_size
            if progress_callback:
                progress_callback(
                    f"处理第 {batch_num}/{total_batches} 批 ({len(batch)} 块)...",
                    (i + len(batch)) / total,
                )
            for chunk in batch:
                data = extract_entities_and_relations(chunk, neo4j_graph.get_ontology(graph_id) or {})
                neo4j_graph.add_extracted(
                    graph_id,
                    data.get("entities") or [],
                    data.get("relations") or [],
                )
            time.sleep(0.3)
        return []

    def _get_graph_info(self, graph_id: str) -> GraphInfo:
        return _graph_info(graph_id)

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        nodes = neo4j_graph.get_all_nodes(graph_id)
        edges = neo4j_graph.get_all_edges(graph_id)
        node_map = {n["uuid"]: n.get("name") or "" for n in nodes}
        nodes_data = [
            {
                "uuid": n["uuid"],
                "name": n["name"],
                "labels": n.get("labels") or [],
                "summary": n.get("summary") or "",
                "attributes": n.get("attributes") or {},
                "created_at": n.get("created_at"),
            }
            for n in nodes
        ]
        edges_data = [
            {
                "uuid": e["uuid"],
                "name": e["name"],
                "fact": e["fact"],
                "fact_type": e.get("name") or "",
                "source_node_uuid": e["source_node_uuid"],
                "target_node_uuid": e["target_node_uuid"],
                "source_node_name": node_map.get(e["source_node_uuid"], ""),
                "target_node_name": node_map.get(e["target_node_uuid"], ""),
                "attributes": {},
                "created_at": e.get("created_at"),
                "valid_at": e.get("valid_at"),
                "invalid_at": e.get("invalid_at"),
                "expired_at": e.get("expired_at"),
                "episodes": [],
            }
            for e in edges
        ]
        return {
            "graph_id": graph_id,
            "nodes": nodes_data,
            "edges": edges_data,
            "node_count": len(nodes_data),
            "edge_count": len(edges_data),
        }

    def delete_graph(self, graph_id: str) -> None:
        neo4j_graph.delete_graph(graph_id)
