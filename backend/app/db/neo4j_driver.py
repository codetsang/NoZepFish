"""
Neo4j 驱动封装
单例连接，供图谱构建、实体读取、检索等使用
"""

import time
from typing import Optional
from neo4j import GraphDatabase, Driver

from ..config import Config
from ..utils.logger import get_logger

logger = get_logger("mirofish.neo4j_driver")

_driver: Optional[Driver] = None


def get_driver() -> Driver:
    """获取 Neo4j 驱动单例（首次连接失败时会重试几次，便于 Docker 下 Neo4j 未就绪）"""
    global _driver
    if _driver is None:
        if not Config.NEO4J_PASSWORD:
            raise ValueError("NEO4J_PASSWORD 未配置")
        _driver = GraphDatabase.driver(
            Config.NEO4J_URI,
            auth=(Config.NEO4J_USER, Config.NEO4J_PASSWORD),
        )
        for attempt in range(5):
            try:
                _driver.verify_connectivity()
                logger.info("Neo4j 驱动已初始化")
                break
            except Exception as e:
                if attempt < 4:
                    logger.warning(f"Neo4j 连通性检查失败（{attempt + 1}/5），5 秒后重试: {e}")
                    time.sleep(5)
                else:
                    logger.warning(f"Neo4j 初次连通性检查失败，后续操作可能报错: {e}")
    return _driver


def close_driver():
    """关闭驱动（应用退出时调用）"""
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        logger.info("Neo4j 驱动已关闭")
