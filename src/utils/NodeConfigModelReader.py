import json
import logging
from typing import Dict, Optional

from src.Constants import DEFAULT_MEMORY_POWER_DRAW


node_config = None
logger = logging.getLogger(__name__)


def load_node_config():
    global node_config
    if node_config == None:
        with open('node_config_models/nodes.json') as nodes_json_data:
            node_config = json.load(nodes_json_data)
    return node_config


def get_model_governor(node_id: str, model_name: str, node_governors: Optional[Dict[str, str]] = None) -> str:
    load_node_config()
    fallback = model_name.split('_', 1)[0] if model_name else ''
    override = None
    if node_governors:
        override = node_governors.get(node_id)
    if override and node_id in node_config and override in node_config[node_id]:
        return override
    if override:
        if node_id not in node_config:
            logger.warning(
                "Ignoring node governor override for %s=%s: node is not configured; using %s",
                node_id,
                override,
                fallback,
            )
        else:
            logger.warning(
                "Ignoring node governor override for %s=%s: governor is not configured for node; using %s",
                node_id,
                override,
                fallback,
            )
    return fallback


def get_cpu_model(node_id: str) -> str:
    load_node_config()
    return node_config[node_id]['cpu_model']


def get_memory_draw(node_id: str, model_name: str, node_governors: Optional[Dict[str, str]] = None) -> float:
    load_node_config()
    try:
        governor: str = get_model_governor(node_id, model_name, node_governors)
        return node_config[node_id][governor]['mem_draw']
    except:
        return DEFAULT_MEMORY_POWER_DRAW


def get_system_cores(node_id: str) -> int:
    load_node_config()
    return node_config[node_id]['system_cores']


def get_system_memory(node_id: str) -> int:
    load_node_config()
    return node_config[node_id]['memory']
