import json
from src.Constants import DEFAULT_MEMORY_POWER_DRAW


node_config = None


def load_node_config():
    global node_config
    if node_config == None:
        with open('node_config_models/nodes.json') as nodes_json_data:
            node_config = json.load(nodes_json_data)
    return node_config


def get_cpu_model(node_id: str, model_name: str) -> str:
    load_node_config()
    model_data = model_name.split('_')
    governor: str = model_data[0]
    return node_config[node_id][governor]['cpu_model']


def get_memory_draw(node_id: str, model_name: str) -> float:
    load_node_config()
    try:
        model_data = model_name.split('_')
        governor: str = model_data[0]
        return node_config[node_id][governor]['mem_draw']
    except:
        return DEFAULT_MEMORY_POWER_DRAW


def get_system_cores(node_id: str, model_name: str) -> int:
    load_node_config()
    model_data = model_name.split('_')
    governor: str = model_data[0]
    return node_config[node_id][governor]['system_cores']


def get_system_memory(node_id: str) -> int:
    load_node_config()
    return node_config[node_id]['memory']
