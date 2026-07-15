from typing import Callable
import src.utils.MathModels as MathModels
from src.utils.NodeConfigModelReader import get_model_governor, load_node_config


def get_power_model_for_node(node_id: str, model_name: str, node_governors: dict[str, str] | None = None) -> Callable[[float], float]:
    node_config = load_node_config()

    # Get the model data
    model_data = model_name.split('_')
    if len(model_data) < 2:
        raise ValueError(f"Power model name must include governor and model type, got {model_name!r}")
    if node_id not in node_config:
        raise ValueError(f"Node {node_id!r} is not configured in node_config_models/nodes.json")
    governor: str = get_model_governor(node_id, model_name, node_governors)
    model_type: str = model_data[1]

    if model_type == 'minmax':
        if governor not in node_config[node_id]:
            raise ValueError(f"Node {node_id!r} does not define governor {governor!r}")
        min_watts = node_config[node_id][governor]['min_watts']
        max_watts = node_config[node_id][governor]['max_watts']
        print(f'Node {node_id} with power model {governor}_{model_type} selected')
        return (MathModels.min_max_linear_power_model(min_watts, max_watts), min_watts)
    elif model_type == 'baseline':
        tdp_per_core = node_config[node_id]['tdp_per_core']
        print(f'Node {node_id} with power model {governor}_{model_type} selected')
        return (MathModels.baseline_linear_power_model(tdp_per_core), 0)
    elif model_type == 'linear':
        if governor not in node_config[node_id]:
            raise ValueError(f"Node {node_id!r} does not define governor {governor!r}")
        linear_vals = node_config[node_id][governor]['linear']
        coeff = linear_vals[0]
        inter = linear_vals[1]
        print(f'Node {node_id} with power model {governor}_{model_type} selected')
        return (MathModels.fitted_linear_power_model(coeff, inter), inter)

    raise ValueError(
        f"Node {node_id!r} governor {governor!r} does not define model type {model_type!r}"
    )
