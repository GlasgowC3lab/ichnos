from typing import Any
import src.utils.MathModels as MathModels
from src.utils.NodeConfigModelReader import load_node_config


def get_power_model_for_node(node_id: str, model_name: str) -> Any:
    print(f'Node {node_id} with power model {model_name} selected')
    node_config = load_node_config()

    # Get the model data
    model_data = model_name.split('_')
    governor: str = model_data[0]
    model_type: str = model_data[1]

    if model_type == 'minmax':
        min_watts = node_config[node_id][governor]['min_watts']
        max_watts = node_config[node_id][governor]['max_watts']
        return (MathModels.min_max_linear_power_model(min_watts, max_watts), min_watts)
    elif model_type == 'baseline':
        tdp_per_core = node_config[node_id][governor]['tdp_per_core']
        return float(tdp_per_core)
    elif model_type == 'linear':
        linear_vals = node_config[node_id][governor]['linear']
        coeff = linear_vals[0]
        inter = linear_vals[1]
        return (MathModels.fitted_linear_power_model(coeff, inter), inter)

    return (MathModels.polynomial_model(node_config[node_id][governor][model_type]), inter)  # this should not be used...
