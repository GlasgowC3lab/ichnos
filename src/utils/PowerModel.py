from typing import Any
import json
import src.utils.MathModels as MathModels
from src.Constants import DEFAULT_MEMORY_POWER_DRAW

def get_power_model(model_name: str) -> Any:
    """
    Return a power model function requested based on stored configuration data
    The model name is formatted, split by underscores, in the pattern of 'node id', 
    then the 'governor' setting and finally the type of 'power model'.
    Note that the 'minmax' model scales between the lower and upper bound, and that the fitted linear is preferred version where possible.
    """
    print(f'Model Name Provided: {model_name}')

    with open('node_config_models/nodes.json') as nodes_json_data:
        models = json.load(nodes_json_data)

        # Get the model data
        model_data = model_name.split('_')
        node_id: str = model_data[0]
        governor: str = model_data[1]
        model_type: str = model_data[2]

        if model_type == 'minmax':
            min_watts = models[node_id][governor]['min_watts']
            max_watts = models[node_id][governor]['max_watts']
            return MathModels.min_max_linear_power_model(min_watts, max_watts)
        elif model_type == 'baseline':
            tdp_per_core = models[node_id][governor]['tdp_per_core']
            return float(tdp_per_core)
        elif model_type == 'linear':
            linear_vals = models[node_id][governor]['linear']
            coeff = linear_vals[0]
            inter = linear_vals[1]
            return MathModels.fitted_linear_power_model(coeff, inter)

        return MathModels.polynomial_model(models[node_id][governor][model_type])  # this should not be used...


def get_power_model_for_node(node_id: str, model_name: str) -> Any:
    print(f'Node {node_id} with power model {model_name} selected')

    with open('node_config_models/nodes.json') as nodes_json_data:
        models = json.load(nodes_json_data)

        # Get the model data
        model_data = model_name.split('_')
        governor: str = model_data[0]
        model_type: str = model_data[1]

        if model_type == 'minmax':
            min_watts = models[node_id][governor]['min_watts']
            max_watts = models[node_id][governor]['max_watts']
            return (MathModels.min_max_linear_power_model(min_watts, max_watts), min_watts)
        elif model_type == 'baseline':
            tdp_per_core = models[node_id][governor]['tdp_per_core']
            return float(tdp_per_core)
        elif model_type == 'linear':
            linear_vals = models[node_id][governor]['linear']
            coeff = linear_vals[0]
            inter = linear_vals[1]
            return (MathModels.fitted_linear_power_model(coeff, inter), inter)

        return (MathModels.polynomial_model(models[node_id][governor][model_type]), inter)  # this should not be used...
