import json
from src.Constants import DEFAULT_MEMORY_POWER_DRAW

def get_cpu_model(node_id: str, model_name: str) -> str:
	"""
	Retrieves the CPU model from the node configuration model name.
	
	Args:
		model_name (str): The name of the node configuration model.
		
	Returns:
		str: The CPU model extracted from the node configuration model name.
	"""
 
	with open('node_config_models/nodes.json') as nodes_json_data:
		models = json.load(nodes_json_data)

		# Get the model data
		model_data = model_name.split('_')
		governor: str = model_data[0]
  
		return models[node_id][governor]['cpu_model']

def get_memory_draw(node_id: str, model_name: str) -> float:
    try:
        with open('node_config_models/nodes.json') as nodes_json_data:
            models = json.load(nodes_json_data)

            # Get the model data
            model_data = model_name.split('_')
            governor: str = model_data[0]

            return models[node_id][governor]['mem_draw']
    except:
        return DEFAULT_MEMORY_POWER_DRAW

def get_system_cores(node_id: str, model_name: str) -> int:
    with open('node_config_models/nodes.json') as nodes_json_data:
        models = json.load(nodes_json_data)

        # Get the model data
        model_data = model_name.split('_')
        governor: str = model_data[0]

        return models[node_id][governor]['system_cores']

def get_system_memory(node_id: str) -> int:
    with open('node_config_models/nodes.json') as nodes_json_data:
        models = json.load(nodes_json_data)
        return models[node_id]['memory']
