import requests

def make_json_post_request(url: str, payload: dict):
	headers = {
		"accept": "application/json",
		"Content-Type": "application/json"
	}
	response = requests.post(url, headers=headers, json=payload)
	if response.status_code == 200:
		return response.json()
	else:
		raise Exception(f"Request failed with status code {response.status_code}: {response.text}")

def make_json_get_request(url: str, api_key: str = ""):
	headers = {
		"accept": "application/json",
		"auth-token": api_key
	}
	response = requests.get(url, headers=headers)
	if response.status_code == 200:
		return response.json()
	else:
		raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
