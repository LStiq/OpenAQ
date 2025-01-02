from config.settings import API_BASE_URL, AQ_KEY
import requests

def get_headers():
    """Retourne les en-têtes pour les requêtes avec un jeton valide."""
    return {
        'X-API-Key': AQ_KEY
    }

def get_countries():
    headers = get_headers()
    response = requests.get(f"{API_BASE_URL}countries", headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch countries data : {response.status_code}, {response.text}")
    return response.json()

def get_parameters():
    headers = get_headers()
    response = requests.get(f"{API_BASE_URL}parameters", headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch parameters data : {response.status_code}, {response.text}")
    return response.json()
