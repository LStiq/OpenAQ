from spark.config.settings import API_BASE_URL, AQ_KEY, API_GOUV_FR
import requests, time
from datetime import date, timedelta


def get_headers():
    """Retourne les en-têtes pour les requêtes avec un jeton valide."""
    return {
        'X-API-Key': AQ_KEY
    }

def get_countries():
    headers = get_headers()
    response = requests.get(f"{API_BASE_URL}countries?limit=200", headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch countries data : {response.status_code}, {response.text}")
    return response.json()

def get_providers():
    headers = get_headers()
    response = requests.get(f"{API_BASE_URL}providers", headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch providers data : {response.status_code}, {response.text}")
    return response.json()

def get_parameters():
    headers = get_headers()
    response = requests.get(f"{API_BASE_URL}parameters", headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch parameters data : {response.status_code}, {response.text}")
    return response.json()

def get_world_locations(page):
    headers = get_headers()
    params = {
        "page":page,
        "limit":1000
    }
    response = requests.get(f"{API_BASE_URL}locations",params=params, headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch location data : {response.status_code}, {response.text}")
    data = response.json().get("results",[])
    if not data:
        print(f"Aucune donnée trouvée à la page {page}, arrêt.")
    return data

def get_measurements(sensors_id):
    today = date.today()
    yesterday = today - timedelta(days=1)
    headers = get_headers()
    #response = requests.get(f"{API_BASE_URL}sensors/{sensors_id}/measurements?datetime_from={yesterday}&datetime_to{today}", headers=headers, verify=False) #Jour au jour
    response = requests.get(f"{API_BASE_URL}sensors/{sensors_id}/measurements?datetime_from=2025-01-01", headers=headers, verify=False)
    if response.status_code == 429: # Trop de requêtes
        print("Pause d'1 min")
        time.sleep(60)
        return get_measurements(sensors_id)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch measurements data from sensors : {response.status_code}, {response.text}")
    return response.json().get("results",[])

def chunk_list(data_list, chunk_size):
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i:i + chunk_size]

def get_communes(lon,lat):
    response = requests.get(f"{API_GOUV_FR}?lon={lon}&lat={lat}", verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch cities data from gouv api : {response.status_code}, {response.text}")
    city = response.json()
    if not city:
        print(f"Aucune commune trouvée pour ({lon}, {lat})")
        return {}
    return city[0]