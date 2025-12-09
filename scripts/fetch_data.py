import requests
import json
from datetime import datetime, timedelta, timezone
import os

def fetch_lock_level(station_id, measurement_type):
    '''Fetch current level from a lock'''
    try:
        url = f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_id}.json"
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            print(f"Error fetching {station_id}: {response.status_code}")
            return None

        station_data = response.json()
        measures = station_data.get('items', {}).get('measures', [])

        # Find the right measurement - prefer mASD (meters above sea datum)
        for measure in measures:
            measure_id = measure['@id'].split('/')[-1]
            # Check if measurement_type matches exactly (not as substring) and has mASD
            if (f'-{measurement_type}-' in measure_id or measure_id.endswith(f'-{measurement_type}')) and 'mASD' in measure_id:
                # Use latestReading from the station data if available
                latest = measure.get('latestReading')
                if latest and 'value' in latest and 'dateTime' in latest:
                    return {
                        'value': latest['value'],
                        'timestamp': latest['dateTime']
                    }

                # Fallback to fetching readings endpoint
                readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{measure_id}/readings.json"
                params = {'_limit': 1}

                r = requests.get(readings_url, params=params, timeout=30)
                if r.status_code == 200:
                    items = r.json().get('items', [])
                    if items:
                        return {
                            'value': items[0]['value'],
                            'timestamp': items[0]['dateTime']
                        }
        return None
    except Exception as e:
        print(f"Error fetching {station_id}: {e}")
        return None

def fetch_rainfall():
    '''Fetch rainfall from nearby stations in last 24 hours'''
    try:
        lat, lon = 51.7520, -1.2577
        
        url = "https://environment.data.gov.uk/flood-monitoring/id/stations.json"
        params = {'parameter': 'rainfall', 'lat': lat, 'long': lon, 'dist': 15}
        
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return None
        
        stations = response.json().get('items', [])
        total_rainfall = 0
        count = 0
        
        for station in stations[:3]:
            measures = station.get('measures', [])
            for measure in measures:
                measure_id = measure['@id'].split('/')[-1]
                
                since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat().replace('+00:00', 'Z')
                readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{measure_id}/readings.json"
                params = {'since': since, '_limit': 5000}
                
                r = requests.get(readings_url, params=params, timeout=30)
                if r.status_code == 200:
                    items = r.json().get('items', [])
                    if items:
                        station_total = sum([item['value'] for item in items])
                        total_rainfall += station_total
                        count += 1
                        break
        
        if count > 0:
            return total_rainfall / count
        return 0
        
    except Exception as e:
        print(f"Error fetching rainfall: {e}")
        return None

def main():
    print("Fetching river data...")
    
    # Note: On the Thames, Godstow is upstream of Osney
    # We want downstage from Godstow and stage from Osney
    godstow = fetch_lock_level('1302TH', 'downstage')  # Godstow downstream side
    osney = fetch_lock_level('1303TH', 'stage')  # Osney general level
    rainfall = fetch_rainfall()
    
    differential = None
    if osney and godstow:
        differential = godstow['value'] - osney['value']  # upstream - downstream
    
    data = {
        'last_updated': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'osney_lock': {
            'level': osney['value'] if osney else None,
            'timestamp': osney['timestamp'] if osney else None
        },
        'godstow_lock': {
            'level': godstow['value'] if godstow else None,
            'timestamp': godstow['timestamp'] if godstow else None
        },
        'differential': differential,
        'rainfall_24h': rainfall if rainfall is not None else 0
    }
    
    os.makedirs('data', exist_ok=True)
    with open('data/current.json', 'w') as f:
        json.dump(data, f, indent=2)
    
    print("Data saved!")
    print(f"Osney: {osney['value'] if osney else 'N/A'}m")
    print(f"Godstow: {godstow['value'] if godstow else 'N/A'}m")
    print(f"Differential: {differential if differential else 'N/A'}m")
    print(f"Rainfall: {rainfall if rainfall else 'N/A'}mm")

if __name__ == '__main__':
    main()