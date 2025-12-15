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

def fetch_rainfall(hours=24):
    '''Fetch rainfall from nearby stations for specified hours'''
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

                since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace('+00:00', 'Z')
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

def fetch_weather_forecast():
    '''Fetch 24-hour weather forecast from Open-Meteo API for Oxford'''
    try:
        # Oxford coordinates
        lat, lon = 51.7520, -1.2577
        url = f"https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': 'temperature_2m,precipitation_probability,weather_code',
            'forecast_hours': 24,
            'timezone': 'Europe/London'
        }

        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return None

        data = response.json()
        hourly = data.get('hourly', {})

        # Get next 24 hours of data
        forecast = []
        times = hourly.get('time', [])[:24]
        temps = hourly.get('temperature_2m', [])[:24]
        precip_prob = hourly.get('precipitation_probability', [])[:24]
        weather_codes = hourly.get('weather_code', [])[:24]

        for i in range(min(24, len(times))):
            forecast.append({
                'time': times[i],
                'temperature': temps[i] if i < len(temps) else None,
                'precipitation_probability': precip_prob[i] if i < len(precip_prob) else None,
                'weather_code': weather_codes[i] if i < len(weather_codes) else None
            })

        return forecast

    except Exception as e:
        print(f"Error fetching weather forecast: {e}")
        return None

def main():
    print("Fetching river data...")

    # Load previous data to calculate flow trend
    previous_flow = None
    try:
        if os.path.exists('data/current.json'):
            with open('data/current.json', 'r') as f:
                previous_data = json.load(f)
                if previous_data.get('differential') is not None:
                    previous_flow = previous_data['differential'] - 1.63
                    print(f"Previous flow: {previous_flow}m")
    except Exception as e:
        print(f"Could not load previous data: {e}")

    # Note: On the Thames, Godstow is upstream of Osney
    # We want downstage from Godstow and stage from Osney
    godstow = fetch_lock_level('1302TH', 'downstage')  # Godstow downstream side
    osney = fetch_lock_level('1303TH', 'stage')  # Osney general level
    rainfall_24h = fetch_rainfall(24)
    rainfall_7d = fetch_rainfall(168)  # 7 days = 168 hours
    weather_forecast = fetch_weather_forecast()

    differential = None
    current_flow = None
    flow_trend = None

    if osney and godstow:
        differential = godstow['value'] - osney['value']  # upstream - downstream
        current_flow = differential - 1.63

        # Calculate trend
        if previous_flow is not None:
            flow_change = current_flow - previous_flow
            if flow_change > 0.05:
                flow_trend = 'increasing'
            elif flow_change < -0.05:
                flow_trend = 'decreasing'
            else:
                flow_trend = 'level'

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
        'flow_trend': flow_trend,
        'rainfall_24h': rainfall_24h if rainfall_24h is not None else 0,
        'rainfall_7d': rainfall_7d if rainfall_7d is not None else 0,
        'weather_forecast': weather_forecast if weather_forecast else []
    }

    os.makedirs('data', exist_ok=True)
    with open('data/current.json', 'w') as f:
        json.dump(data, f, indent=2)

    print("Data saved!")
    print(f"Osney: {osney['value'] if osney else 'N/A'}m")
    print(f"Godstow: {godstow['value'] if godstow else 'N/A'}m")
    print(f"Differential: {differential if differential else 'N/A'}m")
    print(f"Flow: {current_flow if current_flow is not None else 'N/A'}m")
    print(f"Flow trend: {flow_trend if flow_trend else 'N/A'}")
    print(f"Rainfall 24h: {rainfall_24h if rainfall_24h else 'N/A'}mm")
    print(f"Rainfall 7d: {rainfall_7d if rainfall_7d else 'N/A'}mm")
    print(f"Weather forecast: {len(weather_forecast) if weather_forecast else 0} hours")

if __name__ == '__main__':
    main()