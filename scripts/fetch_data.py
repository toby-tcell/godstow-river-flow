import requests
import json
from datetime import datetime, timedelta, timezone
import os

# Hardcoded measure IDs (these don't change)
MEASURE_IDS = {
    'godstow': '1302TH-level-downstage-i-15_min-mASD',
    'osney': '1303TH-level-stage-i-15_min-mASD',
    'farmoor': '1100TH-flow--Mean-15_min-m3_s'
}

def fetch_all_readings_for_period(measure_id, since_timestamp):
    '''Fetch all readings for a measure since a given timestamp'''
    try:
        readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{measure_id}/readings.json"
        params = {'since': since_timestamp, '_limit': 10000, '_sorted': ''}

        r = requests.get(readings_url, params=params, timeout=60)
        if r.status_code == 200:
            items = r.json().get('items', [])
            return [{'timestamp': item['dateTime'], 'value': item['value']} for item in items]
        return []
    except Exception as e:
        print(f"Error fetching readings: {e}")
        return []

def update_history(measure_id, existing_history, days=14):
    '''
    Update history by:
    1. Checking what data we already have
    2. Fetching all data for the last 14 days from the API
    3. Merging with existing data (API data fills gaps, existing data preserved)
    4. Trimming to 14 days
    '''
    # Calculate 14 days ago
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace('+00:00', 'Z')

    # Fetch all available data for the last 14 days
    print(f"Fetching {days}-day history for {measure_id}")
    api_readings = fetch_all_readings_for_period(measure_id, cutoff)
    print(f"  API returned {len(api_readings)} readings")

    # Create dict from existing history
    history_dict = {r['timestamp']: r['value'] for r in existing_history}
    existing_count = len(history_dict)

    # Merge API readings (will add new timestamps, update existing)
    for r in api_readings:
        history_dict[r['timestamp']] = r['value']

    # Filter to last 14 days and sort (newest first)
    filtered = [{'timestamp': ts, 'value': val} for ts, val in history_dict.items() if ts >= cutoff]
    filtered.sort(key=lambda x: x['timestamp'], reverse=True)

    print(f"  History: {existing_count} existing + {len(api_readings)} from API = {len(filtered)} total (after dedup/trim)")

    return filtered

def fetch_lock_level(station_id, measurement_type):
    '''Fetch current level from a lock - tries multiple methods to get latest data'''
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
                print(f"Found measure: {measure_id}")

                # Use latestReading from the station data if available
                latest = measure.get('latestReading')
                if latest and 'value' in latest and 'dateTime' in latest:
                    print(f"Using latestReading: {latest['value']}m at {latest['dateTime']}")
                    return {
                        'value': latest['value'],
                        'timestamp': latest['dateTime']
                    }

                # Fallback to fetching readings endpoint with more data points
                print(f"No latestReading, trying readings endpoint...")
                readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{measure_id}/readings.json"
                params = {'_limit': 100}  # Get more readings to find recent data

                r = requests.get(readings_url, params=params, timeout=30)
                if r.status_code == 200:
                    items = r.json().get('items', [])
                    if items:
                        print(f"Found {len(items)} readings, using most recent: {items[0]['value']}m at {items[0]['dateTime']}")
                        return {
                            'value': items[0]['value'],
                            'timestamp': items[0]['dateTime']
                        }
                    else:
                        print(f"Readings endpoint returned no items")
                else:
                    print(f"Readings endpoint error: {r.status_code}")

        print(f"No suitable measure found for {station_id} {measurement_type}")
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

def fetch_ourcs_flag(reach):
    '''Fetch OURCS flag status for a given reach (godstow or isis)'''
    try:
        url = f"https://ourcs.co.uk/api/flags/status/{reach}/"
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            print(f"Error fetching OURCS {reach} flag: {response.status_code}")
            return None

        data = response.json()
        print(f"OURCS {reach.title()} flag: {data.get('status_text', 'Unknown')}")
        return data
    except Exception as e:
        print(f"Error fetching OURCS {reach} flag: {e}")
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
            'hourly': 'temperature_2m,precipitation_probability,precipitation,weather_code',
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
        precip = hourly.get('precipitation', [])[:24]
        weather_codes = hourly.get('weather_code', [])[:24]

        for i in range(min(24, len(times))):
            forecast.append({
                'time': times[i],
                'temperature': temps[i] if i < len(temps) else None,
                'precipitation_probability': precip_prob[i] if i < len(precip_prob) else None,
                'precipitation': precip[i] if i < len(precip) else None,
                'weather_code': weather_codes[i] if i < len(weather_codes) else None
            })

        return forecast

    except Exception as e:
        print(f"Error fetching weather forecast: {e}")
        return None

def main():
    print("Fetching river data...")

    # Load previous data to calculate flow trend AND as fallback
    previous_data = None
    previous_flow = None
    try:
        if os.path.exists('data/current.json'):
            with open('data/current.json', 'r') as f:
                previous_data = json.load(f)
                if previous_data.get('differential') is not None:
                    previous_flow = previous_data['differential'] - 1.63
                    print(f"Previous flow: {previous_flow}m")
                elif previous_data.get('flow') is not None:
                    previous_flow = previous_data['flow']
                    print(f"Previous flow (from flow field): {previous_flow}m")
    except Exception as e:
        print(f"Could not load previous data: {e}")

    # Note: On the Thames, Godstow is upstream of Osney
    # We want downstage from Godstow and stage from Osney
    print("\n=== Fetching current data ===")
    godstow = fetch_lock_level('1302TH', 'downstage')  # Godstow downstream side
    osney = fetch_lock_level('1303TH', 'stage')  # Osney general level

    # Use previous data as fallback if current fetch fails
    if not godstow and previous_data and previous_data.get('godstow_lock', {}).get('level') is not None:
        print("WARNING: Could not fetch Godstow data, using previous reading")
        godstow = {
            'value': previous_data['godstow_lock']['level'],
            'timestamp': previous_data['godstow_lock']['timestamp']
        }

    if not osney and previous_data and previous_data.get('osney_lock', {}).get('level') is not None:
        print("WARNING: Could not fetch Osney data, using previous reading")
        osney = {
            'value': previous_data['osney_lock']['level'],
            'timestamp': previous_data['osney_lock']['timestamp']
        }

    rainfall_24h = fetch_rainfall(24)
    rainfall_7d = fetch_rainfall(168)  # 7 days = 168 hours
    weather_forecast = fetch_weather_forecast()
    ourcs_godstow = fetch_ourcs_flag('godstow')
    ourcs_isis = fetch_ourcs_flag('isis')

    # Update 14-day history for each measure
    print("\n=== Updating 14-day history ===")
    godstow_history = update_history(
        MEASURE_IDS['godstow'],
        previous_data.get('godstow_history', []) if previous_data else []
    )
    osney_history = update_history(
        MEASURE_IDS['osney'],
        previous_data.get('osney_history', []) if previous_data else []
    )
    farmoor_history = update_history(
        MEASURE_IDS['farmoor'],
        previous_data.get('farmoor_history', []) if previous_data else []
    )

    # Get current Farmoor flow from history (most recent reading)
    farmoor_current = None
    if farmoor_history:
        farmoor_current = {
            'value': farmoor_history[0]['value'],
            'timestamp': farmoor_history[0]['timestamp']
        }
        print(f"Farmoor flow: {farmoor_current['value']} m³/s")

    differential = None
    current_flow = None
    flow_trend = None
    flow_2h_ago = None

    if osney and godstow:
        differential = godstow['value'] - osney['value']  # upstream - downstream
        current_flow = differential - 1.63

        # Calculate trend by comparing to flow from ~2 hours ago
        # Use history data to find reading closest to 2 hours ago
        two_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat().replace('+00:00', 'Z')

        # Create map of osney values by timestamp for flow calculation
        osney_map = {r['timestamp']: r['value'] for r in osney_history}

        # Find godstow reading closest to 2 hours ago and calculate its flow
        for reading in godstow_history:
            if reading['timestamp'] <= two_hours_ago:
                osney_val = osney_map.get(reading['timestamp'])
                if osney_val is not None:
                    flow_2h_ago = (reading['value'] - osney_val) - 1.63
                    print(f"Flow 2h ago ({reading['timestamp']}): {flow_2h_ago:.3f}m")
                    break

        # Calculate trend with 0.1 threshold
        if flow_2h_ago is not None:
            flow_change = current_flow - flow_2h_ago
            if flow_change > 0.1:
                flow_trend = 'increasing'
            elif flow_change < -0.1:
                flow_trend = 'decreasing'
            else:
                flow_trend = 'level'
            print(f"Flow change over 2h: {flow_change:+.3f}m -> {flow_trend}")
        else:
            flow_trend = 'level'
            print("No 2h-ago data available for trend calculation")
    else:
        print("ERROR: Unable to calculate flow - missing lock data even after fallback attempt")

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
        'farmoor': {
            'flow': farmoor_current['value'] if farmoor_current else None,
            'timestamp': farmoor_current['timestamp'] if farmoor_current else None
        },
        'differential': differential,
        'flow': current_flow,
        'flow_2h_ago': flow_2h_ago,
        'flow_trend': flow_trend,
        'rainfall_24h': rainfall_24h if rainfall_24h is not None else 0,
        'rainfall_7d': rainfall_7d if rainfall_7d is not None else 0,
        'weather_forecast': weather_forecast if weather_forecast else [],
        'ourcs_godstow_flag': ourcs_godstow,
        'ourcs_isis_flag': ourcs_isis,
        'godstow_history': godstow_history,
        'osney_history': osney_history,
        'farmoor_history': farmoor_history
    }

    os.makedirs('data', exist_ok=True)
    with open('data/current.json', 'w') as f:
        json.dump(data, f, indent=2)

    print("\n=== Data saved! ===")
    print(f"Osney: {osney['value'] if osney else 'N/A'}m")
    print(f"Godstow: {godstow['value'] if godstow else 'N/A'}m")
    print(f"Farmoor: {farmoor_current['value'] if farmoor_current else 'N/A'} m³/s")
    print(f"Differential: {differential if differential else 'N/A'}m")
    print(f"Flow: {current_flow if current_flow is not None else 'N/A'}m")
    print(f"Flow trend: {flow_trend if flow_trend else 'N/A'}")
    print(f"Rainfall 24h: {rainfall_24h if rainfall_24h else 'N/A'}mm")
    print(f"Rainfall 7d: {rainfall_7d if rainfall_7d else 'N/A'}mm")
    print(f"Weather forecast: {len(weather_forecast) if weather_forecast else 0} hours")
    print(f"History: Godstow={len(godstow_history)}, Osney={len(osney_history)}, Farmoor={len(farmoor_history)} readings")

if __name__ == '__main__':
    main()