import requests
import json
from datetime import datetime, timedelta, timezone
import os
import statistics

# Hardcoded measure IDs (these don't change)
MEASURE_IDS = {
    'godstow': '1302TH-level-downstage-i-15_min-mASD',
    'osney': '1303TH-level-stage-i-15_min-mASD',
    'farmoor': '1100TH-flow--Mean-15_min-m3_s'
}

# Rainfall measure IDs (all catchment stations including local Oxford gauge)
RAINFALL_STATIONS = [
    '256230TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Oxford (1km)
    '254336TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Farmoor/Eynsham
    '253861TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Witney
    '254829TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Chipping Norton
    '251530TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Lechlade
    '248332TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Cricklade
    '248965TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Cirencester
    '251556TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Northleach
    '249744TP-rainfall-tipping_bucket_raingauge-t-15_min-mm',  # Swindon
]

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

def _fetch_rainfall_total(measure_id, hours):
    '''Fetch total rainfall from a single measure for specified hours'''
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace('+00:00', 'Z')
        readings_url = f"https://environment.data.gov.uk/flood-monitoring/id/measures/{measure_id}/readings.json"
        params = {'since': since, '_limit': 5000}
        r = requests.get(readings_url, params=params, timeout=30)
        if r.status_code == 200:
            items = r.json().get('items', [])
            if items:
                return sum(item['value'] for item in items)
        return None
    except Exception as e:
        print(f"Error fetching rainfall for {measure_id}: {e}")
        return None


def fetch_avg_rainfall(hours=24):
    '''Fetch average rainfall across all catchment stations'''
    totals = []
    for measure_id in RAINFALL_STATIONS:
        total = _fetch_rainfall_total(measure_id, hours)
        if total is not None:
            totals.append(total)
    if totals:
        return sum(totals) / len(totals)
    return 0

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

def calculate_percentile(values, percentile):
    '''Calculate percentile from a list of values'''
    if not values:
        return None
    sorted_values = sorted(values)
    index = (percentile / 100.0) * (len(sorted_values) - 1)
    lower = int(index)
    upper = lower + 1
    weight = index - lower

    if upper >= len(sorted_values):
        return sorted_values[-1]
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

def fetch_weather_forecast():
    '''Fetch 24-hour ensemble weather forecast from Open-Meteo API for Oxford'''
    try:
        # Oxford coordinates
        lat, lon = 51.7520, -1.2577
        url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': 'temperature_2m,precipitation,weather_code',
            'models': 'icon_seamless',  # Best model for UK/European weather
            'forecast_hours': 24,
            'timezone': 'Europe/London'
        }

        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"Ensemble API error: {response.status_code}")
            return None

        data = response.json()

        # Ensemble API returns data differently - need to handle multiple models/members
        # The response structure has hourly data with arrays for each variable
        hourly = data.get('hourly', {})
        times = hourly.get('time', [])

        # For ensemble data, each variable may have multiple values per timestamp
        # We need to calculate statistics across ensemble members
        forecast = []

        # Get the shape of the data to understand ensemble structure
        temp_data = hourly.get('temperature_2m', [])
        precip_data = hourly.get('precipitation', [])
        weather_data = hourly.get('weather_code', [])

        # Process first 24 hours
        for i in range(min(24, len(times))):
            # Extract values for this hour across all ensemble members
            # Temperature: use mean
            temp_val = temp_data[i] if i < len(temp_data) and temp_data[i] is not None else None

            # Precipitation: calculate mean and range
            precip_val = precip_data[i] if i < len(precip_data) and precip_data[i] is not None else None

            # Weather code: use mode or first value
            weather_val = weather_data[i] if i < len(weather_data) and weather_data[i] is not None else None

            forecast.append({
                'time': times[i],
                'temperature': temp_val,
                'precipitation': precip_val,
                'precipitation_probability': None,  # Not available in ensemble API
                'weather_code': weather_val
            })

        return forecast

    except Exception as e:
        print(f"Error fetching ensemble weather forecast: {e}")
        import traceback
        traceback.print_exc()
        return None

def fetch_ensemble_rainfall_data():
    '''Fetch ensemble rainfall data: statistics (mean/percentiles) and 3-day daily breakdown'''
    try:
        lat, lon = 51.7520, -1.2577
        url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        params = {
            'latitude': lat,
            'longitude': lon,
            'hourly': 'precipitation',
            'models': 'icon_seamless',  # Best model for UK/European weather - 40 members
            'forecast_hours': 72,
            'timezone': 'Europe/London'
        }

        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"Ensemble rainfall API error: {response.status_code}")
            return None, None

        data = response.json()
        hourly = data.get('hourly', {})
        times = hourly.get('time', [])

        if not hourly or not times:
            print("No ensemble hourly data")
            return None, None

        # Find all ensemble member keys
        member_keys = [key for key in hourly.keys() if key.startswith('precipitation_member')]
        print(f"Found {len(member_keys)} ensemble members")

        if not member_keys:
            print("No ensemble members found in response")
            return None, None

        # Calculate totals for each member (for statistics)
        totals_24h = []
        totals_72h = []

        for member_key in member_keys:
            member_data = hourly[member_key]
            total_24h = sum(member_data[:24]) if len(member_data) >= 24 else sum(member_data)
            totals_24h.append(total_24h)
            total_72h = sum(member_data[:72]) if len(member_data) >= 72 else sum(member_data)
            totals_72h.append(total_72h)

        # Calculate statistics across all members
        mean_24h = statistics.mean(totals_24h)
        p10_24h = calculate_percentile(totals_24h, 10)
        p90_24h = calculate_percentile(totals_24h, 90)

        mean_72h = statistics.mean(totals_72h)
        p10_72h = calculate_percentile(totals_72h, 10)
        p90_72h = calculate_percentile(totals_72h, 90)

        stats = {
            'rainfall_24h_mean': mean_24h,
            'rainfall_24h_p10': p10_24h,
            'rainfall_24h_p90': p90_24h,
            'rainfall_72h_mean': mean_72h,
            'rainfall_72h_p10': p10_72h,
            'rainfall_72h_p90': p90_72h
        }

        print(f"Ensemble 24h: {mean_24h:.1f}mm (range: {p10_24h:.1f}-{p90_24h:.1f}mm)")
        print(f"Ensemble 72h: {mean_72h:.1f}mm (range: {p10_72h:.1f}-{p90_72h:.1f}mm)")

        # Calculate daily breakdown from ensemble mean
        daily_totals = {}
        for i, time_str in enumerate(times):
            if i >= 72:
                break
            date = time_str.split('T')[0]
            if date not in daily_totals:
                daily_totals[date] = []
            # Average across all ensemble members for this hour
            hour_values = [hourly[key][i] for key in member_keys if i < len(hourly[key]) and hourly[key][i] is not None]
            daily_totals[date].append(statistics.mean(hour_values) if hour_values else 0)

        forecast = []
        for date in sorted(daily_totals.keys())[:3]:
            daily_sum = sum(daily_totals[date])
            forecast.append({'date': date, 'precipitation': daily_sum})

        print(f"3-day forecast: {len(forecast)} days")
        for day in forecast:
            print(f"  {day['date']}: {day['precipitation']:.1f}mm")

        return stats, forecast

    except Exception as e:
        print(f"Error fetching ensemble rainfall data: {e}")
        import traceback
        traceback.print_exc()
        return None, None

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

    avg_rainfall_24h = fetch_avg_rainfall(24)
    avg_rainfall_7d = fetch_avg_rainfall(168)  # 7 days = 168 hours
    weather_forecast = fetch_weather_forecast()
    ensemble_stats, rainfall_forecast_3d = fetch_ensemble_rainfall_data()
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
                flow_trend = 'Rising'
            elif flow_change < -0.1:
                flow_trend = 'Falling'
            else:
                flow_trend = 'Stable'
            print(f"Flow change over 2h: {flow_change:+.3f}m -> {flow_trend}")
        else:
            flow_trend = 'Stable'
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
        'avg_rainfall_24h': avg_rainfall_24h if avg_rainfall_24h is not None else 0,
        'avg_rainfall_7d': avg_rainfall_7d if avg_rainfall_7d is not None else 0,
        'weather_forecast': weather_forecast if weather_forecast else [],
        'rainfall_forecast_3d': rainfall_forecast_3d if rainfall_forecast_3d else [],
        'ensemble_rainfall_24h_mean': ensemble_stats.get('rainfall_24h_mean') if ensemble_stats else None,
        'ensemble_rainfall_24h_p10': ensemble_stats.get('rainfall_24h_p10') if ensemble_stats else None,
        'ensemble_rainfall_24h_p90': ensemble_stats.get('rainfall_24h_p90') if ensemble_stats else None,
        'ensemble_rainfall_72h_mean': ensemble_stats.get('rainfall_72h_mean') if ensemble_stats else None,
        'ensemble_rainfall_72h_p10': ensemble_stats.get('rainfall_72h_p10') if ensemble_stats else None,
        'ensemble_rainfall_72h_p90': ensemble_stats.get('rainfall_72h_p90') if ensemble_stats else None,
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
    print(f"Avg rainfall 24h: {avg_rainfall_24h if avg_rainfall_24h else 'N/A'}mm")
    print(f"Avg rainfall 7d: {avg_rainfall_7d if avg_rainfall_7d else 'N/A'}mm")
    print(f"Weather forecast: {len(weather_forecast) if weather_forecast else 0} hours")
    print(f"History: Godstow={len(godstow_history)}, Osney={len(osney_history)}, Farmoor={len(farmoor_history)} readings")

if __name__ == '__main__':
    main()