#!/usr/bin/env python3
"""
Update the prediction model by:
1. Fetching the latest historic data from EA archive
2. Calculating correlation and regression parameters
3. Analyzing decay rates
4. Outputting a JSON model file for the website
"""

import requests
import json
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Measure IDs
MEASURE_URLS = {
    'godstow': 'http://environment.data.gov.uk/flood-monitoring/id/measures/1302TH-level-downstage-i-15_min-mASD',
    'osney': 'http://environment.data.gov.uk/flood-monitoring/id/measures/1303TH-level-stage-i-15_min-mASD',
    'farmoor': 'http://environment.data.gov.uk/flood-monitoring/id/measures/1100TH-flow--Mean-15_min-m3_s'
}

ARCHIVE_URL = "https://environment.data.gov.uk/flood-monitoring/archive/readings-{date}.csv"
HISTORIC_FILE = 'data/historic.json'
MODEL_FILE = 'data/prediction_model.json'


def fetch_archive_day(date_str):
    """Fetch a single day's archive CSV and extract our measures (2-hour resolution)."""
    url = ARCHIVE_URL.format(date=date_str)

    try:
        response = requests.get(url, timeout=120)
        if response.status_code == 200:
            readings = {'godstow': [], 'osney': [], 'farmoor': []}

            reader = csv.DictReader(StringIO(response.text))
            for row in reader:
                measure = row.get('measure', '')
                timestamp = row.get('dateTime', '')

                # Only keep readings at 2-hour intervals (00:00, 02:00, 04:00, etc.)
                # Check if hour is even and minutes are 00
                if 'T' in timestamp:
                    time_part = timestamp.split('T')[1][:5]  # HH:MM
                    hour = int(time_part[:2])
                    minute = int(time_part[3:5])
                    if hour % 2 != 0 or minute != 0:
                        continue

                for key, measure_url in MEASURE_URLS.items():
                    if measure == measure_url:
                        try:
                            val_str = row.get('value', '')
                            if '|' in val_str:
                                continue
                            readings[key].append({
                                'timestamp': timestamp,
                                'value': float(val_str)
                            })
                        except (ValueError, KeyError):
                            pass

            return readings
        return None
    except Exception as e:
        print(f"    Error fetching {date_str}: {e}")
        return None


def load_existing_data():
    """Load existing historic data if available."""
    if os.path.exists(HISTORIC_FILE):
        try:
            with open(HISTORIC_FILE, 'r') as f:
                data = json.load(f)
            return {
                'godstow': {r['timestamp']: r['value'] for r in data.get('godstow_history', [])},
                'osney': {r['timestamp']: r['value'] for r in data.get('osney_history', [])},
                'farmoor': {r['timestamp']: r['value'] for r in data.get('farmoor_history', [])}
            }
        except:
            pass
    return {'godstow': {}, 'osney': {}, 'farmoor': {}}


def fetch_historic_data(days_to_fetch=14, max_age_days=365):
    """
    Incremental update: load existing data, fetch recent days, trim to max age.
    """
    # Load existing data
    print(f"  Loading existing data from {HISTORIC_FILE}...")
    all_readings = load_existing_data()
    existing_count = len(all_readings['godstow'])
    print(f"  Found {existing_count:,} existing Godstow readings")

    # Fetch recent days
    end_date = datetime.now(timezone.utc) - timedelta(days=1)
    start_date = end_date - timedelta(days=days_to_fetch)

    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    print(f"  Fetching last {len(dates)} days...")

    # Fetch in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_archive_day, date): date for date in dates}

        for future in as_completed(futures):
            readings = future.result()
            if readings:
                for key in ['godstow', 'osney', 'farmoor']:
                    for r in readings[key]:
                        all_readings[key][r['timestamp']] = r['value']

    # Trim to max age
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat().replace('+00:00', 'Z')

    result = {}
    for key in ['godstow', 'osney', 'farmoor']:
        result[key] = [
            {'timestamp': ts, 'value': val}
            for ts, val in sorted(all_readings[key].items())
            if ts >= cutoff
        ]

    print(f"  After update: {len(result['godstow']):,} Godstow readings")
    return result


def calculate_model_parameters(data):
    """Calculate regression parameters and thresholds."""

    # Build lookup dicts
    godstow = {r['timestamp']: r['value'] for r in data['godstow_history']}
    osney = {r['timestamp']: r['value'] for r in data['osney_history']}
    farmoor = {r['timestamp']: r['value'] for r in data['farmoor_history']}

    # Calculate flow values
    flows = {}
    for ts in godstow:
        if ts in osney:
            flows[ts] = (godstow[ts] - osney[ts]) - 1.63

    # Pair with farmoor
    paired_data = []
    for ts in flows:
        if ts in farmoor:
            paired_data.append((farmoor[ts], flows[ts]))

    if len(paired_data) < 100:
        raise ValueError(f"Not enough paired data: {len(paired_data)}")

    farm_vals = [p[0] for p in paired_data]
    flow_vals = [p[1] for p in paired_data]

    # Linear regression
    n = len(paired_data)
    mean_farm = sum(farm_vals) / n
    mean_flow = sum(flow_vals) / n

    numerator = sum((f - mean_farm) * (fl - mean_flow) for f, fl in paired_data)
    denominator = sum((f - mean_farm)**2 for f, fl in paired_data)

    slope = numerator / denominator
    intercept = mean_flow - slope * mean_farm

    # R²
    predicted = [slope * f + intercept for f in farm_vals]
    ss_res = sum((actual - pred)**2 for actual, pred in zip(flow_vals, predicted))
    ss_tot = sum((actual - mean_flow)**2 for actual in flow_vals)
    r_squared = 1 - (ss_res / ss_tot)

    # Correlation
    var_farm = sum((f - mean_farm)**2 for f in farm_vals) / n
    var_flow = sum((f - mean_flow)**2 for f in flow_vals) / n
    cov = sum((f - mean_farm) * (fl - mean_flow) for f, fl in paired_data) / n
    correlation = cov / ((var_farm * var_flow) ** 0.5)

    # Thresholds
    farmoor_green_amber = (0.45 - intercept) / slope
    farmoor_amber_red = (0.75 - intercept) / slope

    return {
        'slope': slope,
        'intercept': intercept,
        'r_squared': r_squared,
        'correlation': correlation,
        'n_samples': n,
        'thresholds': {
            'green_amber': farmoor_green_amber,
            'amber_red': farmoor_amber_red
        }
    }


def calculate_decay_rate(data):
    """Calculate average decay rate from peak analysis."""

    farmoor = [(r['timestamp'], r['value']) for r in data['farmoor_history']]
    farmoor.sort(key=lambda x: x[0])

    # Convert to datetime
    readings = []
    for ts, val in farmoor:
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            readings.append((dt, val))
        except:
            pass

    # Find peaks > 30 m³/s
    MIN_PEAK = 30
    WINDOW = 48  # 12 hours

    peaks = []
    for i in range(WINDOW, len(readings) - WINDOW):
        ts, val = readings[i]
        if val < MIN_PEAK:
            continue

        is_peak = all(readings[j][1] <= val for j in range(i-WINDOW, i+WINDOW))
        if is_peak:
            peaks.append((i, ts, val))

    # Filter peaks within 2 days
    filtered_peaks = []
    for i, (idx, ts, val) in enumerate(peaks):
        if not filtered_peaks:
            filtered_peaks.append((idx, ts, val))
        elif (ts - filtered_peaks[-1][1]).days >= 2:
            filtered_peaks.append((idx, ts, val))
        elif val > filtered_peaks[-1][2]:
            filtered_peaks[-1] = (idx, ts, val)

    # Calculate decay rates
    decay_rates = []
    for idx, peak_ts, peak_val in filtered_peaks:
        for i in range(idx, min(idx + 100, len(readings))):
            ts, val = readings[i]
            hours_after = (ts - peak_ts).total_seconds() / 3600

            if 23 <= hours_after <= 25:
                decay = peak_val - val
                if decay > 0:
                    decay_rates.append(decay)
                break

    if decay_rates:
        return {
            'mean': sum(decay_rates) / len(decay_rates),
            'median': sorted(decay_rates)[len(decay_rates)//2],
            'min': min(decay_rates),
            'max': max(decay_rates),
            'n_peaks': len(decay_rates)
        }

    return {'mean': 3.5, 'median': 3.5, 'min': 3.5, 'max': 3.5, 'n_peaks': 0}


def main():
    print("="*60)
    print("Updating Prediction Model")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch historic data
    print("\n1. Fetching historic data from EA archive...")
    raw_data = fetch_historic_data(days_to_fetch=14, max_age_days=365)

    # Save historic data
    os.makedirs('data', exist_ok=True)

    all_timestamps = (
        [r['timestamp'] for r in raw_data['godstow']] +
        [r['timestamp'] for r in raw_data['osney']] +
        [r['timestamp'] for r in raw_data['farmoor']]
    )

    historic = {
        'metadata': {
            'created': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'earliest_reading': min(all_timestamps) if all_timestamps else None,
            'latest_reading': max(all_timestamps) if all_timestamps else None,
            'reading_counts': {
                'godstow': len(raw_data['godstow']),
                'osney': len(raw_data['osney']),
                'farmoor': len(raw_data['farmoor'])
            }
        },
        'godstow_history': raw_data['godstow'],
        'osney_history': raw_data['osney'],
        'farmoor_history': raw_data['farmoor']
    }

    with open(HISTORIC_FILE, 'w') as f:
        json.dump(historic, f, indent=2)

    print(f"   Saved {HISTORIC_FILE}")
    print(f"   Godstow: {len(raw_data['godstow']):,} readings")
    print(f"   Osney: {len(raw_data['osney']):,} readings")
    print(f"   Farmoor: {len(raw_data['farmoor']):,} readings")

    # Calculate model parameters
    print("\n2. Calculating model parameters...")
    params = calculate_model_parameters(historic)
    print(f"   Slope: {params['slope']:.5f}")
    print(f"   Intercept: {params['intercept']:.4f}")
    print(f"   R²: {params['r_squared']:.4f}")
    print(f"   Correlation: {params['correlation']:.4f}")
    print(f"   Green/Amber threshold: {params['thresholds']['green_amber']:.1f} m³/s")
    print(f"   Amber/Red threshold: {params['thresholds']['amber_red']:.1f} m³/s")

    # Calculate decay rate
    print("\n3. Calculating decay rates...")
    decay = calculate_decay_rate(historic)
    print(f"   Mean decay: {decay['mean']:.1f} m³/s per day")
    print(f"   Median decay: {decay['median']:.1f} m³/s per day")
    print(f"   Based on {decay['n_peaks']} peaks")

    # Build model JSON
    model = {
        'updated': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'data_range': {
            'start': historic['metadata']['earliest_reading'],
            'end': historic['metadata']['latest_reading'],
            'samples': params['n_samples']
        },
        'regression': {
            'slope': round(params['slope'], 6),
            'intercept': round(params['intercept'], 4),
            'r_squared': round(params['r_squared'], 4),
            'correlation': round(params['correlation'], 4)
        },
        'thresholds': {
            'green_amber_farmoor': round(params['thresholds']['green_amber'], 1),
            'amber_red_farmoor': round(params['thresholds']['amber_red'], 1),
            'green_amber_flow': 0.45,
            'amber_red_flow': 0.75
        },
        'decay_rate': {
            'mean': round(decay['mean'], 1),
            'median': round(decay['median'], 1),
            'conservative': round(decay['median'] * 0.8, 1),  # 80% of median for safety
            'n_peaks_analyzed': decay['n_peaks']
        }
    }

    with open(MODEL_FILE, 'w') as f:
        json.dump(model, f, indent=2)

    print(f"\n4. Saved model to {MODEL_FILE}")

    print("\n" + "="*60)
    print("Model Update Complete")
    print("="*60)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()
