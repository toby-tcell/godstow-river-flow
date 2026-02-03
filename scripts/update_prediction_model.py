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
import math
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


def calculate_godstow_drop_rates(data):
    """Calculate average daily godstow level drop rates, binned by level.

    Looks at all days where godstow was falling (dry periods) and computes
    the average drop in mm/day for each level bin. This is the primary driver
    of the flow differential since osney is relatively stable.
    """
    # Build daily noon readings for godstow
    godstow_daily = {}
    for r in data['godstow_history']:
        if r.get('value') is None:
            continue
        ts = r['timestamp']
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        if dt.hour == 12 and dt.minute == 0:
            godstow_daily[dt.date()] = r['value']

    # Build daily noon readings for osney (for average osney level)
    osney_vals = []
    for r in data['osney_history']:
        if r.get('value') is None:
            continue
        ts = r['timestamp']
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        if dt.hour == 12 and dt.minute == 0:
            osney_vals.append(r['value'])

    avg_osney = sum(osney_vals) / len(osney_vals) if osney_vals else -0.04

    # Collect daily drops during falling periods
    # A "falling day" is one where godstow dropped from the previous day
    bins = [
        {'min_level': 2.4, 'max_level': 3.0, 'drops': []},
        {'min_level': 2.0, 'max_level': 2.4, 'drops': []},
        {'min_level': 1.6, 'max_level': 2.0, 'drops': []},
    ]

    days = sorted(godstow_daily.keys())
    for i in range(1, len(days)):
        if (days[i] - days[i - 1]).days != 1:
            continue  # skip gaps
        prev_val = godstow_daily[days[i - 1]]
        curr_val = godstow_daily[days[i]]
        drop_mm = (prev_val - curr_val) * 1000  # positive = falling

        if drop_mm <= 0:
            continue  # only count falling days

        # Bin by the starting level
        for b in bins:
            if b['min_level'] <= prev_val < b['max_level']:
                b['drops'].append(drop_mm)
                break

    result_bins = []
    for b in bins:
        drops = b['drops']
        if drops:
            drops.sort()
            avg = sum(drops) / len(drops)
            median = drops[len(drops) // 2]
        else:
            avg = 0
            median = 0
        result_bins.append({
            'min_level': b['min_level'],
            'max_level': b['max_level'],
            'avg_drop_mm_per_day': round(avg, 1),
            'median_drop_mm_per_day': round(median, 1),
            'n_days': len(drops),
        })

    return {
        'bins': result_bins,
        'avg_osney': round(avg_osney, 3),
    }


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

    # Calculate godstow drop rates
    print("\n3. Calculating godstow daily drop rates...")
    drop_rates = calculate_godstow_drop_rates(historic)
    for b in drop_rates['bins']:
        print(f"   Level {b['min_level']:.1f}-{b['max_level']:.1f}m: "
              f"avg {b['avg_drop_mm_per_day']:.0f} mm/day, "
              f"median {b['median_drop_mm_per_day']:.0f} mm/day "
              f"({b['n_days']} days)")
    print(f"   Average osney level: {drop_rates['avg_osney']:.3f}m")

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
            'green_amber_flow': 0.45,
            'amber_red_flow': 0.75
        },
        'godstow_decay_rates': drop_rates,
    }

    with open(MODEL_FILE, 'w') as f:
        json.dump(model, f, indent=2)

    print(f"\n5. Saved model to {MODEL_FILE}")

    print("\n" + "="*60)
    print("Model Update Complete")
    print("="*60)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()
