#!/usr/bin/env python3
"""
Update the prediction model by:
1. Fetching the latest historic data from EA archive (godstow + osney)
2. Calculating differential decay rates binned by differential level
3. Outputting a JSON model file for the website
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
            readings = {'godstow': [], 'osney': []}

            reader = csv.DictReader(StringIO(response.text))
            for row in reader:
                measure = row.get('measure', '')
                timestamp = row.get('dateTime', '')

                # Only keep readings at 2-hour intervals (00:00, 02:00, 04:00, etc.)
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
            }
        except:
            pass
    return {'godstow': {}, 'osney': {}}


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
                for key in ['godstow', 'osney']:
                    for r in readings[key]:
                        all_readings[key][r['timestamp']] = r['value']

    # Trim to max age
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat().replace('+00:00', 'Z')

    result = {}
    for key in ['godstow', 'osney']:
        result[key] = [
            {'timestamp': ts, 'value': val}
            for ts, val in sorted(all_readings[key].items())
            if ts >= cutoff
        ]

    print(f"  After update: {len(result['godstow']):,} Godstow, {len(result['osney']):,} Osney readings")
    return result


def calculate_differential_decay_rate(data):
    """Calculate a single constant daily differential drop rate using 2-hourly data.

    For each 2-hourly timestamp where both godstow and osney exist, compute
    the differential. Then find pairs exactly 24h apart and compute the daily
    drop. Only includes days where the differential actually fell (drop > 0).
    Only considers readings above the green threshold (0.45).
    Returns a single median drop rate.
    """
    # Build differential at each timestamp
    godstow = {r['timestamp']: r['value'] for r in data['godstow_history']}
    osney = {r['timestamp']: r['value'] for r in data['osney_history']}

    differentials = {}
    for ts in godstow:
        if ts in osney:
            differentials[ts] = (godstow[ts] - osney[ts]) - 1.63

    print(f"   {len(differentials):,} paired differential readings")

    # Build a lookup by datetime for finding 24h-apart pairs
    dt_lookup = {}
    for ts, diff in differentials.items():
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            dt_lookup[dt] = diff
        except (ValueError, AttributeError):
            continue

    # For each reading above green threshold, find the reading exactly 24h later
    GREEN_THRESHOLD = 0.45
    drops = []
    for dt, diff_start in dt_lookup.items():
        if diff_start < GREEN_THRESHOLD:
            continue  # below green — no-one cares about decay here
        dt_next = dt + timedelta(hours=24)
        if dt_next in dt_lookup:
            diff_end = dt_lookup[dt_next]
            drop = diff_start - diff_end  # positive = falling

            # Only count days where differential actually fell
            if drop > 0:
                drops.append(drop * 1000)  # convert to mm

    print(f"   {len(drops):,} valid 24h pairs (drop > 0)")

    drops.sort()
    avg = sum(drops) / len(drops) if drops else 0
    median = drops[len(drops) // 2] if drops else 0

    return {
        'avg_drop_mm_per_day': round(avg, 1),
        'median_drop_mm_per_day': round(median, 1),
        'n_pairs': len(drops),
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
        [r['timestamp'] for r in raw_data['osney']]
    )

    historic = {
        'metadata': {
            'created': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'earliest_reading': min(all_timestamps) if all_timestamps else None,
            'latest_reading': max(all_timestamps) if all_timestamps else None,
            'reading_counts': {
                'godstow': len(raw_data['godstow']),
                'osney': len(raw_data['osney']),
            }
        },
        'godstow_history': raw_data['godstow'],
        'osney_history': raw_data['osney'],
    }

    with open(HISTORIC_FILE, 'w') as f:
        json.dump(historic, f, indent=2)

    print(f"   Saved {HISTORIC_FILE}")
    print(f"   Godstow: {len(raw_data['godstow']):,} readings")
    print(f"   Osney: {len(raw_data['osney']):,} readings")

    # Calculate differential decay rate
    print("\n2. Calculating differential decay rate...")
    decay_rate = calculate_differential_decay_rate(historic)
    print(f"   Avg: {decay_rate['avg_drop_mm_per_day']:.0f} mm/day, "
          f"Median: {decay_rate['median_drop_mm_per_day']:.0f} mm/day "
          f"({decay_rate['n_pairs']} pairs)")

    # Build model JSON
    model = {
        'updated': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'data_range': {
            'start': historic['metadata']['earliest_reading'],
            'end': historic['metadata']['latest_reading'],
            'samples': decay_rate['n_pairs']
        },
        'thresholds': {
            'green_amber_flow': 0.45,
            'amber_red_flow': 0.75
        },
        'differential_decay_rate': decay_rate,
    }

    with open(MODEL_FILE, 'w') as f:
        json.dump(model, f, indent=2)

    print(f"\n3. Saved model to {MODEL_FILE}")

    print("\n" + "="*60)
    print("Model Update Complete")
    print("="*60)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()
