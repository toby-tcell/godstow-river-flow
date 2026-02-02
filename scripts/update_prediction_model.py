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


def _fit_exponential_decay(readings, peaks, min_rise_threshold):
    """
    Fit exponential decay curves to peak events.

    Model: value(t) = baseline + amplitude * exp(-t / tau)
    where tau is the time constant and half_life = tau * ln(2).

    Returns list of fitted decay parameters for peaks with R² > 0.5.
    """
    results = []

    for p_idx, (idx, peak_ts, peak_val) in enumerate(peaks):
        # Estimate baseline from minimum value 7-14 days before peak
        baseline_readings = [val for _, val in readings[max(0, idx - 84):idx - 12]]
        if not baseline_readings:
            baseline_readings = [val for _, val in readings[max(0, idx - 84):idx]]
        if not baseline_readings:
            continue

        baseline = min(baseline_readings)
        amplitude = peak_val - baseline
        if amplitude < min_rise_threshold:
            continue

        # Collect decay points until next peak or flow starts rising
        decay_points = []
        next_peak_ts = peaks[p_idx + 1][1] if p_idx + 1 < len(peaks) else None

        for j in range(idx, min(idx + 180, len(readings))):
            ts, val = readings[j]
            hours = (ts - peak_ts).total_seconds() / 3600

            if next_peak_ts and ts >= next_peak_ts:
                break
            # Stop if flow starts rising significantly (new rain event)
            if hours > 12 and j > idx + 3:
                recent = [readings[k][1] for k in range(j - 3, j + 1)]
                if recent[-1] > recent[0] + min_rise_threshold:
                    break

            decay_points.append((hours, val))

        if len(decay_points) < 6:
            continue

        # Linearize: ln((val - baseline) / amplitude) = -t / tau
        log_points = []
        for hours, val in decay_points:
            ratio = (val - baseline) / amplitude
            if 0.01 < ratio <= 1.0:
                log_points.append((hours, math.log(ratio)))

        if len(log_points) < 4:
            continue

        # Linear regression on log-transformed data
        n = len(log_points)
        sum_x = sum(p[0] for p in log_points)
        sum_y = sum(p[1] for p in log_points)
        sum_xy = sum(p[0] * p[1] for p in log_points)
        sum_xx = sum(p[0] ** 2 for p in log_points)

        denom = n * sum_xx - sum_x ** 2
        if denom == 0:
            continue

        slope = (n * sum_xy - sum_x * sum_y) / denom
        if slope >= 0:  # not decaying
            continue

        tau = -1.0 / slope
        half_life = tau * math.log(2)

        # R² of the fit
        mean_y = sum_y / n
        ss_tot = sum((p[1] - mean_y) ** 2 for p in log_points)
        ss_res = sum((p[1] - (slope * p[0])) ** 2 for p in log_points)
        r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0

        if r_sq > 0.5:
            results.append({
                'peak_time': peak_ts,
                'peak_val': peak_val,
                'baseline': baseline,
                'amplitude': amplitude,
                'tau_hours': tau,
                'half_life_hours': half_life,
                'r_squared': r_sq,
                'fit_points': n,
            })

    return results


def _find_peaks(readings, min_peak, window, min_gap):
    """Find local maxima in a time series, at least min_gap apart."""
    peaks = []
    for i in range(window, len(readings) - window):
        ts, val = readings[i]
        if val < min_peak:
            continue
        window_vals = [readings[j][1] for j in range(i - window, i + window + 1)]
        if val == max(window_vals):
            if not peaks or (ts - peaks[-1][1]) > min_gap:
                peaks.append((i, ts, val))
            elif val > peaks[-1][2]:
                peaks[-1] = (i, ts, val)
    return peaks


def _to_2h_readings(history_list):
    """Convert a history list to 2-hour interval (dt, value) tuples."""
    readings = []
    for r in history_list:
        if r.get('value') is None:
            continue
        try:
            dt = datetime.fromisoformat(r['timestamp'].replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        if dt.hour % 2 == 0 and dt.minute == 0:
            readings.append((dt, r['value']))
    readings.sort(key=lambda x: x[0])
    return readings


def _summarise_decay(fits, default_tau, baseline=0.0):
    """Summarise a list of exponential decay fits into model parameters."""
    if not fits:
        return {
            'tau_hours_mean': default_tau,
            'tau_hours_median': default_tau,
            'half_life_hours_mean': default_tau * math.log(2),
            'half_life_hours_median': default_tau * math.log(2),
            'baseline': baseline,
            'n_peaks': 0,
        }

    taus = [f['tau_hours'] for f in fits]
    halflives = [f['half_life_hours'] for f in fits]
    taus_sorted = sorted(taus)
    hl_sorted = sorted(halflives)

    return {
        'tau_hours_mean': sum(taus) / len(taus),
        'tau_hours_median': taus_sorted[len(taus) // 2],
        'half_life_hours_mean': sum(halflives) / len(halflives),
        'half_life_hours_median': hl_sorted[len(halflives) // 2],
        'baseline': baseline,
        'n_peaks': len(fits),
    }


def calculate_farmoor_decay(data):
    """Fit exponential decay to Farmoor flow peaks.

    Uses the median summer (Jun-Sep) flow as the asymptotic baseline
    since Farmoor never fully dries out.
    """
    readings = _to_2h_readings(data['farmoor_history'])

    # Calculate summer baseline (Jun-Sep median)
    summer_vals = [val for dt, val in readings if dt.month in (6, 7, 8, 9)]
    if summer_vals:
        summer_vals.sort()
        baseline = summer_vals[len(summer_vals) // 2]
    else:
        baseline = 1.0  # fallback

    peaks = _find_peaks(readings, min_peak=30, window=6, min_gap=timedelta(days=3))
    fits = _fit_exponential_decay(readings, peaks, min_rise_threshold=5)
    return _summarise_decay(fits, default_tau=112, baseline=baseline)


def calculate_flow_differential_decay(data):
    """Fit exponential decay to flow differential (godstow - osney - 1.63) peaks.

    Baseline is 0 since the differential can genuinely reach zero.
    """
    godstow = {r['timestamp']: r['value'] for r in data['godstow_history']
                if r.get('value') is not None}
    osney = {r['timestamp']: r['value'] for r in data['osney_history']
              if r.get('value') is not None}

    # Build flow differential series at 2h intervals
    readings = []
    for ts in sorted(godstow.keys()):
        if ts not in osney:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        if dt.hour % 2 == 0 and dt.minute == 0:
            flow = (godstow[ts] - osney[ts]) - 1.63
            readings.append((dt, flow))
    readings.sort(key=lambda x: x[0])

    peaks = _find_peaks(readings, min_peak=0.3, window=6, min_gap=timedelta(days=3))
    fits = _fit_exponential_decay(readings, peaks, min_rise_threshold=0.1)
    return _summarise_decay(fits, default_tau=70, baseline=0.0)


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

    # Calculate exponential decay rates
    print("\n3. Calculating Farmoor flow decay (exponential)...")
    farmoor_decay = calculate_farmoor_decay(historic)
    print(f"   Tau (mean):      {farmoor_decay['tau_hours_mean']:.1f}h ({farmoor_decay['tau_hours_mean']/24:.1f} days)")
    print(f"   Tau (median):    {farmoor_decay['tau_hours_median']:.1f}h ({farmoor_decay['tau_hours_median']/24:.1f} days)")
    print(f"   Half-life (mean):   {farmoor_decay['half_life_hours_mean']:.1f}h ({farmoor_decay['half_life_hours_mean']/24:.1f} days)")
    print(f"   Half-life (median): {farmoor_decay['half_life_hours_median']:.1f}h ({farmoor_decay['half_life_hours_median']/24:.1f} days)")
    print(f"   Baseline:        {farmoor_decay['baseline']:.1f} m³/s (summer median)")
    print(f"   Based on {farmoor_decay['n_peaks']} peaks")

    print("\n4. Calculating flow differential decay (exponential)...")
    diff_decay = calculate_flow_differential_decay(historic)
    print(f"   Tau (mean):      {diff_decay['tau_hours_mean']:.1f}h ({diff_decay['tau_hours_mean']/24:.1f} days)")
    print(f"   Tau (median):    {diff_decay['tau_hours_median']:.1f}h ({diff_decay['tau_hours_median']/24:.1f} days)")
    print(f"   Half-life (mean):   {diff_decay['half_life_hours_mean']:.1f}h ({diff_decay['half_life_hours_mean']/24:.1f} days)")
    print(f"   Half-life (median): {diff_decay['half_life_hours_median']:.1f}h ({diff_decay['half_life_hours_median']/24:.1f} days)")
    print(f"   Baseline:        {diff_decay['baseline']:.1f} m")
    print(f"   Based on {diff_decay['n_peaks']} peaks")

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
        'farmoor_decay': {
            'model': 'exponential',
            'baseline': round(farmoor_decay['baseline'], 1),
            'tau_hours_mean': round(farmoor_decay['tau_hours_mean'], 1),
            'tau_hours_median': round(farmoor_decay['tau_hours_median'], 1),
            'half_life_hours_mean': round(farmoor_decay['half_life_hours_mean'], 1),
            'half_life_hours_median': round(farmoor_decay['half_life_hours_median'], 1),
            'conservative_tau_hours': round(farmoor_decay['tau_hours_median'] * 0.8, 1),
            'n_peaks_analyzed': farmoor_decay['n_peaks']
        },
        'flow_differential_decay': {
            'model': 'exponential',
            'baseline': round(diff_decay['baseline'], 1),
            'tau_hours_mean': round(diff_decay['tau_hours_mean'], 1),
            'tau_hours_median': round(diff_decay['tau_hours_median'], 1),
            'half_life_hours_mean': round(diff_decay['half_life_hours_mean'], 1),
            'half_life_hours_median': round(diff_decay['half_life_hours_median'], 1),
            'conservative_tau_hours': round(diff_decay['tau_hours_median'] * 0.8, 1),
            'n_peaks_analyzed': diff_decay['n_peaks']
        }
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
