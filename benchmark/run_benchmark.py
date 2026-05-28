#!/usr/bin/env python3
"""
Benchmark harness: DuckDB vs DataFusion on many small Parquet files.
Produces JSON + CSV results in results/ directory.
"""
import json
import csv
import os
import sys
import time
import statistics
import platform
import psutil

import duckdb
import datafusion
from datafusion import SessionContext

# ── paths ──
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE, 'data')
RESULTS_DIR = os.path.join(BASE, 'results')

SCENARIOS = [
    {'name': '100_files',   'dir': 'scenario_100f_100r',   'files': 100,   'rows': 100},
    {'name': '500_files',   'dir': 'scenario_500f_1000r',  'files': 500,   'rows': 1000},
    {'name': '1000_files',  'dir': 'scenario_1000f_100r',  'files': 1000,  'rows': 100},
    {'name': '5000_files',  'dir': 'scenario_5000f_50r',   'files': 5000,  'rows': 50},
    {'name': '10000_files', 'dir': 'scenario_10000f_50r',  'files': 10000, 'rows': 50},
]

QUERIES = [
    ('count_star',        'SELECT COUNT(*) FROM t'),
    ('selective_filter',  'SELECT * FROM t WHERE id = 42'),
    ('agg_groupby',       'SELECT category, AVG(value) FROM t GROUP BY category'),
    ('count_distinct',    'SELECT COUNT(DISTINCT name) FROM t'),
]

RUNS = 10
WARMUP_RUNS = 3


# ── DuckDB runner ──

def run_duckdb(scenario_dir: str, query_sql: str) -> list[float]:
    pattern = os.path.join(DATA_DIR, scenario_dir, '*.parquet')
    for _ in range(WARMUP_RUNS):
        con = duckdb.connect()
        con.execute("SET enable_progress_bar = false")
        con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{pattern}')")
        con.execute(query_sql).fetchall()
        con.close()
    timings = []
    for _ in range(RUNS):
        con = duckdb.connect()
        con.execute("SET enable_progress_bar = false")
        con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{pattern}')")
        t0 = time.perf_counter()
        con.execute(query_sql).fetchall()
        timings.append((time.perf_counter() - t0) * 1000)
        con.close()
    return timings


# ── DataFusion runner ──

def run_datafusion(scenario_dir: str, query_sql: str) -> list[float]:
    data_path = os.path.join(DATA_DIR, scenario_dir)
    for _ in range(WARMUP_RUNS):
        ctx = SessionContext()
        ctx.register_parquet('t', data_path,
                              table_partition_cols=[],
                              file_extension='.parquet')
        ctx.sql(query_sql).collect()
    timings = []
    for _ in range(RUNS):
        ctx = SessionContext()
        ctx.register_parquet('t', data_path,
                              table_partition_cols=[],
                              file_extension='.parquet')
        t0 = time.perf_counter()
        result = ctx.sql(query_sql).collect()
        timings.append((time.perf_counter() - t0) * 1000)
    return timings


# ── main ──

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results = []

    # Capture environment info
    env_info = {
        'platform': platform.platform(),
        'python': platform.python_version(),
        'cpu_count': os.cpu_count(),
        'ram_gb': round(psutil.virtual_memory().total / (1024**3), 1),
        'duckdb_version': duckdb.__version__,
        'datafusion_version': datafusion.__version__,
    }
    print(f"Environment: {json.dumps(env_info, indent=2)}")

    for scenario in SCENARIOS:
        sdir = scenario['dir']
        data_path = os.path.join(DATA_DIR, sdir)
        if not os.path.isdir(data_path):
            print(f"[SKIP] {scenario['name']} — data not found. Run generate_data.py first.")
            continue

        actual_files = len([f for f in os.listdir(data_path) if f.endswith('.parquet')])
        print(f"\n{'='*60}")
        print(f"Scenario: {scenario['name']} ({actual_files} files)")

        for qname, qsql in QUERIES:
            print(f"  Query: {qname} ...", end=' ', flush=True)

            try:
                duck_times = run_duckdb(sdir, qsql)
            except Exception as e:
                print(f"[DuckDB ERROR: {e}]")
                duck_times = []

            try:
                df_times = run_datafusion(sdir, qsql)
            except Exception as e:
                print(f"[DataFusion ERROR: {e}]")
                df_times = []

            duck_median = statistics.median(duck_times) if duck_times else None
            df_median = statistics.median(df_times) if df_times else None

            if duck_median and df_median:
                ratio = df_median / duck_median
                winner = 'DuckDB' if ratio > 1 else 'DataFusion'
                print(f"DuckDB={duck_median:.0f}ms  DataFusion={df_median:.0f}ms  ({winner} {max(ratio, 1/ratio):.2f}x)")
            else:
                print("partial results")

            results.append({
                'scenario': scenario['name'],
                'files': actual_files,
                'rows_per_file': scenario['rows'],
                'query': qname,
                'query_sql': qsql,
                'duckdb_runs_ms': duck_times,
                'duckdb_median_ms': duck_median,
                'datafusion_runs_ms': df_times,
                'datafusion_median_ms': df_median,
            })

    # ── write results ──
    output = {'environment': env_info, 'results': results}

    json_path = os.path.join(RESULTS_DIR, 'results.json')
    with open(json_path, 'w') as f:
        json.dump(output, f, indent=2)
    print(f"\nJSON results: {json_path}")

    csv_path = os.path.join(RESULTS_DIR, 'results.csv')
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['scenario', 'files', 'rows_per_file', 'query',
                     'duckdb_median_ms', 'datafusion_median_ms', 'winner'])
        for r in results:
            duck_m = r['duckdb_median_ms']
            df_m = r['datafusion_median_ms']
            if duck_m and df_m:
                winner = 'DuckDB' if duck_m < df_m else 'DataFusion'
            elif duck_m:
                winner = 'DuckDB'
            elif df_m:
                winner = 'DataFusion'
            else:
                winner = 'N/A'
            w.writerow([r['scenario'], r['files'], r['rows_per_file'], r['query'],
                         f"{duck_m:.1f}" if duck_m else '',
                         f"{df_m:.1f}" if df_m else '', winner])
    print(f"CSV results:  {csv_path}")

    # ── print summary table ──
    print(f"\n{'='*80}")
    print(f"{'Scenario':<15} {'Query':<20} {'DuckDB(ms)':<12} {'DataFusion(ms)':<15} {'Winner':<12}")
    print(f"{'-'*80}")
    for r in results:
        duck_s = f"{r['duckdb_median_ms']:.0f}" if r['duckdb_median_ms'] else 'ERR'
        df_s = f"{r['datafusion_median_ms']:.0f}" if r['datafusion_median_ms'] else 'ERR'
        if r['duckdb_median_ms'] and r['datafusion_median_ms']:
            w = 'DuckDB' if r['duckdb_median_ms'] < r['datafusion_median_ms'] else 'DataFusion'
        else:
            w = 'N/A'
        print(f"{r['scenario']:<15} {r['query']:<20} {duck_s:<12} {df_s:<15} {w:<12}")


if __name__ == '__main__':
    main()
