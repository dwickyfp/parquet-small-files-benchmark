"""
Generate many small Parquet files for benchmarking.
"""
import os
import sys
import random
import time
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
ROWS_PER_FILE = 100  # default; overridden by scenario
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
CATEGORIES = ['A', 'B', 'C', 'D', 'E']
NAMES = [f'name_{i}' for i in range(200)]


def generate_files(num_files: int, rows_per_file: int, out_dir: str):
    random.seed(SEED)
    os.makedirs(out_dir, exist_ok=True)

    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.utf8()),
        ('value', pa.float64()),
        ('category', pa.utf8()),
        ('ts', pa.timestamp('us')),
    ])

    t0 = time.time()
    for i in range(num_files):
        ids = [random.randint(0, 10_000_000) for _ in range(rows_per_file)]
        names = [random.choice(NAMES) for _ in range(rows_per_file)]
        values = [random.uniform(0, 1000) for _ in range(rows_per_file)]
        cats = [random.choice(CATEGORIES) for _ in range(rows_per_file)]
        ts_list = [1_700_000_000_000_000 + random.randint(0, 86400 * 365 * 1_000_000) for _ in range(rows_per_file)]

        table = pa.table({
            'id': ids,
            'name': names,
            'value': values,
            'category': cats,
            'ts': ts_list,
        }, schema=schema)

        pq.write_table(table, os.path.join(out_dir, f'part_{i:06d}.parquet'),
                        row_group_size=rows_per_file, compression='snappy')

        if (i + 1) % 500 == 0:
            print(f'  Generated {i+1}/{num_files} files...')

    elapsed = time.time() - t0
    print(f'Generated {num_files} files x {rows_per_file} rows in {elapsed:.1f}s -> {out_dir}')


def main():
    scenarios = [
        (100, 100, 'scenario_100f_100r'),
        (500, 1000, 'scenario_500f_1000r'),
        (1000, 100, 'scenario_1000f_100r'),
        (5000, 50, 'scenario_5000f_50r'),
        (10000, 50, 'scenario_10000f_50r'),
    ]

    # Allow selecting a single scenario via CLI arg
    if len(sys.argv) > 1:
        idx = int(sys.argv[1])
        scenarios = [scenarios[idx]]

    for num_files, rows_per_file, dirname in scenarios:
        out_dir = os.path.join(DATA_DIR, dirname)
        if os.path.exists(out_dir) and len(os.listdir(out_dir)) >= num_files:
            print(f'Skipping {dirname} (already exists)')
            continue
        generate_files(num_files, rows_per_file, out_dir)


if __name__ == '__main__':
    main()
