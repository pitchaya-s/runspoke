"""A small reproducible CPU experiment using only Python's standard library."""
import csv
import argparse
import json
from pathlib import Path
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--data', type=Path, default=Path('measurements.csv'),
                    help='CSV with a value column; absolute paths refer to worker data')
parser.add_argument('--fail', action='store_true', help='Write outputs, then fail for diagnostics')
args = parser.parse_args()

# Stream worker-provisioned datasets without loading them into the job's RAM budget.
count, mean, squared_deviations, sum_squares = 0, 0.0, 0.0, 0.0
with args.data.open() as source:
    for row in csv.DictReader(source):
        value = float(row['value'])
        count += 1
        delta = value - mean
        mean += delta / count
        squared_deviations += delta * (value - mean)
        sum_squares += value * value
if not count:
    parser.error('dataset must contain at least one value')
summary = {'samples': count, 'mean': mean,
           'stdev': (squared_deviations / count) ** 0.5, 'sum_squares': sum_squares}
Path('results').mkdir(exist_ok=True)
Path('results/summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary), flush=True)
if args.fail:
    print('Intentional failure after writing diagnostic outputs', file=sys.stderr, flush=True)
    sys.exit(7)
