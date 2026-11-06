#!/usr/bin/env python3
"""Measure matched typed/string row decoders over native in-memory backend results."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import platform
import statistics
import subprocess
from collections import defaultdict
from pathlib import Path


def capture(command: list[str], cwd: Path | None = None) -> str:
    return subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True).stdout


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--build-root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--samples', type=int, default=21)
    parser.add_argument('--target-rows', type=int, default=8192)
    arguments = parser.parse_args()
    if arguments.runs < 3 or arguments.samples < 5 or arguments.target_rows < 256:
        parser.error('use at least 3 runs, 5 samples and 256 target rows')
    root = Path(__file__).resolve().parent.parent
    build = arguments.build_root.resolve()
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    timing = build / 'benchmarks/row_decode_benchmark'
    allocations = build / 'benchmarks/row_decode_allocations'
    verification = capture([str(timing), '--verify-only'])
    verification += capture([str(allocations), '--verify-only'])
    (output / 'verification.txt').write_text(verification)
    all_options = json.loads(capture(['meson', 'introspect', str(build), '--buildoptions']))
    options = {
        entry['name']: entry['value'] for entry in all_options
        if entry['name'] in {'buildtype', 'optimization', 'debug', 'default_library', 'b_lto',
                             'cpp_std', 'cpp_args', 'cpp_link_args', 'benchmarks',
                             'postgres', 'sqlite', 'mysql'}
    }
    if options['optimization'] not in {'2', '3'} or options['default_library'] != 'static' or options['b_lto']:
        raise RuntimeError('use an optimized static build with LTO disabled')
    cpu = platform.processor()
    if platform.system() == 'Darwin':
        cpu = capture(['sysctl', '-n', 'machdep.cpu.brand_string']).strip()
    sources = [
        'benchmarks/row_decode_benchmark.cpp', 'benchmarks/run_row_decode_benchmark.py',
        'benchmarks/measurement_support.cpp', 'benchmarks/measurement_support.hpp',
        'benchmarks/allocation_probe.cpp', 'benchmarks/meson.build',
        'include/asterorm/core/repository.hpp', 'include/asterorm/sql/typed_query.hpp',
        'include/asterorm/sql/typed.hpp', 'include/asterorm/core/codecs.hpp',
        'include/asterorm/core/types.hpp', 'include/asterorm/core/result.hpp',
        *[f'include/asterorm/{backend}/result.hpp' for backend in ('postgres', 'sqlite', 'mysql')],
        *[f'src/database/{backend}/result.cpp' for backend in ('postgres', 'sqlite', 'mysql')],
    ]
    metadata = {
        'started_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'platform': platform.platform(), 'machine': platform.machine(), 'cpu_model': cpu,
        'compiler': json.loads(capture(['meson', 'introspect', str(build), '--compilers'])),
        'build_options': options, 'runs': arguments.runs, 'samples': arguments.samples,
        'target_rows_per_batch': arguments.target_rows, 'row_counts': [1, 256, 16384],
        'material_timing_ratio': 1.10,
        'git_commit': capture(['git', 'rev-parse', 'HEAD'], root).strip(),
        'tracked_worktree_dirty': bool(capture(['git', 'diff', '--name-only', 'HEAD'], root).strip()),
        'source_sha256': {source: hashlib.sha256((root / source).read_bytes()).hexdigest()
                          for source in sources},
    }
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    rows = []
    for run in range(1, arguments.runs + 1):
        measured = list(csv.DictReader(io.StringIO(capture([
            str(timing), '--target-rows', str(arguments.target_rows),
            '--samples', str(arguments.samples), '--seed', str(run),
        ]))))
        if len(measured) != 3 * 3 * 3 * 2 * arguments.samples * 2:
            raise RuntimeError('incomplete timing results')
        rows.extend({'run': run, **row} for row in measured)
        print(f'Completed decoding timing run {run}/{arguments.runs}', flush=True)
    with (output / 'timings.csv').open('w', newline='') as destination:
        writer = csv.DictWriter(destination, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    allocation_text = capture([str(allocations), '--target-rows', '256', '--samples', '1'])
    (output / 'allocations.csv').write_text(allocation_text)
    allocation_rows = list(csv.DictReader(io.StringIO(allocation_text)))
    allocation_index = {
        (row['backend'], row['workload'], row['row_count'], row['phase'], row['path']): row
        for row in allocation_rows
    }
    if len(allocation_index) != 108:
        raise RuntimeError('incomplete allocation results')
    pairs = defaultdict(dict)
    for row in rows:
        key = (row['backend'], row['workload'], row['row_count'], row['phase'], row['run'], row['sample'])
        pairs[key][row['path']] = float(row['ns_per_row'])
    grouped = defaultdict(list)
    per_run = defaultdict(list)
    for key, pair in pairs.items():
        ratio = pair['typed'] / pair['string']
        grouped[key[:4]].append((pair['string'], pair['typed'], ratio))
        per_run[key[:5]].append(ratio)
    lines = [
        '# Typed versus string projection row decoding', '',
        f"Machine: {cpu}, {metadata['platform']}; started {metadata['started_utc']}.", '',
        'Actual production per-row helpers and native backend result getters are measured.',
        'Fixtures are synthetic in-memory text results, not executed SQL queries.',
        'Timed work includes decoding/destruction, plus vector reserve/append/destruction',
        'in `result_vector`. SQL construction, execution, fixture creation and verification',
        'are outside measurement. Timing and C++ allocation counting use separate executables.', '',
        f'{arguments.runs} processes × {arguments.samples} paired samples per case; target',
        f'{arguments.target_rows} decoded rows per batch, at least one whole result set.',
        'Predeclared investigation: paired median ratio >1.10 in every process, or extra',
        'allocation calls/requested bytes. IQR is dispersion, not a confidence interval.', '',
        '| Backend | Shape | Rows/result | Phase | String ns/row | Typed ns/row | Paired ratio [IQR] | Calls/row string→typed | Bytes/row string→typed | Signal |',
        '|---|---|---:|---|---:|---:|---|---:|---:|---|',
    ]
    summary = []
    for key in sorted(grouped, key=lambda key: (key[0], key[1], int(key[2]), key[3])):
        samples = grouped[key]
        ratio = statistics.median(sample[2] for sample in samples)
        quartiles = statistics.quantiles([sample[2] for sample in samples], n=4, method='inclusive')
        baseline = allocation_index[(*key, 'string')]
        typed = allocation_index[(*key, 'typed')]
        calls = [float(row['allocations_per_row']) for row in (baseline, typed)]
        sizes = [float(row['bytes_per_row']) for row in (baseline, typed)]
        signals = []
        if all(statistics.median(per_run[(*key, run)]) > 1.10 for run in range(1, arguments.runs + 1)):
            signals.append('repeatable slowdown')
        elif ratio > 1.10:
            signals.append('timing inconclusive')
        if calls[1] > calls[0]:
            signals.append('extra allocations')
        if sizes[1] > sizes[0]:
            signals.append('extra bytes')
        summary.append({'backend': key[0], 'shape': key[1], 'row_count': int(key[2]),
                        'phase': key[3], 'paired_ratio': ratio, 'signals': signals,
                        'calls_string': calls[0], 'calls_typed': calls[1],
                        'bytes_string': sizes[0], 'bytes_typed': sizes[1]})
        lines.append(
            f'| {key[0]} | {key[1]} | {key[2]} | {key[3]} | '
            f'{statistics.median(sample[0] for sample in samples):.1f} | '
            f'{statistics.median(sample[1] for sample in samples):.1f} | '
            f'{ratio:.3f} [{quartiles[0]:.3f}, {quartiles[2]:.3f}] | '
            f'{calls[0]:.6f}→{calls[1]:.6f} | {sizes[0]:.4f}→{sizes[1]:.4f} | '
            f'{"; ".join(signals) or "within screen"} |'
        )
    lines += ['', 'Raw samples, correctness verification and build/source provenance are retained.',
              'These results do not establish server, I/O, full-query or portable runtime equivalence.', '']
    (output / 'report.md').write_text('\n'.join(lines))
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    metadata['completed_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'Recorded decoding results in {output}', flush=True)


if __name__ == '__main__':
    main()
