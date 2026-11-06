#!/usr/bin/env python3
"""Record repeated DSL timing samples, separate allocation counts, and provenance."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import os
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
    parser.add_argument('--iterations', type=int, default=10000)
    arguments = parser.parse_args()
    if arguments.runs < 3 or arguments.samples < 5 or arguments.iterations < 100:
        parser.error('use at least 3 runs, 5 samples, and 100 iterations')
    root = Path(__file__).resolve().parent.parent
    build_root = arguments.build_root.resolve()
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    timing = build_root / 'benchmarks/dsl_benchmark'
    allocations = build_root / 'benchmarks/dsl_allocations'
    capture([str(timing), '--verify-only'])
    capture([str(allocations), '--verify-only'])
    (output / 'ast_layouts.csv').write_text(capture([str(timing), '--describe-asts']))
    options = json.loads(capture(['meson', 'introspect', str(build_root), '--buildoptions']))
    selected_options = {
        entry['name']: entry['value'] for entry in options
        if entry['name'] in {'buildtype', 'optimization', 'debug', 'default_library', 'b_lto',
                             'cpp_std', 'cpp_args', 'cpp_link_args', 'benchmarks'}
    }
    if selected_options['optimization'] not in {'2', '3'}:
        raise RuntimeError('refusing to record an unoptimized build')
    sources = [
        'src/sql/compiler.cpp', 'include/asterorm/sql/compiler.hpp',
        'include/asterorm/sql/ast.hpp', 'include/asterorm/sql/typed.hpp',
        'include/asterorm/core/codecs.hpp', 'include/asterorm/core/parameter.hpp',
        *[str(path.relative_to(root)) for path in sorted((root / 'benchmarks').glob('*.cpp'))],
        'benchmarks/measurement_support.hpp', 'benchmarks/meson.build',
        'benchmarks/run_dsl_benchmark.py',
    ]
    cpu_model = platform.processor()
    if platform.system() == 'Darwin':
        cpu_model = capture(['sysctl', '-n', 'machdep.cpu.brand_string']).strip()
    metadata = {
        'started_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'platform': platform.platform(), 'machine': platform.machine(),
        'cpu_model': cpu_model, 'logical_cpus': os.cpu_count(),
        'python': platform.python_version(), 'build_options': selected_options,
        'compilers': json.loads(capture(['meson', 'introspect', str(build_root), '--compilers'])),
        'git_commit': capture(['git', 'rev-parse', 'HEAD'], root).strip(),
        'tracked_worktree_dirty': bool(capture(['git', 'diff', '--name-only', 'HEAD'], root).strip()),
        'runs': arguments.runs, 'samples': arguments.samples, 'iterations': arguments.iterations,
        'allocation_iterations': 256, 'material_timing_ratio': 1.10,
        'source_sha256': {
            source: hashlib.sha256((root / source).read_bytes()).hexdigest() for source in sources
        },
    }
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    timing_rows = []
    for run in range(1, arguments.runs + 1):
        command = [str(timing), '--iterations', str(arguments.iterations),
                   '--samples', str(arguments.samples), '--seed', str(run)]
        rows = list(csv.DictReader(io.StringIO(capture(command))))
        expected_count = 3 * 2 * 3 * arguments.samples * 2
        if len(rows) != expected_count:
            raise RuntimeError(f'expected {expected_count} timing rows, got {len(rows)}')
        timing_rows.extend({'run': run, **row} for row in rows)
        print(f'Completed timing run {run}/{arguments.runs}', flush=True)
    with (output / 'timings.csv').open('w', newline='') as destination:
        writer = csv.DictWriter(destination, fieldnames=list(timing_rows[0]))
        writer.writeheader()
        writer.writerows(timing_rows)
    allocation_text = capture([str(allocations), '--iterations', '256', '--samples', '1'])
    (output / 'allocations.csv').write_text(allocation_text)
    allocation_rows = list(csv.DictReader(io.StringIO(allocation_text)))
    allocation_index = {
        (row['workload'], row['dialect'], row['phase'], row['path']): row
        for row in allocation_rows
    }
    if len(allocation_index) != 36:
        raise RuntimeError('incomplete allocation results')
    pairs = defaultdict(dict)
    for row in timing_rows:
        key = (row['workload'], row['dialect'], row['phase'], row['run'], row['sample'])
        pairs[key][row['path']] = float(row['ns_per_query'])
    grouped = defaultdict(list)
    run_ratios = defaultdict(list)
    for key, pair in pairs.items():
        ratio = pair['typed'] / pair['string']
        grouped[key[:3]].append((pair['string'], pair['typed'], ratio))
        run_ratios[key[:4]].append(ratio)
    lines = [
        '# Typed versus string SQL DSL measurements', '',
        f"Recorded on {metadata['started_utc']} with {cpu_model}, {metadata['platform']}.", '',
        'These measurements cover construction/destruction and SQL/parameter compilation only.',
        'They do not measure decoding, database execution, network I/O, or concurrent use.',
        'Timing uses the standard allocator; a separate executable counts successful C++',
        '`new` calls and requested bytes (not peak RSS, allocator overhead, or direct `malloc`).', '',
        f'{arguments.runs} processes × {arguments.samples} alternating-order paired samples ×',
        f'{arguments.iterations} operations per timing sample. Warmup: 1,000 operations per API/case.',
        'Allocation counting: 256 operations per case, outside all timing measurements.', '',
        'The predeclared investigation screen is a paired median typed/string ratio >1.10',
        'in every process, or extra allocations/requested bytes. It is not a CI performance gate',
        'or a statistical equivalence proof. Ratio IQR is sample dispersion, not a confidence interval.', '',
        '| Workload | Compiler dialect | Phase | String ns/op | Typed ns/op | Paired ratio median [IQR] | Allocations string→typed | Bytes string→typed | Investigation signal |',
        '|---|---|---|---:|---:|---|---:|---:|---|',
    ]
    for key, samples in sorted(grouped.items()):
        string_median = statistics.median(sample[0] for sample in samples)
        typed_median = statistics.median(sample[1] for sample in samples)
        ratio_median = statistics.median(sample[2] for sample in samples)
        quartiles = statistics.quantiles([sample[2] for sample in samples], n=4, method='inclusive')
        allocation_string = allocation_index[(*key, 'string')]
        allocation_typed = allocation_index[(*key, 'typed')]
        calls = [float(row['allocations_per_query']) for row in [allocation_string, allocation_typed]]
        sizes = [float(row['bytes_per_query']) for row in [allocation_string, allocation_typed]]
        signals = []
        if all(statistics.median(run_ratios[(*key, run)]) > 1.10
               for run in range(1, arguments.runs + 1)):
            signals.append('repeatable slowdown')
        elif ratio_median > 1.10:
            signals.append('timing inconclusive')
        if calls[1] > calls[0]:
            signals.append('extra allocations')
        if sizes[1] > sizes[0]:
            signals.append('extra bytes')
        lines.append(
            f'| {key[0]} | {key[1]} | {key[2]} | {string_median:.1f} | {typed_median:.1f} | '
            f'{ratio_median:.3f} [{quartiles[0]:.3f}, {quartiles[2]:.3f}] | '
            f'{calls[0]:g}→{calls[1]:g} | {sizes[0]:g}→{sizes[1]:g} | '
            f'{"; ".join(signals) or "within screen"} |'
        )
    lines += ['', '`postgres_sqlite` is the shared compiler dialect, not a database benchmark.',
              'The compile phase reuses input 0; construction phases vary inputs each iteration.',
              'Object destruction and a common out-of-line observation call are inside each batch.',
              'Do not generalize a within-screen result beyond these workloads and this machine.', '',
              'See `timings.csv`, `allocations.csv`, and `metadata.json` for raw samples and provenance.', '']
    (output / 'report.md').write_text('\n'.join(lines))
    metadata['completed_utc'] = dt.datetime.now(dt.timezone.utc).isoformat()
    (output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    print(f'Recorded results in {output}', flush=True)


if __name__ == '__main__':
    main()
