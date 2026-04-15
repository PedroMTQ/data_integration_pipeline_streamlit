"""
Profile Dataset1MetaModel bronze/silver validation.

Run from repo root (models optional extra, env for settings):

  EMBEDDING_DIMENSIONS=1 PYTHONPATH=src uv run python dev/profile_dataset_1_model.py --rows 2000
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
import time
from collections.abc import Callable
from io import StringIO


from data_integration_pipeline.common.core.models.dataset_1.metamodel import Dataset1MetaModel
from data_integration_pipeline.common.io.cloud_storage import CloudFileReader


def load_rows() -> list[dict]:
    file_path = 'chunked_bronze/dataset_1/019d7cc1-2968-7064-9933-27c2fb63f255/chunk_2.parquet'
    reader = CloudFileReader(file_path, as_table=False)
    res = []
    for row in reader:
        res.append(row)
        if len(res) >= 10000:
            break
    return res
    return [row for row in reader]


def row_runner(rows: list[dict], function: Callable[[list[dict]], tuple[int, int]]) -> Callable[[], tuple[int, int]]:
    def run() -> tuple[int, int]:
        return function(rows)

    return run


def _run_validate_bronze(rows: list[dict]) -> tuple[int, int]:
    """``from_raw_record`` only (materializes bronze on the metamodel instance)."""
    ok = err = 0
    for row in rows:
        try:
            _ = Dataset1MetaModel.from_raw_record(raw_record=row).silver_record
            ok += 1
        except Exception:
            err += 1
    return ok, err


def _run_production_path(rows: list[dict]) -> tuple[int, int]:
    """Same path as ``BronzeProcessor``: validate + ``silver_record.model_dump()``."""
    ok = err = 0
    for row in rows:
        try:
            _ = Dataset1MetaModel.from_raw_record(raw_record=row).silver_record.model_dump()
            ok += 1
        except Exception:
            err += 1
    return ok, err


def profile_phase(
    label: str,
    func: Callable[[], tuple[int, int]],
    rows: list[dict],
    top_n: int,
    show_callers: bool,
    save_path: str | None,
    profiler: cProfile.Profile | None = None,
) -> pstats.Stats:
    print(f'{"=" * 80}')
    print(f'  PROFILING: {label}  ({len(rows)} rows)')
    print(f'{"=" * 80}\n')

    own_profiler = profiler is None
    if own_profiler:
        profiler = cProfile.Profile()
    t0 = time.perf_counter()
    profiler.enable()
    ok, err = func()
    profiler.disable()
    elapsed = time.perf_counter() - t0

    print(f'  Wall time : {elapsed:.3f}s')
    print(f'  Successes : {ok}')
    print(f'  Failures  : {err}')
    print(f'  Throughput: {len(rows) / elapsed:.0f} rows/s\n')

    stats = pstats.Stats(profiler, stream=sys.stdout)
    stats.strip_dirs()

    print(f'--- Top {top_n} by CUMULATIVE time ---')
    stats.sort_stats('cumulative').print_stats(top_n)

    print(f'--- Top {top_n} by TOTAL (self) time ---')
    stats.sort_stats('tottime').print_stats(top_n)

    if show_callers:
        print(f'--- Callers for top {min(top_n, 15)} functions ---')
        stats.sort_stats('cumulative').print_callers(min(top_n, 15))

    if save_path and own_profiler:
        suffix = label.lower().replace(' ', '_').replace('+', 'and')
        out = save_path.replace('.prof', f'_{suffix}.prof') if '.prof' in save_path else f'{save_path}_{suffix}.prof'
        profiler.dump_stats(out)
        print(f'  Profile saved to {out}')
        print(f'    View with: snakeviz {out}')
        print(f'    Or:        python -m pyprof2calltree -i {out} -k\n')

    return stats


def _print_hotspot_summary(stats: pstats.Stats, top_n: int = 15) -> None:
    """Print a compact table of the biggest time sinks for quick scanning."""
    buf = StringIO()
    # Python 3.13+ Stats() no longer accepts another Stats instance; merge via add().
    tmp = pstats.Stats(stream=buf)
    tmp.add(stats)
    tmp.strip_dirs().sort_stats('tottime').print_stats(9999)
    buf.seek(0)

    lines = buf.read().splitlines()
    header_idx = next((i for i, ln in enumerate(lines) if 'ncalls' in ln and 'tottime' in ln), None)
    if header_idx is None:
        return

    data_lines = lines[header_idx + 1 :]
    entries = []
    for ln in data_lines:
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split(None, 5)
        if len(parts) < 6:
            continue
        try:
            tottime = float(parts[1])
            cumtime = float(parts[3])
            func_desc = parts[5]
            entries.append((tottime, cumtime, func_desc))
        except (ValueError, IndexError):
            continue

    entries.sort(key=lambda e: e[0], reverse=True)
    entries = entries[:top_n]

    if not entries:
        return

    total_self = sum(e[0] for e in entries)

    print(f'\n{"=" * 80}')
    print('  HOTSPOT SUMMARY  (top functions by self-time)')
    print(f'{"=" * 80}')
    print(f'  {"Self (s)":>10}  {"%Self":>6}  {"Cum (s)":>10}  Function')
    print(f'  {"-" * 10}  {"-" * 6}  {"-" * 10}  {"-" * 40}')
    for tottime, cumtime, func_desc in entries:
        pct = (tottime / total_self * 100) if total_self else 0
        bar = '█' * int(pct / 2)
        print(f'  {tottime:10.4f}  {pct:5.1f}%  {cumtime:10.4f}  {func_desc}  {bar}')
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description='Profile Dataset1MetaModel (bronze / silver path)')
    parser.add_argument('--top', type=int, default=40, help='Number of top functions to show (default: 40)')
    parser.add_argument('--callers', action='store_true', help='Show caller info for top functions')
    parser.add_argument('--save', type=str, default=None, metavar='FILE.prof', help='Save .prof file (viewable with snakeviz)')
    args = parser.parse_args()

    rows = load_rows()

    phases: list[tuple[str, Callable[[], tuple[int, int]]]] = []
    phases.append(('Validate + silver model_dump (production path)', row_runner(rows, _run_production_path)))
    phases.append(('Validate only (from_raw_record)', row_runner(rows, _run_validate_bronze)))

    all_stats: list[pstats.Stats] = []
    shared_profiler: cProfile.Profile | None = cProfile.Profile() if args.save and len(phases) > 1 else None

    for label, run in phases:
        st = profile_phase(
            label,
            run,
            rows,
            args.top,
            args.callers,
            args.save if not shared_profiler else None,
            profiler=shared_profiler,
        )
        all_stats.append(st)

    if shared_profiler and args.save:
        out = args.save if args.save.endswith('.prof') else f'{args.save}.prof'
        shared_profiler.dump_stats(out)
        print(f'  Combined profile saved to {out}')
        print(f'    snakeviz {out}\n')

    if all_stats:
        _print_hotspot_summary(all_stats[0], top_n=20)

    if args.save and not shared_profiler:
        out = args.save if args.save.endswith('.prof') else f'{args.save}.prof'
        print('Tip: install snakeviz for interactive flame graphs:')
        print('  pip install snakeviz')
        print(f'  snakeviz {out}')


if __name__ == '__main__':
    main()
