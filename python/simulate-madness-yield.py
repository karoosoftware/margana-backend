#!/usr/bin/env python3
"""
Madness Puzzle Yield Simulator

Purpose
- Run many independent attempts of the generator (without touching S3 or the usage log)
  to estimate success rate, time to success, and projected weekly capacity.

Notes
- This does NOT write usage logs or upload anything. It's a pure simulator around
  the internal build functions in generate-column-puzzle-madness.py.
- It supports both classic and madness-required modes; defaults to --require-madness.

Examples
  python3 python/simulate-madness-yield.py \
    --words-file python/margana-word-list.txt \
    --runs 50 --parallel 4 --require-madness \
    --max-path-tries 400 --max-target-tries 300 --max-diag-tries 200 \
    --report pretty

Outputs
- A summary (pretty or JSON) with success rate, runtime percentiles, and projected weekly capacity.
- Optional CSV with per-run rows via --csv-out.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys as _sys
from datetime import datetime, timedelta

# Local imports from the project
from margana_gen.word_graph import load_words
import hashlib as _hashlib
import json as _json

# Import the internal builders by path (the filename has a hyphen, so we can't use a normal module import)
import importlib.util as _importlib_util
import sys as _sys

_DEF_GEN_PATH = (Path(__file__).resolve().parents[0] / "generate-column-puzzle-madness.py").resolve()
_spec = _importlib_util.spec_from_file_location("gen_madness_module", str(_DEF_GEN_PATH))
if _spec is None or _spec.loader is None:
    raise ImportError(f"Unable to load builders from {_DEF_GEN_PATH}")
_gen_mod = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_gen_mod)  # type: ignore

# Extract builders
build_puzzle_with_path = getattr(_gen_mod, "build_puzzle_with_path")
build_puzzle = getattr(_gen_mod, "build_puzzle")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Simulate puzzle generation to estimate yield/performance.")
    p.add_argument("--words-file", type=str, default=str(Path(__file__).resolve().parents[0] / "margana-word-list.txt"))
    p.add_argument("--seed", type=int, default=None, help="Base RNG seed (per-run seeds will be derived).")

    # Simulation sizing
    p.add_argument("--runs", type=int, default=50, help="Number of independent generation attempts to run.")
    p.add_argument("--parallel", type=int, default=1, help="Number of parallel worker processes.")
    p.add_argument("--max-seconds", type=float, default=None, help="Optional wall-time budget; stop launching new runs after this many seconds.")

    # Generator parameters (mirrors builder flags)
    p.add_argument("--require-madness", action="store_true", help="Use the path-first builder requiring madness.")
    p.add_argument("--madness-word", type=str, default="both", choices=["margana","anagram","both"], help="Which madness word(s) to use when path-first.")
    p.add_argument("--diag-direction", type=str, default="random", choices=["main","anti","random"], help="Diagonal direction preference.")
    p.add_argument("--max-path-tries", type=int, default=200)
    p.add_argument("--max-target-tries", type=int, default=500)
    p.add_argument("--max-column-tries", type=int, default=5)
    p.add_argument("--max-diag-tries", type=int, default=200)

    # Reporting & progress
    p.add_argument("--report", type=str, default="pretty", choices=["pretty","json"], help="Summary output format.")
    p.add_argument("--csv-out", type=str, default=None, help="Write per-run metrics to CSV at this path.")
    p.add_argument("--progress", action="store_true", help="Print periodic progress/heartbeat to stderr while running.")
    p.add_argument("--progress-interval", type=float, default=10.0, help="Seconds between heartbeat progress lines when --progress is set.")
    p.add_argument("--stream", action="store_true", help="Stream a one-line status to stderr for each completed run.")
    p.add_argument("--report-every", type=int, default=0, help="If >0, print a brief summary to stderr every K completions.")
    p.add_argument("--max-seconds-hard", type=float, default=None, help="If set, stop waiting after this many seconds and summarize completed runs only.")
    p.add_argument("--per-run-seconds", type=float, default=None, help="Optional hard timeout per run in seconds; timed-out runs count as failures with reason 'timeout'.")

    # Usage log simulation (optional, defaults to no I/O)
    p.add_argument("--usage-in", type=str, default=None, help="Read an existing usage log JSON and avoid repeats found there (read-only).")
    p.add_argument("--usage-out", type=str, default=None, help="Write accepted unique puzzles to this usage log JSON (sandbox file).")
    p.add_argument("--level-key", type=str, default="column_puzzle", help="Top-level usage log key to store puzzles under (default: column_puzzle).")
    p.add_argument("--simulate-weeks", type=int, default=None, help="If set, simulate scheduling up to this many weeks (e.g., 260 for ~5 years).")
    p.add_argument("--per-week-cap", type=int, default=1, help="Max unique puzzles to accept per simulated week (default: 1).")
    p.add_argument("--start-date", type=str, default=None, help="Start date ISO YYYY-MM-DD for simulated weeks (default: today).")
    p.add_argument("--cooldown-days", type=int, default=7, help="Cooldown in days between uses when writing usage-out (default: 7 = weekly).")

    return p.parse_args()


def _worker_once(args_tuple: Tuple[int, Dict[str, Any], List[str]]) -> Dict[str, Any]:
    run_index, cfg, words5 = args_tuple
    import random
    import signal

    class _RunTimeout(Exception):
        pass

    def _timeout_handler(signum, frame):
        raise _RunTimeout()

    rng = random.Random(cfg.get("seed"))
    t0 = time.perf_counter()
    ok = False
    err: Optional[str] = None
    detail: Dict[str, Any] = {}

    per_run_seconds = cfg.get("per_run_seconds")
    timer_set = False
    try:
        if per_run_seconds is not None and per_run_seconds > 0:
            # Use real-time interval timer for sub-second precision (Unix only)
            try:
                signal.signal(signal.SIGALRM, _timeout_handler)
                signal.setitimer(signal.ITIMER_REAL, float(per_run_seconds))
                timer_set = True
            except Exception:
                # Fallback: no timer; proceed without per-run enforcement
                timer_set = False
        if cfg.get("require_madness"):
            target, col, ddir, diag, rows, path, madness_word = build_puzzle_with_path(
                words5=words5,
                rng=rng,
                max_path_tries=int(cfg["max_path_tries"]),
                madness_word_mode=str(cfg["madness_word"]),
                diag_direction_pref=str(cfg["diag_direction"]),
                max_target_tries=int(cfg["max_target_tries"]),
                max_column_tries=int(cfg["max_column_tries"]),
                max_diag_tries=int(cfg["max_diag_tries"]),
            )
            ok = True
            detail.update({
                "target": target,
                "column": col,
                "diag_dir": ddir,
                "diag_target": diag,
                "rows": rows,
                "madness_word": madness_word,
                "path": path,
            })
        else:
            target, col, ddir, diag, rows = build_puzzle(
                words5=words5,
                rng=rng,
                target_forced=None,
                column_forced=None,
                diag_target_forced=None,
                diag_direction_pref=str(cfg["diag_direction"]),
                max_target_tries=int(cfg["max_target_tries"]),
                max_column_tries=int(cfg["max_column_tries"]),
                max_diag_tries=int(cfg["max_diag_tries"]),
            )
            ok = True
            detail.update({
                "target": target,
                "column": col,
                "diag_dir": ddir,
                "diag_target": diag,
                "rows": rows,
            })
    except _RunTimeout:
        ok = False
        err = "timeout"
    except Exception as e:
        ok = False
        err = str(e)
    finally:
        if timer_set:
            try:
                signal.setitimer(signal.ITIMER_REAL, 0)
            except Exception:
                pass

    dt = time.perf_counter() - t0
    return {
        "run": run_index,
        "ok": ok,
        "seconds": dt,
        "error": err,
        "detail": detail,
    }


def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(results)
    succ = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]
    succ_times = [r.get("seconds", 0.0) for r in succ]

    summary: Dict[str, Any] = {
        "runs": n,
        "successes": len(succ),
        "failures": len(fail),
        "success_rate": (len(succ) / n) if n else 0.0,
    }

    if succ_times:
        summary.update({
            "time_sec_avg": statistics.mean(succ_times),
            "time_sec_med": statistics.median(succ_times),
            "time_sec_p90": statistics.quantiles(succ_times, n=10)[-1] if len(succ_times) >= 10 else max(succ_times),
            "time_sec_min": min(succ_times),
            "time_sec_max": max(succ_times),
        })
        # Projected throughput assuming sequential runs with median runtime
        med = summary["time_sec_med"]
        if med > 0:
            per_hour = math.floor(3600.0 / med * summary["success_rate"])  # successes/hour
            per_week = per_hour * 24 * 7
            summary.update({
                "proj_success_per_hour": per_hour,
                "proj_success_per_week": per_week,
            })
    else:
        summary.update({
            "time_sec_avg": None,
            "time_sec_med": None,
            "time_sec_p90": None,
            "time_sec_min": None,
            "time_sec_max": None,
            "proj_success_per_hour": 0,
            "proj_success_per_week": 0,
        })

    # Common failure reasons (top 5)
    reasons: Dict[str, int] = {}
    for r in fail:
        k = r.get("error") or "unknown"
        reasons[k] = reasons.get(k, 0) + 1
    top_reasons = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[:5]
    summary["top_failure_reasons"] = [{"reason": k, "count": v} for k, v in top_reasons]
    return summary


def main():
    args = parse_args()

    words_path = Path(args.words_file).resolve()
    if not words_path.exists():
        raise FileNotFoundError(f"Word list not found at {words_path}")

    words_by_len, all_words = load_words(str(words_path))
    words5 = words_by_len.get(5, [])
    if not words5:
        raise RuntimeError("No 5-letter words found in the provided word list.")

    # Prepare per-run configs
    base_seed = args.seed if args.seed is not None else int(time.time())

    def cfg_for_run(i: int) -> Dict[str, Any]:
        return {
            "seed": (base_seed + i * 9973),  # decorrelated per worker
            "require_madness": bool(args.require_madness),
            "madness_word": str(args.madness_word),
            "diag_direction": str(args.diag_direction),
            "max_path_tries": int(args.max_path_tries),
            "max_target_tries": int(args.max_target_tries),
            "max_column_tries": int(args.max_column_tries),
            "max_diag_tries": int(args.max_diag_tries),
            "per_run_seconds": (float(args.per_run_seconds) if args.per_run_seconds is not None else None),
        }

    jobs: List[Tuple[int, Dict[str, Any], List[str]]] = []
    start_launch = time.perf_counter()
    for i in range(args.runs):
        if args.max_seconds is not None and (time.perf_counter() - start_launch) > args.max_seconds:
            break
        jobs.append((i + 1, cfg_for_run(i + 1), words5))

    results: List[Dict[str, Any]] = []

    def _emit_heartbeat():
        if not args.progress:
            return
        elapsed = time.perf_counter() - start_launch
        done = len(results)
        succ = sum(1 for r in results if r.get("ok"))
        fail = done - succ
        total = len(jobs)
        _sys.stderr.write(
            f"[progress] elapsed={elapsed:6.1f}s launched={total} completed={done} successes={succ} failures={fail}\n"
        )
        _sys.stderr.flush()

    def _emit_stream(r: Dict[str, Any]):
        if not args.stream:
            return
        d = r.get("detail") or {}
        status = "OK" if r.get("ok") else "ERR"
        tgt = d.get("target") or "-"
        diag = d.get("diag_target") or "-"
        secs = r.get("seconds", 0.0)
        _sys.stderr.write(
            f"[run {r.get('run')}] {status} {secs:.2f}s target={tgt} diag={diag}\n"
        )
        _sys.stderr.flush()

    def _emit_periodic_summary():
        if not args.report_every or args.report_every <= 0:
            return
        done = len(results)
        if done == 0 or (done % args.report_every) != 0:
            return
        succ = [r for r in results if r.get("ok")]
        fail = done - len(succ)
        succ_times = [r.get("seconds", 0.0) for r in succ]
        med = statistics.median(succ_times) if succ_times else None
        rate = (len(succ) / done) if done else 0.0
        _sys.stderr.write(
            (
                f"[summary {done}] success_rate={rate*100:.1f}% successes={len(succ)} failures={fail} "
                + (f"med_sec={med:.2f}" if med is not None else "med_sec=NA")
                + "\n"
            )
        )
        _sys.stderr.flush()

    # Optional startup banner
    if args.progress or args.stream:
        _sys.stderr.write(
            (
                f"[start] runs={args.runs} parallel={args.parallel} require_madness={bool(args.require_madness)} "
                f"madness_word={args.madness_word} diag={args.diag_direction} max_seconds={args.max_seconds} "
                f"max_seconds_hard={args.max_seconds_hard}\n"
            )
        )
        _sys.stderr.flush()

    # CSV streaming setup (optional)
    csv_stream_file = None
    csv_stream_writer = None
    if args.csv_out and args.stream:
        csv_path = Path(args.csv_out).resolve()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_stream_file = open(csv_path, "w", newline="", encoding="utf-8")
        csv_stream_writer = csv.writer(csv_stream_file)
        csv_stream_writer.writerow(["run","ok","seconds","error","target","column","diag_dir","diag_target","rows","madness_word"]) 
        csv_stream_file.flush()

    last_hb = time.perf_counter()

    if args.parallel and args.parallel > 1:
        with ProcessPoolExecutor(max_workers=args.parallel) as ex:
            futs = {ex.submit(_worker_once, job): job[0] for job in jobs}
            while futs:
                now = time.perf_counter()
                remaining_budget = None
                if args.max_seconds_hard is not None:
                    remaining_budget = max(0.0, args.max_seconds_hard - (now - start_launch))
                    if remaining_budget == 0.0:
                        # Hard stop: do not wait further
                        break
                timeout = args.progress_interval if args.progress else (remaining_budget if remaining_budget is not None else None)
                if timeout is not None and remaining_budget is not None:
                    timeout = min(timeout, remaining_budget) if args.progress else remaining_budget
                done_set, pending_set = wait(list(futs.keys()), timeout=timeout, return_when=FIRST_COMPLETED)
                for fut in done_set:
                    try:
                        r = fut.result()
                    except Exception as e:
                        # Capture worker failure as a failed run with error
                        run_idx = futs.get(fut)
                        r = {"run": run_idx, "ok": False, "seconds": 0.0, "error": str(e), "detail": {}}
                    results.append(r)
                    _emit_stream(r)
                    if csv_stream_writer is not None:
                        d = r.get("detail") or {}
                        csv_stream_writer.writerow([
                            r.get("run"),
                            int(bool(r.get("ok"))),
                            f"{r.get('seconds', 0.0):.4f}",
                            (r.get("error") or ""),
                            (d.get("target") or ""),
                            (d.get("column") if d.get("column") is not None else ""),
                            (d.get("diag_dir") or ""),
                            (d.get("diag_target") or ""),
                            (" ".join(d.get("rows") or []) if d.get("rows") else ""),
                            (d.get("madness_word") or ""),
                        ])
                        csv_stream_file.flush()
                    del futs[fut]
                # Heartbeat
                if args.progress and (time.perf_counter() - last_hb) >= args.progress_interval:
                    _emit_heartbeat()
                    last_hb = time.perf_counter()
    else:
        for j in jobs:
            if args.max_seconds_hard is not None and (time.perf_counter() - start_launch) >= args.max_seconds_hard:
                break
            r = _worker_once(j)
            results.append(r)
            _emit_stream(r)
            if csv_stream_writer is not None:
                d = r.get("detail") or {}
                csv_stream_writer.writerow([
                    r.get("run"),
                    int(bool(r.get("ok"))),
                    f"{r.get('seconds', 0.0):.4f}",
                    (r.get("error") or ""),
                    (d.get("target") or ""),
                    (d.get("column") if d.get("column") is not None else ""),
                    (d.get("diag_dir") or ""),
                    (d.get("diag_target") or ""),
                    (" ".join(d.get("rows") or []) if d.get("rows") else ""),
                    (d.get("madness_word") or ""),
                ])
                csv_stream_file.flush()
            if args.progress and (time.perf_counter() - last_hb) >= args.progress_interval:
                _emit_heartbeat()
                last_hb = time.perf_counter()

    # Clean up CSV stream
    if csv_stream_file is not None:
        csv_stream_file.close()

    # Sort results by run index
    results.sort(key=lambda r: r.get("run", 0))

    # CSV output if requested (batch write when not streaming)
    if args.csv_out and csv_stream_writer is None:
        csv_path = Path(args.csv_out).resolve()
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["run","ok","seconds","error","target","column","diag_dir","diag_target","rows","madness_word"]) 
            for r in results:
                d = r.get("detail") or {}
                w.writerow([
                    r.get("run"),
                    int(bool(r.get("ok"))),
                    f"{r.get('seconds', 0.0):.4f}",
                    (r.get("error") or ""),
                    (d.get("target") or ""),
                    (d.get("column") if d.get("column") is not None else ""),
                    (d.get("diag_dir") or ""),
                    (d.get("diag_target") or ""),
                    (" ".join(d.get("rows") or []) if d.get("rows") else ""),
                    (d.get("madness_word") or ""),
                ])

    # ---- Optional usage-log gating and weekly simulation ----
    usage_info: Dict[str, Any] = {}
    def _load_usage(path: Optional[str]) -> Dict[str, Any]:
        if not path:
            return {}
        p = Path(path)
        if not p.exists():
            return {}
        try:
            with open(p, "r", encoding="utf-8") as f:
                return _json.load(f)
        except Exception:
            return {}

    def _save_usage(obj: Dict[str, Any], path: Optional[str]):
        if not path:
            return
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            _json.dump(obj, f, indent=2)

    # Compose a stable puzzle ID identical to the generator's _column_puzzle_id
    _column_puzzle_id = getattr(_gen_mod, "_column_puzzle_id", None)
    def _pid_for_detail(d: Dict[str, Any]) -> Optional[str]:
        if _column_puzzle_id is None:
            return None
        t = d.get("target"); c = d.get("column"); dd = d.get("diag_dir"); dt = d.get("diag_target"); rows = d.get("rows")
        if not (t is not None and c is not None and dd and dt and rows):
            return None
        try:
            return _column_puzzle_id(str(t), int(c), str(dd), str(dt), list(rows))
        except Exception:
            return None

    # Prepare incoming usage log and existing IDs
    incoming_usage = _load_usage(args.usage_in)
    lvl = args.level_key or "column_puzzle"
    lvl_obj = incoming_usage.get(lvl) or {}
    puzzles_map = dict(lvl_obj.get("puzzles") or {})
    existing_ids = set(puzzles_map.keys())

    # Collect unique OK results
    unique_results: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    dup_count = 0
    for r in results:
        if not r.get("ok"):
            continue
        d = r.get("detail") or {}
        pid = _pid_for_detail(d)
        if not pid:
            continue
        if pid in existing_ids or pid in seen_ids:
            dup_count += 1
            continue
        seen_ids.add(pid)
        r = dict(r)
        r["puzzle_id"] = pid
        unique_results.append(r)

    # Weekly simulation scheduling
    accepted: List[Dict[str, Any]] = []
    weeks_target = int(args.simulate_weeks) if args.simulate_weeks is not None else None
    per_week_cap = int(args.per_week_cap)
    start_date = datetime.today().date() if not args.start_date else datetime.strptime(args.start_date, "%Y-%m-%d").date()

    if weeks_target is None:
        # Accept all unique results; record using today's date
        current_date = start_date
        for r in unique_results:
            r["scheduled_date"] = current_date.isoformat()
            accepted.append(r)
    else:
        # Fill up to weeks_target weeks, with per_week_cap per week
        idx = 0
        week = 0
        while week < weeks_target and idx < len(unique_results):
            week_date = start_date + timedelta(days=7*week)
            taken = 0
            while taken < per_week_cap and idx < len(unique_results):
                rr = unique_results[idx]
                rr = dict(rr)
                rr["scheduled_date"] = week_date.isoformat()
                accepted.append(rr)
                idx += 1
                taken += 1
            week += 1

    # Optionally write usage-out with accepted puzzle IDs and dates
    out_usage: Dict[str, Any] = _load_usage(args.usage_out) if args.usage_out else {}
    if args.usage_out:
        lvl_out = out_usage.setdefault(lvl, {"puzzles": {}})
        lvl_puzzles = lvl_out.setdefault("puzzles", {})
        for r in accepted:
            pid = r.get("puzzle_id")
            date_iso = r.get("scheduled_date") or start_date.isoformat()
            if pid:
                lvl_puzzles.setdefault(pid, date_iso)
        _save_usage(out_usage, args.usage_out)

    # Build usage summary
    usage_info = {
        "unique_candidates": len(unique_results),
        "duplicates_skipped": dup_count,
        "accepted_unique": len(accepted),
        "weeks_target": weeks_target,
        "weeks_filled": (min(weeks_target, math.ceil(len(accepted)/per_week_cap)) if weeks_target else None),
        "per_week_cap": per_week_cap,
        "usage_in": str(Path(args.usage_in).resolve()) if args.usage_in else None,
        "usage_out": str(Path(args.usage_out).resolve()) if args.usage_out else None,
        "level": lvl,
    }

    summary = _aggregate(results)

    if args.report == "json":
        print(json.dumps({"summary": summary, "results": results, "usage": usage_info, "accepted": accepted}, indent=2))
    else:
        # Pretty summary
        print("\n=== Madness Puzzle Yield Simulator ===")
        print(f"Runs:         {summary['runs']}")
        print(f"Successes:    {summary['successes']}")
        print(f"Failures:     {summary['failures']}")
        print(f"Success rate: {summary['success_rate']*100:.1f}%")
        if summary.get("time_sec_med") is not None:
            print(f"Time (s):     avg={summary['time_sec_avg']:.2f}  med={summary['time_sec_med']:.2f}  p90={summary['time_sec_p90']:.2f}  min={summary['time_sec_min']:.2f}  max={summary['time_sec_max']:.2f}")
            print(f"Projected:    ~{summary['proj_success_per_hour']} successes/hour, ~{summary['proj_success_per_week']} per week")
        if summary.get("top_failure_reasons"):
            print("Top failure reasons:")
            for item in summary["top_failure_reasons"]:
                print(f"  - {item['reason']}  (x{item['count']})")
        if args.csv_out:
            print(f"Per-run CSV written to: {Path(args.csv_out).resolve()}")
        # Usage block
        if args.usage_in or args.usage_out or args.simulate_weeks is not None:
            print("\nUsage/Weekly simulation:")
            print(f"  unique candidates:  {usage_info['unique_candidates']}")
            print(f"  duplicates skipped: {usage_info['duplicates_skipped']}")
            print(f"  accepted unique:    {usage_info['accepted_unique']}")
            if usage_info['weeks_target'] is not None:
                print(f"  weeks target:       {usage_info['weeks_target']}")
                print(f"  weeks filled:       {usage_info['weeks_filled']}")
                print(f"  per-week cap:       {usage_info['per_week_cap']}")
            if usage_info['usage_in']:
                print(f"  usage-in:           {usage_info['usage_in']}")
            if usage_info['usage_out']:
                print(f"  usage-out:          {usage_info['usage_out']}")


if __name__ == "__main__":
    main()
