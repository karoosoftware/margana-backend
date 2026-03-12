#!/usr/bin/env python3
import argparse
import json
import random
import uuid
from datetime import datetime
from pathlib import Path

from .s3_utils import (
    download_word_list_from_s3,
    download_usage_log_from_s3,
    upload_usage_log_to_s3,
    upload_puzzle_output_to_s3,
)
from .word_graph import load_words
from .usage_log import load_usage_log, save_usage_log
from .puzzle_gen import make_level_puzzles
from .stats import run_stats_mode

# ---------- PATHS ----------
SCRIPT_DIR = Path(__file__).resolve()
# Project root is two levels up from this file: scripts/margana_gen/cli.py -> project root
PROJECT_ROOT = SCRIPT_DIR.parents[2]
RESOURCES_DIR = (PROJECT_ROOT / "resources").resolve()
# ----------------------------------------

# ---------- S3 WORD LIST CONFIG ----------
S3_BUCKET = "margana-word-game"
S3_KEY = "words/margana-word-list.txt"
WORD_LIST_FILE = RESOURCES_DIR / "margana-word-list.txt"   # local cache path
WORD_LIST_ETAG_FILE = RESOURCES_DIR / "margana-word-list.etag"
# ----------------------------------------

# ---------- CONFIG ----------
OUTPUT_FILE = RESOURCES_DIR / "margana-puzzle-values.json"
USAGE_LOG_FILE = RESOURCES_DIR / "margana-puzzle-usage-log.json"

# Levels → target word lengths
LEVELS = {
    "easy": 3,
    "medium": 4,
    "hard": 5,
}

CHAINS_PER_LEVEL = 1          # how many puzzles per level to emit
MAX_STARTS = 3000             # randomized start words to try per chain search
MAX_DFS_VISITS = 200000       # safety valve for DFS expansions per chain search
MAX_PAIR_TRIES = 500          # attempts per level to find fresh (start,end) or chain
RNG_SEED = None               # set an int for reproducibility, e.g. 42

# --- Usage tracking (cool-down & log) ---
COOLDOWN_DAYS = 365

# ---------- S3 USAGE LOG CONFIG ----------
USAGE_S3_KEY = "logs/margana-puzzle-usage-log.json"  # key in the same bucket
# ----------------------------------------


def parse_args():
    p = argparse.ArgumentParser(description="Generate Margana puzzles (free or fixed mode) with cooldown & S3 sync.")
    # Core generation / stats
    p.add_argument("--stats", action="store_true",
                   help="Print stats for lengths 3, 4, 5 and exit.")
    p.add_argument("--stats-trials", type=int, default=200,
                   help="Trials per length for Monte Carlo chain estimate in stats mode (default: 200).")
    p.add_argument("--chains-per-level", type=int, default=CHAINS_PER_LEVEL,
                   help="How many puzzles to generate per level (default from script).")
    p.add_argument("--seed", type=int, default=None,
                   help="Random seed for reproducibility.")
    p.add_argument("--max-starts", type=int, default=MAX_STARTS,
                   help="Max randomized starts per chain search.")
    p.add_argument("--max-dfs-visits", type=int, default=MAX_DFS_VISITS,
                   help="Max DFS node expansions per chain search.")
    p.add_argument("--max-pair-tries", type=int, default=MAX_PAIR_TRIES,
                   help="Max attempts per level to find a fresh (start,end) or chain.")
    p.add_argument("--cooldown-days", type=int, default=COOLDOWN_DAYS,
                   help="Days before a start/end/pair/chain/puzzle can be reused.")
    p.add_argument("--no-chain-in-output", action="store_true",
                   help="If set (free mode), omit the full chain from output JSON (only start/end).")
    p.add_argument("--chain-words", type=int, default=None,
                   help="Total number of words in each chain (includes start and end). Defaults to word_length+1 if omitted.")

    # Mode & rule relaxations
    p.add_argument("--mode", choices=["free", "fixed"], default="free",
                   help="Mode: 'free' (start/end+pair cooldown) or 'fixed' (enforced chain + composite puzzle cooldown).")
    p.add_argument("--allow-same-index-twice", action="store_true",
                   help="Allow changing the same letter index in consecutive steps (expands search).")
    p.add_argument("--allow-shared-letters-start-end", action="store_true",
                   help="Allow start and end words to share letters (expands search).")
    p.add_argument("--max-anagrams-per-chain", type=int, default=3,
                   help="In fixed mode, emit up to this many distinct anagram targets per chain (default: 3).")

    # S3 toggles and overrides
    p.add_argument("--no-s3-wordlist", action="store_true",
                   help="Skip downloading the word list from S3; use local file as-is.")
    p.add_argument("--no-s3-usage", action="store_true",
                   help="Skip syncing usage log with S3 (no download, no upload).")
    p.add_argument("--s3-bucket", type=str, default=S3_BUCKET,
                   help="Override S3 bucket name (default from script).")
    p.add_argument("--wordlist-s3-key", type=str, default=S3_KEY,
                   help="Override S3 key for the word list (default from script).")
    p.add_argument("--usage-s3-key", type=str, default=USAGE_S3_KEY,
                   help="Override S3 key for the usage log (default from script).")

    # Puzzle upload options
    p.add_argument("--upload-puzzle", action="store_true",
                   help="If set, upload the generated puzzle JSON to S3 using the provided --puzzle-date.")
    p.add_argument("--puzzle-date", type=str, default=None,
                   help="Date to use for the S3 puzzle path, format DD/MM/YYYY.")
    p.add_argument("--puzzle-s3-prefix", type=str, default="public/puzzles",
                   help="Root prefix for puzzle uploads in S3 (default: 'public/puzzles').")

    # Local output control
    p.add_argument("--output-dir", type=str, default=None,
                   help="Optional directory for puzzle JSON output. If set, puzzles will be saved there instead of alongside OUTPUT_FILE.")
    return p.parse_args()


def main():
    global RNG_SEED, MAX_STARTS, MAX_DFS_VISITS, MAX_PAIR_TRIES, COOLDOWN_DAYS
    args = parse_args()

    # Validate chain words if provided
    if args.chain_words is not None and args.chain_words < 2:
        raise ValueError("--chain-words must be at least 2 (includes start and end).")

    if args.seed is not None:
        RNG_SEED = args.seed
        random.seed(RNG_SEED)
    MAX_STARTS = args.max_starts
    MAX_DFS_VISITS = args.max_dfs_visits
    MAX_PAIR_TRIES = args.max_pair_tries
    COOLDOWN_DAYS = args.cooldown_days

    # Mode & rule toggles
    mode = args.mode
    forbid_same_idx = not args.allow_same_index_twice
    require_disjoint_end = not args.allow_shared_letters_start_end

    # S3: word list
    if not args.no_s3_wordlist:
        ok_wordlist = download_word_list_from_s3(
            bucket=args.s3_bucket,
            key=args.wordlist_s3_key,
            dest_path=WORD_LIST_FILE,
            etag_cache_path=WORD_LIST_ETAG_FILE,
            use_cache=True,
        )
        if not ok_wordlist:
            raise RuntimeError(
                f"Could not obtain word list from S3 and no local file found at {WORD_LIST_FILE}."
            )

    # S3: usage log (download before load)
    if not args.no_s3_usage:
        download_usage_log_from_s3(
            bucket=args.s3_bucket,
            key=args.usage_s3_key,
            dest_path=USAGE_LOG_FILE,
        )

    # Load words and usage log
    words_by_len, all_words = load_words(WORD_LIST_FILE)

    # Initialize or load usage
    usage_log = load_usage_log(USAGE_LOG_FILE)
    # Ensure level keys exist
    for lvl in LEVELS.keys():
        usage_log.setdefault(lvl, {})
        usage_log[lvl].setdefault("pairs", {})
        usage_log[lvl].setdefault("last_used_start", {})
        usage_log[lvl].setdefault("last_used_end", {})
        usage_log[lvl].setdefault("chains", {})
        usage_log[lvl].setdefault("puzzles", {})

    if args.stats:
        run_stats_mode(words_by_len, trials=args.stats_trials)
        return

    # Generate puzzles
    levels_data = make_level_puzzles(
        words_by_len, all_words, LEVELS,
        chains_per_level=args.chains_per_level,
        cooldown_days=COOLDOWN_DAYS,
        max_pair_tries=MAX_PAIR_TRIES,
        include_chain_in_output=(not args.no_chain_in_output),
        usage_log=usage_log,
        mode=mode,
        forbid_same_index_repeat=forbid_same_idx,
        require_disjoint_end=require_disjoint_end,
        max_anagrams_per_chain=args.max_anagrams_per_chain,
        chain_words=args.chain_words,
    )

    today = datetime.today().date().isoformat()
    output = {
        "date": today,
        "levels": levels_data
    }

    # Decide output file path
    out_path = OUTPUT_FILE
    if args.output_dir:
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)
        base = Path(OUTPUT_FILE).name
        out_path = Path(args.output_dir) / base

    if not args.upload_puzzle:
        # Add a random suffix for local uniqueness
        suffix = uuid.uuid4().hex[:6]
        out_path = str(out_path).replace(".json", f"-{suffix}.json")

    # Write to file
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    # Print to stdout
    print(json.dumps(output, indent=2))
    print(f"\n💾 Puzzle saved locally to {out_path}")

    # S3: upload updated usage log after generation
    if not args.no_s3_usage:
        upload_usage_log_to_s3(
            bucket=args.s3_bucket,
            key=args.usage_s3_key,
            src_path=USAGE_LOG_FILE,
        )

    # S3: optional upload of the puzzle output using provided date
    if args.upload_puzzle:
        if not args.puzzle_date:
            raise ValueError("--upload-puzzle requires --puzzle-date in DD/MM/YYYY format.")
        key = upload_puzzle_output_to_s3(
            bucket=args.s3_bucket,
            prefix_root=args.puzzle_s3_prefix,
            puzzle_date_ddmmyyyy=args.puzzle_date,
            src_path=str(out_path),  # upload exactly what we wrote
        )
        print(f"\n✅ Uploaded puzzle to s3://{args.s3_bucket}/{key}")
