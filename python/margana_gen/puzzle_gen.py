from .word_graph import find_chain, longest_constructible_words, pick_anagram_targets_from_chain, constructible_words_min_length
from .usage_log import (
    load_usage_log, save_usage_log, select_fresh_pair_for_chain,
    chain_hash, record_chain, puzzle_in_cooldown, record_puzzle
)
import random


def make_level_puzzles(
    words_by_len,
    dictionary_all,
    levels,
    chains_per_level=1,
    cooldown_days=365,
    max_pair_tries=500,
    include_chain_in_output=True,
    usage_log=None,
    mode="free",
    forbid_same_index_repeat=True,
    require_disjoint_end=True,
    max_anagrams_per_chain=3,
    chain_words=None,
):
    """
    Generate puzzles for each level described in 'levels' dict.
    """
    out = {}
    if usage_log is None:
        usage_log = load_usage_log
    if usage_log == load_usage_log:
        # Caller likely forgot to pass a log; let them handle separately normally.
        usage_log = {}

    for level_name, L in levels.items():
        bucket = words_by_len.get(L, [])
        accepted = []
        tries = 0

        while len(accepted) < chains_per_level and tries < max_pair_tries:
            tries += 1
            chain_target_len = chain_words if chain_words is not None else (L + 1)

            chain, _ = find_chain(
                bucket, chain_target_len,
                forbid_same_index_repeat=forbid_same_index_repeat,
                require_disjoint_end=require_disjoint_end
            )
            if not chain:
                continue

            if mode == "free":
                sel = select_fresh_pair_for_chain(level_name, usage_log, chain, cooldown_days=cooldown_days)
                if sel is None:
                    continue
                start, end = sel
                # Compute the longest anagram from the chain letters (exclude chain words)
                longest_candidates = pick_anagram_targets_from_chain(chain, dictionary_all)
                longest_one = longest_candidates[0] if longest_candidates else ""
                rec = {"start": start, "end": end, "longest_anagram": longest_one}
                if include_chain_in_output:
                    rec["chain"] = chain
                accepted.append(rec)

            else:  # fixed mode with composite puzzle IDs
                level_log = usage_log[level_name]
                cid = chain_hash(chain)  # optional analytics
                # Record the chain itself for reference (not used for gating by default)
                from .usage_log import _today_iso  # local import to avoid cycle in docs
                record_chain(level_log, cid, _today_iso())

                # Build per-chain anagram candidates
                candidates = pick_anagram_targets_from_chain(chain, dictionary_all)
                if not candidates:
                    continue

                emitted_from_chain = 0
                for ana in candidates:
                    if emitted_from_chain >= max_anagrams_per_chain:
                        break
                    from .usage_log import puzzle_hash
                    pid = puzzle_hash(chain, ana)
                    if puzzle_in_cooldown(level_log, pid, cooldown_days):
                        continue

                    # Record usage for this composite puzzle
                    record_puzzle(level_log, pid, _today_iso())

                    # Compute longest anagram from the chain letters (exclude chain words)
                    longest_candidates = pick_anagram_targets_from_chain(chain, dictionary_all)
                    longest_one = longest_candidates[0] if longest_candidates else ""

                    rec = {
                        "id": pid,                 # composite id (chain + anagram)
                        "chain_id": cid,           # chain-only id (for analytics)
                        "start": chain[0],
                        "end": chain[-1],
                        "required_chain": chain,
                        "anagram_target": ana,
                        "anagram_pool": "".join(chain),
                        "longest_anagram": longest_one,
                    }
                    accepted.append(rec)
                    emitted_from_chain += 1

                    if len(accepted) >= chains_per_level:
                        break

        # Build anagram words from per-puzzle required_chain letters with length > level length,
        # then remove any candidates that contain a chain word in the same order (e.g., chain word "gasp" excludes "gasps").
        if accepted:
            agg = set()
            if mode == "free":
                # Use each puzzle's chain letters when available; otherwise fallback to start+end pool for that puzzle
                for rec in accepted:
                    if "chain" in rec:
                        pool_letters = "".join(rec["chain"])  # letters from the required chain
                    else:
                        pool_letters = rec["start"] + rec["end"]
                    for w in constructible_words_min_length(pool_letters, dictionary_all, L + 1):
                        agg.add(w)
            else:
                for rec in accepted:
                    pool_letters = "".join(rec["required_chain"])  # fixed mode always has required_chain
                    for w in constructible_words_min_length(pool_letters, dictionary_all, L + 1):
                        agg.add(w)

            # Build forbidden substrings set based on chains/required_chain words
            forbidden = set()
            if mode == "free":
                for rec in accepted:
                    if "chain" in rec:
                        forbidden.update(rec["chain"])  # words of length L
                    else:
                        # If chain omitted, at least avoid start/end appearing as prefixes/substrings
                        forbidden.add(rec["start"])  # length L
                        forbidden.add(rec["end"])    # length L
            else:
                for rec in accepted:
                    forbidden.update(rec["required_chain"])  # words of length L

            # Filter out any anagram that contains any forbidden substring in order
            filtered = [w for w in agg if not any(sub in w for sub in forbidden)]

            # Rank by least repeated letters, then prefer longer words, then alphabetical for stability
            from collections import Counter
            def repeats_count(word: str) -> int:
                c = Counter(word)
                return sum(max(0, v - 1) for v in c.values())

            anagrams_sorted = sorted(
                filtered,
                key=lambda w: (repeats_count(w), -len(w), w)
            )
            # Select top 4 by the ranking above
            anagrams = anagrams_sorted[:4]
        else:
            anagrams = []

        out[level_name] = {
            "length": L,
            "puzzles": accepted,
            "anagram_words": anagrams
        }

    return out
