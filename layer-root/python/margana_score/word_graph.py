import re
import random
from collections import defaultdict, Counter, deque
from statistics import mean, median

# Ignore patterns for word list cleaning
ignore_patterns = [
    r"\.",        # ignore words with a dot
    r"\d",        # ignore words with digits
    r"[^a-zA-Z]"  # ignore words with any non-letter characters
]


def should_ignore(word: str) -> bool:
    for pattern in ignore_patterns:
        if re.search(pattern, word):
            return True
    return False


def load_words(filename):
    """
    Load words, clean, lower, filter non-letters.
    Return:
      - words_by_len: dict[len] -> list[str]
      - all_words: list[str] (for anagram dictionary)
    """
    with open(filename, "r", encoding="utf-8") as f:
        words = []
        for line in f:
            w = line.strip().lower()
            if w and not should_ignore(w):
                words.append(w)

    words_by_len = defaultdict(list)
    for w in words:
        words_by_len[len(w)].append(w)

    # Shuffle each bucket to diversify search paths
    for L in list(words_by_len.keys()):
        random.shuffle(words_by_len[L])

    return words_by_len, words


def differing_index(a: str, b: str):
    """If a and b differ in exactly one position, return that index; else None."""
    if len(a) != len(b):
        return None
    diff_idx = -1
    for i, (ca, cb) in enumerate(zip(a, b)):
        if ca != cb:
            if diff_idx != -1:
                return None  # more than one difference
            diff_idx = i
    return diff_idx if diff_idx != -1 else None


def build_adjacency(words):
    """
    Adjacency list for words differing by exactly one letter using wildcard buckets.
    """
    if not words:
        return {}
    L = len(words[0])
    buckets = defaultdict(list)
    for w in words:
        for i in range(L):
            buckets[w[:i] + "*" + w[i+1:]].append(w)

    adj = {w: set() for w in words}
    for w in words:
        for i in range(L):
            key = w[:i] + "*" + w[i+1:]
            for v in buckets[key]:
                if v != w:
                    adj[w].add(v)

    # shuffle neighbor order for variety
    adj_shuffled = {}
    for w, nbrs in adj.items():
        lst = list(nbrs)
        random.shuffle(lst)
        adj_shuffled[w] = lst
    return adj_shuffled


def find_chain(
    words,
    chain_target_len,
    max_starts=3000,
    max_visits=200000,
    forbid_same_index_repeat=True,
    require_disjoint_end=True,
):
    """
    Find a chain with 'chain_target_len' words satisfying:
      - each step changes exactly one letter
      - consecutive steps cannot change the same index (unless forbid_same_index_repeat=False)
      - first and last words share no letters (unless require_disjoint_end=False)
      - no repeated words
    Returns (chain: list[str] | None, visits_used: int).
    """
    if not words:
        return None, 0
    words = list(dict.fromkeys(words))  # dedupe preserving order
    random.shuffle(words)

    adj = build_adjacency(words)

    def dfs_from(start_word):
        visits = 0
        # stack holds tuples: (current_word, path_so_far, last_changed_index)
        stack = [(start_word, [start_word], None)]

        while stack:
            current, path, last_idx = stack.pop()
            visits += 1
            if visits > max_visits:
                return None, visits  # safety cutoff

            if len(path) == chain_target_len:
                if (not require_disjoint_end) or set(path[0]).isdisjoint(set(path[-1])):
                    return path, visits
                continue

            if len(path) > chain_target_len:
                continue

            for nbr in adj.get(current, []):
                if nbr in path:
                    continue
                idx = differing_index(current, nbr)
                if idx is None:
                    continue
                if forbid_same_index_repeat and (last_idx is not None) and (idx == last_idx):
                    continue
                stack.append((nbr, path + [nbr], idx))
        return None, visits

    attempts = 0
    total_visits = 0
    for start in words:
        chain, v = dfs_from(start)
        total_visits += v
        attempts += 1
        if chain:
            return chain, total_visits
        if attempts >= max_starts:
            break
    return None, total_visits


def longest_constructible_words(pool_letters, dictionary_words):
    """Return only the longest words that can be formed from the multiset of letters."""
    pool = Counter(pool_letters)
    best = []
    best_len = 0
    for w in dictionary_words:
        c = Counter(w)
        if c - pool:
            continue  # needs letters not available
        lw = len(w)
        if lw > best_len:
            best = [w]
            best_len = lw
        elif lw == best_len:
            best.append(w)
    return sorted(best)


def constructible_words_min_length(pool_letters, dictionary_words, min_len: int):
    """
    Return all dictionary words that can be formed from the given multiset of letters
    with length >= min_len. Sorted alphabetically.
    """
    pool = Counter(pool_letters)
    out = []
    for w in dictionary_words:
        if len(w) < min_len:
            continue
        c = Counter(w)
        if c - pool:
            continue
        out.append(w)
    return sorted(out)


def pick_anagram_targets_from_chain(chain: list[str], dictionary_all: list[str]) -> list[str]:
    """
    Return the list of longest constructible words from the chain's letters,
    excluding any words that appear in the chain itself. Sorted alphabetically.
    """
    letters = "".join(chain)
    longest = longest_constructible_words(letters, dictionary_all)
    chain_set = set(chain)
    return [w for w in longest if w not in chain_set]


# ---- Stats helpers ----

def build_adj_and_stats(words):
    adj = build_adjacency(words)
    degrees = [len(adj[w]) for w in adj]
    n = len(words)
    deg_avg = mean(degrees) if degrees else 0.0
    deg_med = median(degrees) if degrees else 0.0
    deg_max = max(degrees) if degrees else 0

    # connected components via BFS
    seen = set()
    comps = []
    for w in adj.keys():
        if w in seen:
            continue
        q = deque([w])
        seen.add(w)
        size = 0
        while q:
            cur = q.popleft()
            size += 1
            for nb in adj[cur]:
                if nb not in seen:
                    seen.add(nb)
                    q.append(nb)
        comps.append(size)

    comps.sort(reverse=True)
    num_comps = len(comps)
    giant = comps[0] if comps else 0
    pct_in_giant = (giant / n * 100.0) if n else 0.0

    return {
        "n_words": n,
        "deg_avg": deg_avg,
        "deg_med": deg_med,
        "deg_max": deg_max,
        "num_components": num_comps,
        "giant_component": giant,
        "pct_in_giant": pct_in_giant,
        "adj": adj,
    }


def monte_carlo_chain_estimate(words, L, trials=200, chain_target_len=None):
    if not words:
        return {"trials": 0, "found": 0, "success_rate": 0.0, "avg_visits_success": 0.0}
    chain_target_len = chain_target_len or (L + 1)
    found = 0
    visits_list = []
    for _ in range(trials):
        start_pool = words[:]
        random.shuffle(start_pool)
        chain, visits = find_chain(start_pool, chain_target_len)
        if chain:
            found += 1
            visits_list.append(visits)
    sr = (found / trials * 100.0) if trials else 0.0
    av = mean(visits_list) if visits_list else 0.0
    return {"trials": trials, "found": found, "success_rate": sr, "avg_visits_success": av}
