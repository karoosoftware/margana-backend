from .word_graph import build_adj_and_stats, monte_carlo_chain_estimate


def run_stats_mode(words_by_len, trials=200):
    print("=== Margana Word Graph Stats ===")
    for L in (3, 4, 5):
        bucket = words_by_len.get(L, [])
        print(f"\n-- Length {L} --")
        print(f"Words: {len(bucket)}")
        if not bucket:
            continue
        stats = build_adj_and_stats(bucket)
        print(f"Avg degree: {stats['deg_avg']:.2f}  |  Median degree: {stats['deg_med']:.0f}  |  Max degree: {stats['deg_max']}")
        print(f"Components: {stats['num_components']}  |  Giant size: {stats['giant_component']} ({stats['pct_in_giant']:.1f}%)")
        mc = monte_carlo_chain_estimate(bucket, L, trials=trials, chain_target_len=L+1)
        print(f"Chain target length: {L+1}  |  Trials: {mc['trials']}  |  Found: {mc['found']}  |  Success rate: {mc['success_rate']:.1f}%")
        if mc["found"] > 0:
            print(f"Avg DFS visits (successes): {mc['avg_visits_success']:.0f}")
