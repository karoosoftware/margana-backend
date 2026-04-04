# Margana Puzzle Generator Performance Notes

This document tracks generator performance after the ECS refactor work, with emphasis on comparing the current `margana-backend` generator path against the older standalone generator in `/Users/paulbradbury/IdeaProjects/margana/python/generate-column-puzzle.py`.

## Goal

Keep the refactored ECS/containerized generator behaviorally correct and validation-safe, while recovering as much of the original batch-generation speed and reliability as possible.

The current state is:

- payload validation is much stronger than before
- container orchestration is working end to end
- scoring/band validation is now stricter and more accurate
- a full 53-week sweep now completes successfully with `MAX_USAGE_TRIES=800`
- performance is still slower than the old standalone generator, especially for `hard` days

## What Changed

The following refactors and safety checks were added on the ECS/generator side:

- shared builder extraction into `margana_gen`
  - `column_logic.py`
  - `semi_completed_builder.py`
  - `valid_words_builder.py`
  - `valid_words_metadata_builder.py`
  - `completed_payload_builder.py`
  - `payload_io.py`
  - `valid_word_items_builder.py`
  - `generator_resources.py`
  - `generator_scoring.py`
  - `generator_bootstrap.py`
  - `generator_difficulty.py`
- separate payload validator CLI:
  - `ecs/validate-puzzle.py`
- ECS container entrypoint orchestration:
  - `ecs/main.py`
- generator wrapper:
  - `ecs/generator_wrapper.py`
- full post-generation validation before treating a run as success
- ISO-week completeness enforcement in `ecs/main.py`
  - a target week now fails unless exactly 7 completed payloads exist

## Important Behavior Changes

These changes are likely relevant to the performance gap:

1. Difficulty gating now uses the finalized published score path.

- The current generator builds final `valid_words_metadata`
- computes the final published `total_score`
- then checks the difficulty band against that score

This is more correct than the older split between an earlier gating score and the later payload score, but it is also more expensive and may reject more candidates.

2. Batch mode now locks the selected band for the whole day.

- A day picks one band
- all retries for that day stay on that band
- this avoids silently drifting into easier bands during retries

This is more faithful to the idea of “random day difficulty”, but it makes hard days more likely to fail if the search space cannot reach the band often enough.

3. Batch mode now again honors weighted random difficulty selection.

The weighted random settings in the current generator are now:

```python
easy=2,medium=4,hard=3
```

This was briefly regressed during refactoring when batch mode used a uniform pick. That has now been fixed and tests were added.

4. `ecs/main.py` now exposes `--max-usage-tries`.

This was tested at:

- `200`
- `400`
- `800`

## Single Source of Truth

Current difficulty bands in the refactored code:

```python
DIFFICULTY_BANDS = {
    "easy": {"min_score": 160, "max_score": 179},
    "medium": {"min_score": 180, "max_score": 199},
    "hard": {"min_score": 200, "max_score": None},
}
```

## Validation and Test Coverage Added

The refactor work added stronger protection than the old generator path had:

- golden tests for generator behavior
- payload validation framework
- diagonal scoring/palindrome/semordnilap regression tests
- weekly madness validation
- horizontal exclude validation
- `difficultyBandApplied` vs final `total_score` validation
- container entrypoint tests
- intentionally failing payload fixtures

Important point:

- the new path is much safer and more observable
- the remaining problem is generator performance and acceptance rate, not missing validation

## Current Diagnostics

Batch runs now emit one diagnostic line per day:

```text
BATCH_DAY date=2026-10-20 band=hard madness=False written=True total_score=208 anagram_length=10 attempts=298/800 builder_exception=0 timeout=0 anagram_length=0 score_below_band=297 score_above_band=0 usage_log_cooldown=0
```

This makes it possible to see exactly why a day failed.

The key rejection categories currently tracked are:

- `builder_exception`
- `timeout`
- `anagram_length`
- `score_below_band`
- `score_above_band`
- `usage_log_cooldown`

## What We Have Observed

### Old Generator Baseline

Examples from the old standalone repo:

```json
{
  "year": 2026,
  "iso_week": 13,
  "days": 7,
  "written": 7,
  "bands_used": {
    "easy": 2,
    "medium": 1,
    "hard": 2,
    "xtream": 1,
    "skipped": 1
  },
  "duration_seconds": 2.736
}
```

and:

```json
{
  "year": 2026,
  "iso_week": 10,
  "days": 7,
  "written": 7,
  "bands_used": {
    "easy": 2,
    "medium": 2,
    "hard": 1,
    "xtream": 1,
    "skipped": 1
  },
  "duration_seconds": 4.627
}
```

So the old generator could produce complete weeks in a few seconds, including a top-tier day.

### New Generator at 200 Attempts

At `--max-usage-tries 200`, week completion was still unreliable.

Example:

- `2026-W14`
- `written: 6`
- one day selected the top tier at the time
- all `200/200` attempts failed with `score_below_band=200`

This proved the failure mode was not builder instability. It was inability to hit the score band.

### New Generator at 400 Attempts

At `--max-usage-tries 400`, completion improved materially.

Observed:

- `2026-W14`: completed, `16.182s`
- `2026-W15`: completed, `30.153s`
- `2026-W16`: completed, `21.683s`
- `2026-W17`: completed, `25.042s`
- `2026-W18`: completed, `53.396s`

But this is still much slower than the old generator.

### Rejection Pattern

The diagnostic lines show a consistent pattern:

- almost all rejections are `score_below_band`
- builder failures are usually `0`
- cooldown is usually `0`
- anagram length failures are rare

That means the current bottleneck is primarily score attainment, not construction failure.

### Hard Band Behavior

After removing `xtream`, `hard` became the most expensive band operationally.

Examples from later successful runs at `800` attempts:

- `2026-10-20`: `hard`, success at `298/800`, score `208`
- `2026-10-31`: `hard`, success at `116/800`, score `209`

Earlier evidence also showed some `hard` days failing at lower retry budgets:

- `400/400` with `score_below_band=400`

This shows the bottleneck is no longer an extra band tier. It is the cost of finding enough `200+` puzzles consistently.

## Comparison to Old Xtream Output

An old top-tier payload from `2026-03-07` had:

- total score `245`
- strong 5-letter rows
- one palindrome row (`kayak`) scoring double
- several diagonal hits
- a 10-letter anagram (`nighthawks`)

The score breakdown there shows high-tier puzzles are achievable without any special semordnilap bonus. The main contributors were:

- row score mass
- palindrome doubling
- extra diagonals
- a high-value long anagram

This suggests the current lower scores are more likely caused by generation quality / candidate search characteristics than by missing diagonal or palindrome scoring.

## What We Have Already Verified

We added tests to ensure the refactor did not drop these behaviors:

- diagonal item discovery
- diagonal forward/reverse inclusion
- diagonal palindrome scoring
- diagonal semordnilap pair scoring as two separate words
- `difficultyBandApplied` matching final `total_score`

So current evidence does not suggest diagonal scoring was lost during refactoring.

## Working Hypothesis

The current refactored generator is slower mainly because of:

1. finalized-score gating instead of earlier approximate gating
2. locked band per day instead of retry-time band drift
3. expensive `hard` score attainment under the current search space
4. weighted random assigning a substantial number of `hard` days

The evidence so far suggests the performance gap is primarily in candidate acceptance rate, not in payload writing, validation, or S3 overhead.

## One-Year Sweep Result

With:

- `MAX_USAGE_TRIES=800`
- current three-band model
- current validation and week-completeness enforcement

the completed yearly run produced:

- `53` weeks started
- `53` weeks completed successfully
- `53` weekly validations passed
- `0` incomplete weeks
- `0` tracebacks
- `371` total puzzle days generated
- `53` madness days
- `318` non-madness days

### Band Distribution

- `easy`: `73` days
- `medium`: `144` days
- `hard`: `101` days
- `skipped`: `53` days

### Score Summary By Band

- `easy`
  - average score: `167.8`
  - range: `160-179`
- `medium`
  - average score: `187.3`
  - range: `180-199`
- `hard`
  - average score: `210.2`
  - range: `200-241`
- `skipped`
  - average score: `193.1`
  - range: `160-232`

### Attempt Summary By Band

- `easy`
  - average attempts: `7.1`
  - median attempts: `4`
- `medium`
  - average attempts: `27.0`
  - median attempts: `18`
- `hard`
  - average attempts: `139.2`
  - median attempts: `99`
- `skipped`
  - average attempts: `1.0`

### Anagram Summary

Accepted puzzles used only:

- `9`
- `10`

By band:

- `easy`: only `10`
- `medium`: `9` and `10`
- `hard`: only `10`
- `skipped`: only `10`

This strongly suggests that in the current search space:

- `hard` success is correlated with 10-letter anagrams
- 8-letter anagrams are not competitive enough to survive score gating

### Rejection Summary

Across the full year run:

- `score_below_band`: `18,105`
- `score_above_band`: `49`
- `builder_exception`: `0`
- `timeout`: `0`
- `anagram_length` rejects: `0`
- `usage_log_cooldown`: `0`

This is the clearest current result:

- the generator is stable
- validation is stable
- cooldown is not the limiting factor in a forward year sweep
- runtime is dominated by score attainment, especially for `hard`

## Five-Year Sweep Result

With:

- `MAX_USAGE_TRIES=800`
- `COOLDOWN_DAYS=365000`
- current three-band model
- current validation and week-completeness enforcement

the completed five-year weekly run produced:

- `261` weeks started
- `261` weeks completed successfully
- `261` weekly validations passed
- `0` incomplete weeks
- `0` tracebacks
- `1827` total puzzle days generated
- `261` madness days
- `1566` non-madness days

### Year Coverage

- `2026`: `53/53` weeks completed
- `2027`: `52/52` weeks completed
- `2028`: `52/52` weeks completed
- `2029`: `52/52` weeks completed
- `2030`: `52/52` weeks completed

### Band Distribution

- `easy`: `381`
- `medium`: `671`
- `hard`: `514`
- `skipped`: `261`

### Score Summary By Band

- `easy`
  - average score: `167.0`
  - range: `160-179`
- `medium`
  - average score: `186.9`
  - range: `180-199`
- `hard`
  - average score: `210.1`
  - range: `200-270`
- `skipped`
  - average score: `194.5`
  - range: `160-265`

### Attempt Summary By Band

- `easy`
  - average attempts: `6.8`
  - median attempts: `5`
- `medium`
  - average attempts: `28.4`
  - median attempts: `20`
- `hard`
  - average attempts: `112.7`
  - median attempts: `75`
- `skipped`
  - average attempts: `1.1`
  - median attempts: `1`

Overall:

- average attempts per written day: `43.7`
- median attempts per written day: `14`

### Anagram Summary

Accepted puzzles used:

- `8`
- `9`
- `10`

By band:

- `easy`: `9`, `10`
- `medium`: `9`, `10`
- `hard`: `8`, `9`, `10`
- `skipped`: `9`, `10`

### Rejection Summary

Across the full five-year run:

- `score_below_band`: `77,695`
- `score_above_band`: `294`
- `builder_exception`: `0`
- `timeout`: `0`
- `anagram_length` rejects: `0`
- `usage_log_cooldown`: `2`

### Usage-Log Observation

The five-year run is the first strong evidence that the usage-log clash protection is functioning under realistic generation volume:

- exact puzzle reuse remained extremely rare
- but it did happen
- `usage_log_cooldown` was triggered `2` times across `1827` generated days

So the cooldown protection appears to be working, but exact board collisions are rare enough that they are not a material runtime driver.

## Current Comparison Baseline

We are currently comparing:

- old standalone generator:
  - `/Users/paulbradbury/IdeaProjects/margana/python/generate-column-puzzle.py`
- new ECS generator:
  - `/Users/paulbradbury/IdeaProjects/margana-backend/ecs/generate-column-puzzle.py`
- container entrypoint:
  - `/Users/paulbradbury/IdeaProjects/margana-backend/ecs/main.py`

Typical comparison command for the old repo:

```bash
python3 python/generate-column-puzzle.py \
  --environment preprod \
  --year 2026 \
  --iso-week 13 \
  --diag-direction random \
  --madness-word both \
  --max-path-tries 400 \
  --max-target-tries 300 \
  --max-diag-tries 200 \
  --use-s3-path-layout
```

Typical comparison command for the new repo in-container:

```bash
MAX_USAGE_TRIES=800 ecs/scripts/run_container_year_iso_sweep.sh \
  /Users/paulbradbury/IdeaProjects/margana-backend/tmp/year-run-review.log
```

## Next Investigation Steps

1. Compare old vs new acceptance logic line by line around batch gating and retry flow.
2. Measure score distribution for accepted and rejected candidates by band.
3. Decide whether the current finalized-score gating is required for production correctness, or whether there is a safe cheaper pre-filter that could restore old performance without reintroducing incorrect band labels.
4. Investigate whether the high attempt count for `hard` can be reduced by improving candidate quality rather than loosening the band.
5. Add a small analysis tool to summarize:
   - attempts by band
   - score-below-band counts by band
   - weekly band distribution
   - hard-day success cost

## Current Recommendation

Current recommendation:

- keep the stronger validation
- keep week-completeness enforcement
- keep the current diagnostic output
- use `MAX_USAGE_TRIES=800` for scheduled generation unless ECS runtime cost becomes unacceptable

The five-year result suggests this is now operationally viable:

- generation completes reliably
- validation remains clean
- uniqueness is sufficient for at least a 5-year horizon
- the remaining tradeoff is ECS runtime cost versus search strictness for `hard`

Do not remove the new safety checks just to match old speed. The right target is:

- old generator speed characteristics
- with new generator correctness and observability
