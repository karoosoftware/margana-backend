# Margana Backend

This repository contains the backend and shared data-processing code for Margana. It is the backend/data slice extracted from the original monorepo and now owns the Python packages, backend resources, and email templates needed to generate puzzles, score submissions, and compute player metrics.

The repository split strategy is documented in [docs/REPO_SPLIT_STRATEGY.md](/Users/paulbradbury/IdeaProjects/margana-backend/docs/REPO_SPLIT_STRATEGY.md). Based on that roadmap, Phase 2.1 is complete here: the backend code, resources, and templates have been migrated into this standalone repository.

## Scope

This repo currently owns:

- Puzzle generation logic and S3 sync utilities.
- Scoring and results-building logic for Margana submissions.
- Metrics and badge-calculation services backed by DynamoDB.
- Backend sample payloads and event fixtures under `resources/`.
- Postmark email templates under `postmarkTemplates/`.

This repo does not contain:

- The Vue frontend (`margana-web` target repo in the split plan).
- Terraform / AWS infrastructure (`margana-infra` target repo in the split plan).

## Repository Layout

```text
python/
  margana_gen/       Puzzle generation, usage-log handling, S3 upload/download helpers
  margana_score/     Word list loading, score calculation, grid/result building, auth helpers
  margana_metrics/   Metrics aggregation and badge milestone logic
  margana_costing/   DynamoDB capacity / costing log helpers
  tests/             Pytest suite
resources/           Sample events, payload fixtures, and local generation outputs
postmarkTemplates/   Backend email templates
docs/                Migration and repository split documentation
```

## Python Packages

### `margana_gen`

Owns puzzle generation and related operational helpers.

- `cli.py`: local/CI entry point for generating puzzle payloads.
- `puzzle_gen.py`: constructs level puzzles.
- `usage_log.py`: prevents puzzle/word reuse with cooldown rules.
- `s3_utils.py`: downloads the word list and usage logs, uploads generated outputs.
- `word_graph.py`: word-chain graph utilities used by puzzle generation.

### `margana_score`

Owns submission scoring and result-shaping logic.

- `results_builder.py`: shared logic for rebuilding grids, finding valid words, and computing scores/bonuses.
- `margana_madness.py`: madness-path detection/integration.
- `wordlist.py` and `wordlist_loader.py`: word-list loading and caching, including bundled Lambda package support.
- `s3_utils.py`: backend helpers for fetching the word list and writing JSON results to S3.
- `auth_utils.py`: request-user extraction helpers.

### `margana_metrics`

Owns metrics derivation and badge milestone calculation.

- `metrics_service.py`: DynamoDB-backed weekly stats, badge counts, and breakout derivation.
- `badge-milestones.json`: badge milestone rules.

### `margana_costing`

Owns lightweight cost/capacity logging helpers for DynamoDB-backed operations.

## Local Setup

This project is packaged with `setuptools` and targets Python 3.12+.

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e ".[dev]"
```

If you prefer a minimal install:

```bash
python3 -m pip install -r requirements.txt
```

`requirements.txt` is currently minimal; `pyproject.toml` is the more complete source for local development dependencies and test tooling.

## Running Tests

```bash
python3 -m pytest
```

The pytest configuration is defined in [pyproject.toml](/Users/paulbradbury/IdeaProjects/margana-backend/pyproject.toml) and points at `python/tests`.

## Running Puzzle Generation Locally

The generator can be run as a module:

```bash
python3 -m margana_gen.cli --help
```

Typical local usage:

```bash
python3 -m margana_gen.cli --no-s3-wordlist --no-s3-usage
```

Notes:

- By default the generator expects to fetch the canonical word list from S3.
- In the split strategy, that word list becomes a shared build-time asset managed outside this repo.
- For deployed scoring code, the word list is expected to be bundled into `margana_score/data/`.

## Configuration

The codebase currently uses a small set of environment variables:

- `LOG_LEVEL`: logging level for backend S3 helper modules.
- `MARGANA_ANAGRAM_BONUS`: overrides the default anagram bonus in scoring logic.
- `MARGANA_MADNESS_BONUS`: overrides the default madness bonus in scoring logic.
- `TABLE_USER_RESULTS`: DynamoDB table name override for user results metrics reads/writes.
- `TABLE_WEEK_SCORE_STATS`: DynamoDB table name override for weekly stats.
- `TABLE_USER_BADGES`: DynamoDB table name override for badge state.

AWS access is expected through standard `boto3` credential resolution.

## Resources and Fixtures

- [resources/](/Users/paulbradbury/IdeaProjects/margana-backend/resources) contains sample events and JSON payloads used for backend development and manual verification.
- [python/tests/resources/](/Users/paulbradbury/IdeaProjects/margana-backend/python/tests/resources) contains test fixtures used by the pytest suite.
- [postmarkTemplates/](/Users/paulbradbury/IdeaProjects/margana-backend/postmarkTemplates) contains backend-owned email templates.

## Split Status

Per [docs/REPO_SPLIT_STRATEGY.md](/Users/paulbradbury/IdeaProjects/margana-backend/docs/REPO_SPLIT_STRATEGY.md):

- Phase 2.1 is complete: this backend repo has been initialized with `python/`, `resources/`, and `postmarkTemplates/`.
- The next backend milestones are Phase 2.2 through 2.4:
  - fetch the canonical word list during CI/CD,
  - run tests in CI,
  - package and publish Lambda artifacts,
  - switch infrastructure to artifact-based Lambda deployment,
  - verify deployment in `preprod`.

## Current Gaps

- `README.md` has now been added, but CI/CD packaging and deployment steps are not defined in this repo yet.
- `pytest` was not installed in the current shell environment during this review, so the test suite was not executed here.
