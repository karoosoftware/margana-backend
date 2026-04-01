### Margana Puzzle Generator Task Design

This document outlines the design for an ECS-based task that automates the generation, validation, and synchronization of Margana puzzles to S3.

### 1. Workflow Architecture

The process is triggered internally by an **AWS EventBridge Scheduler** on a weekly basis, which invokes an **ECS RunTask** directly:

1.  **Task Invocation**: AWS EventBridge Scheduler triggers the ECS Task.
2.  **Pre-check**: Connect to S3 to check if puzzles for the target week already exist.
3.  **Generate Puzzles**: 
    - Download required assets (word lists, usage logs) from S3.
    - Run the generator logic to produce new puzzles.
    - Write generated files locally to the task's ephemeral storage.
4.  **Validate**: Verify that the generated YAML/JSON files are well-formed and meet game requirements.
5.  **Sync**: Upload the new puzzles and updated usage logs back to S3.
6.  **Notify**: Log the outcome and/or send an email notification (via SES) on success or failure.

### 2. Trigger & Security

The task is intended to be **internal-only** and is not exposed via an HTTP endpoint.

*   **Trigger**: AWS EventBridge Scheduler.
    *   A scheduled rule triggers the task execution via **ECS RunTask** only.
*   **Security**:
    *   The task runs within a VPC private subnet with no public internet ingress required.
    *   IAM-based security: EventBridge requires `ecs:RunTask` and `iam:PassRole` permissions to launch the task.
    *   The task itself uses an IAM Task Role for resource access.

### 3. File System Layout

The repository is structured to support both the existing Lambda-based backend and the ECS-based generation service. The key generation logic is now partially centralized in a shared module under `layer-root/python/margana_gen/`.

```text
.
├── ecs/                            # ECS Container & Job Logic
│   ├── main.py                     # Entrypoint script for the ECS Task
│   ├── generator_wrapper.py        # Logic to invoke the generation scripts
│   ├── requirements.txt            # ECS-specific dependencies
│   ├── Dockerfile                  # Container definition
│   ├── generate-column-puzzle.py   # Existing generator script
│   ├── generate-column-puzzle-madness.py # Existing madness generator script
│   └── validate-puzzle.py          # Standalone payload validator CLI
├── lambdas/                        # Existing Lambda handlers
├── layer-root/                     # Shared Python Logic (Lambda Layer / Package)
│   └── python/
│       ├── margana_gen/            # Puzzle generation utilities
│       │   ├── column_logic.py     # Shared column/diagonal puzzle generation and scoring helpers
│       │   ├── puzzle_gen.py
│       │   ├── s3_utils.py         # S3 download/upload helpers
│       │   ├── usage_log.py        # Usage tracking logic
│       │   └── validation.py       # Shared payload validation framework and rules
│       ├── margana_score/          # Scoring & validation logic
│       └── ...
├── .github/workflows/
│   └── backend-ecs.yml             # CI/CD for ECS container
└── docs/
    └── puzzle-generator-api.md     # This document
```

### 4. Python Application (ECS Task)

The service will be a standalone Python script running as an ECS Task (Fargate or EC2), triggered by EventBridge. It does not maintain a persistent HTTP listener.

*   **Docker Image**:
    *   **Base Image**: `python:3.12-slim`
    *   **Entrypoint**: `["python3", "/app/ecs/main.py"]`
    *   **Default CMD**: `["--target-week", "next"]`
    *   **Smoke Test**: A basic startup check can be run via `python3 /app/ecs/main.py --smoke-test`.
*   **Execution Parameters**: Arguments can be passed via the container's command overrides in the EventBridge Scheduler (e.g., `--target-week`, `--force`).
*   **Logic**:
    *   Uses `boto3` for S3 interactions.
    *   Invokes the generation logic shared through `margana_gen.column_logic` and the ECS generator scripts.
    *   Validates generated payloads in a separate post-generation step using `ecs/validate-puzzle.py` and `margana_gen.validation`.
    *   Uses `SES` or `SNS` for notifications.

### 5. AWS Roles and Permissions

The ECS Task will require an **IAM Task Role** with the following permissions:

#### S3 Permissions
Required to check existence, download assets, and upload new puzzles.
*   `s3:ListBucket` on `margana-puzzles` bucket.
*   `s3:GetObject` on `margana-puzzles/word-lists/*` and `margana-puzzles/usage-logs/*`.
*   `s3:PutObject` on `margana-puzzles/puzzles/*` and `margana-puzzles/usage-logs/*`.

#### SES/SNS Permissions
Required for success/failure notifications.
*   `ses:SendEmail` or `ses:SendRawEmail`.
*   `sns:Publish` (if using SNS for alerts).

#### Logging
*   `logs:CreateLogStream` and `logs:PutLogEvents` for CloudWatch Logs.

### 6. Library Usage & Risk Assessment

To safely modularize the generation logic, we must understand which components depend on our shared libraries.

#### Shared Library Inventory (`layer-root/python/`)

| Library | Primary Responsibility | Risk Level | Used By |
| :--- | :--- | :--- | :--- |
| `margana_score` | Grid validation, scoring, madness detection. | **HIGH** | `lambda_submission.py`, `ecs/generate-column-puzzle.py` |
| `margana_gen` | Word graph traversal, usage logs, shared generator core (`column_logic`), payload validation (`validation`). | **LOW** | `ecs/generate-column-puzzle.py`, `ecs/generate-column-puzzle-madness.py`, `ecs/validate-puzzle.py`, CLI |
| `margana_metrics` | User stats, milestones, badge logic. | **MEDIUM** | Most Lambdas (`submission`, `dashboard`, `metric`, `weekly_seeder`) |
| `margana_costing` | AWS resource costing/tracking logic. | **MEDIUM** | Nearly all Lambdas |

#### Component Dependencies

**1. ECS Generation Scripts (`ecs/`)**
*   **Dependencies**: `margana_gen` (`word_graph`, `usage_log`, `s3_utils`, `column_logic`, `validation`), `margana_score` (`remove_pre_loaded_words`).
*   **Risk**: Low. These are standalone scripts. Changing the logic here only affects the offline puzzle generation process.

**2. Critical Lambda: `lambda_submission.py`**
*   **Dependencies**: `margana_score` (`get_bundled_wordlist_path`, `build_results_response`, `integrate_madness`), `margana_metrics`.
*   **Risk**: **CRITICAL**. Any breaking change to `margana_score` (function signatures, return types) will break the live game submission process.

**3. Utility Lambdas (Dashboard, Leaderboard, etc.)**
*   **Dependencies**: `margana_metrics`, `margana_costing`.
*   **Risk**: Moderate. Affects user-facing stats and internal tracking but not core gameplay.

#### Recommended Action Strategy

1.  **Prefer refactoring shared generator internals into `margana_gen`**: `column_logic` now owns the common column/diagonal row selection, classic puzzle building, and lambda-style total scoring used by the ECS generator scripts.
2.  **Protect `margana_score`**: Do NOT modify existing functions in `margana_score` that are imported by `lambda_submission.py`. If new validation logic is needed for ECS, add new functions or modules rather than updating existing ones.
3.  **Audit `remove_pre_loaded_words`**: This is the only function from `margana_score` used by both ECS and `lambda_submission`. It must remain strictly stable.

### 7. Implemented Refactor Notes

The following generator cleanup has been completed:

*   `margana_gen.column_logic.compute_lambda_style_total(...)` now accepts `letter_scores: Dict[str, int]` instead of loading scores from disk internally.
*   `ecs/generate-column-puzzle.py` now loads `letter-scores-v3.json` locally and passes `LETTER_SCORES` into the shared scorer.
*   `ecs/generate-column-puzzle.py` now delegates its local `choose_rows_for_column_and_diag`, `build_puzzle`, and fallback scoring wrapper to `margana_gen.column_logic`.
*   `ecs/generate-column-puzzle-madness.py` now delegates its shared non-path row-selection and classic puzzle builder behavior to `margana_gen.column_logic`.
*   `tests/test_ecs_generator_core.py` was updated to inject a mock `letter_scores` dictionary directly into the shared scorer.

This refactor reduces file-I/O coupling in the shared logic and makes unit testing easier, but it does not yet provide full end-to-end coverage of both generator scripts.

### 8. Validation Framework

A separate validation framework now exists so payload compliance can be checked independently from puzzle generation.

Implemented components:

*   `margana_gen.validation` provides:
    *   `PuzzleValidationContext`
    *   structured validation issues/results
    *   a rule runner
    *   preset-based rule selection
*   `ecs/validate-puzzle.py` is a standalone validator CLI that:
    *   accepts `--payload-dir`
    *   recursively discovers payload folders containing `margana-completed.json`
    *   optionally loads `margana-semi-completed.json` from the same folder
    *   supports `--verbose` and `--summary-only`
    *   exits non-zero on validation failure

Current default validation rules include:

*   grid must always be 5 rows by 5 characters
*   `valid_words.rows.lr` must match `grid_rows`
*   `valid_words.rows.rl` entries must match reversals of `lr` rows
*   top-level target fields must match `meta`
*   `verticalTargetWord` and `diagonalTargetWord` must match the grid
*   `longestAnagramCount` must match `longestAnagram`
*   the `type="anagram"` metadata item must match `meta.longestAnagram`
*   anagram length and letter inventory checks
*   madness field consistency checks
*   semi-completed payload consistency checks
*   fixed target words and their reverse forms must never appear in scoring metadata
*   all generator bonuses must be zero
*   `valid_words_metadata[*].score` must sum to `total_score`

### 9. Verification Status

The current test coverage includes:

*   shared generator-core tests
*   golden regression tests for both generator scripts using fixed seed, word list, and expected outputs
*   validator rule tests using real payloads from `tmp/payloads`
*   validator CLI tests
*   failing example payload fixtures under `tmp/validation-examples` that are asserted to fail in pytest

Current verification command:

*   `.venv/bin/python -m pytest tests/test_validate_puzzle_cli.py tests/test_puzzle_validation.py tests/test_ecs_generator_core.py tests/test_ecs_generator_golden.py`

This provides strong protection around shared generation behavior and payload compliance, but it still does not fully exercise full script `main()` flows or ECS orchestration end to end.

### 10. Next Steps / Adjustments

*   [ ] Define the exact S3 prefix structure for weekly puzzles.
*   [ ] Determine if the task should accept CLI arguments for target week/force flags.
*   [ ] Define the structure of the success/failure email report.
*   [ ] Add at least one end-to-end generator test covering payload structure and score consistency.
*   [ ] Decide whether ECS generation should always invoke `ecs/validate-puzzle.py` before upload/publish.
*   [ ] Expand validator presets if different compliance levels are needed for local runs vs publish-time checks.
