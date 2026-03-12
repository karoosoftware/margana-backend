# Deployment Inventory

This document defines the current backend deployment inventory for Phase 2.2.1 of the repository split plan. It is the source of truth for:

- which Python entrypoints are deployable Lambda artifacts,
- which paths are excluded from Lambda packaging,
- which shared code and assets belong in the Lambda layer,
- how Lambda artifacts are named for CI/CD and later Terraform integration.

## Deployable Lambda Entry Points

All deployable Lambda handlers currently live under `python/lambdas/`.

| Logical name | Source file | Handler symbol | Purpose / trigger |
| --- | --- | --- | --- |
| `process-margana-results` | `python/lambdas/lambda-process-margana-results.py` | `lambda_handler` | Processes Margana results and writes backend state |
| `athena-events-parquet-runner` | `python/lambdas/lambda_athena_events_parquet_runner.py` | `lambda_handler` | Runs an Athena named query workflow for parquet/event processing |
| `authorizer` | `python/lambdas/lambda_authorizer.py` | `lambda_handler` | API authorization for registered and guest users |
| `cognito-delete-user-audit` | `python/lambdas/lambda_cognito_delete_user_audit.py` | `lambda_handler` | Audits and cleans up backend state after Cognito user deletion |
| `dashboard-summary` | `python/lambdas/lambda_dashboard_summary.py` | `lambda_handler` | Returns dashboard summary API data |
| `leaderboard-service` | `python/lambdas/lambda_leaderboard_service.py` | `lambda_handler` | Serves leaderboard API requests |
| `leaderboard-snapshot` | `python/lambdas/lambda_leaderboard_snapshot.py` | `lambda_handler` | Builds weekly leaderboard snapshot records |
| `margana-metric` | `python/lambdas/lambda_margana_metric.py` | `lambda_handler` | Computes and returns metrics-related responses |
| `post-confirmation-action` | `python/lambdas/lambda_post_confirmation_action.py` | `lambda_handler` | Cognito post-confirmation trigger actions |
| `pre-auth-approval` | `python/lambdas/lambda_pre_auth_approval.py` | `lambda_handler` | Cognito pre-authentication approval gate |
| `profile-service` | `python/lambdas/lambda_profile_service.py` | `lambda_handler` | Serves profile API requests |
| `send-friend-invite` | `python/lambdas/lambda_send_friend_invite.py` | `lambda_handler` | Creates and sends friend/leaderboard invites |
| `submission` | `python/lambdas/lambda_submission.py` | `lambda_handler` | Scores live submissions and writes daily results |
| `terms-audit` | `python/lambdas/lambda_terms_audit.py` | `lambda_handler` | Records terms/audit events |
| `user-settings` | `python/lambdas/lambda_user_settings.py` | `lambda_handler` | Serves user settings API requests |
| `weekly-seeder` | `python/lambdas/process_weekly_seeder.py` | `lambda_handler` | Builds weekly seeder leaderboard data |
| `ses-sns-events-notification` | `python/lambdas/ses_sns_events_notification.py` | `lambda_handler` | Handles SES SNS notification events |

## Packaging Scope and Exclusions

Lambda packaging uses:

- one shared Lambda Layer artifact for common runtime code and assets,
- one thin `.zip` artifact per deployable Lambda handler.

The following paths are excluded from Lambda packaging:

- `python/ecs/`
- `python/tests/`
- `resources/`
- `postmarkTemplates/`
- local-only utilities, experiments, and one-off admin scripts outside `python/lambdas/`

`python/ecs/` is reserved for future puzzle-generation job/container entrypoints and must remain excluded from Lambda artifact builds.

## Shared Lambda Layer Contract

The codebase uses several shared internal packages under `python/`. These should be packaged once into a shared Lambda Layer rather than duplicated into every handler artifact.

The shared Lambda Layer is the default home for:

- `margana_score`
- `margana_metrics`
- `margana_costing`
- pinned third-party runtime dependencies required by deployed Lambdas
- runtime data files that those shared packages load at runtime

The shared Lambda Layer should not include:

- `python/lambdas/`
- `python/ecs/`
- `python/tests/`
- handler-specific source files from `python/lambdas/`
- local fixtures from `resources/`
- email templates from `postmarkTemplates/`

Current shared layer contents:

| Item | Layer rule | Notes |
| --- | --- | --- |
| `margana_costing` | Include in shared layer | Used by many API and scheduled Lambdas for DynamoDB capacity logging |
| `margana_metrics` | Include in shared layer | Required by metrics, dashboard, leaderboard, and weekly seeder flows |
| `margana_score` | Include in shared layer | Required by submission scoring flows and bundled runtime word-list access |
| `margana_gen` | Exclude from Lambda layer by default | Reserved primarily for ECS/job workflows under `python/ecs/` unless a Lambda explicitly imports it later |
| `python/margana_score/data/margana-word-list.txt` | Include in shared layer | Canonical build-time asset fetched by CI and bundled for runtime word-list loading |
| `python/margana_metrics/badge-milestones.json` | Include in shared layer | Runtime config loaded by `margana_metrics.metrics_service` |
| Third-party runtime dependencies | Include in shared layer | Prefer vendoring pinned runtime dependencies into the layer for deterministic Lambda behavior |

Layer packaging notes:

- The layer should expose its Python content using the standard Lambda Python layer layout under `python/`.
- CI-fetched canonical runtime assets must remain out of Git where they are intended to be supplied during the build.
- Test fixtures under `python/tests/resources/` must stay out of the shared layer.

## Thin Handler Zip Contract

Each deployable Lambda `.zip` should be thin and contain only:

- the specific handler source file for that Lambda,
- any Lambda-specific bootstrap or wrapper file if needed for handler naming,
- no duplicated shared packages that already live in the shared layer.

The thin handler zips should not contain:

- the other Lambda handler files,
- `python/ecs/`,
- `python/tests/`,
- shared package trees already provided by the layer.

Current handler-to-layer expectations:

| Logical name | Thin zip contents | Shared layer dependency |
| --- | --- | --- |
| `process-margana-results` | handler file only | `margana_costing` |
| `athena-events-parquet-runner` | handler file only | third-party runtime deps only |
| `authorizer` | handler file only | no current internal shared package imports |
| `cognito-delete-user-audit` | handler file only | no current internal shared package imports |
| `dashboard-summary` | handler file only | `margana_metrics`, `margana_costing` |
| `leaderboard-service` | handler file only | `margana_costing` |
| `leaderboard-snapshot` | handler file only | `margana_costing` |
| `margana-metric` | handler file only | `margana_metrics`, `margana_costing` |
| `post-confirmation-action` | handler file only | third-party runtime deps only |
| `pre-auth-approval` | handler file only | third-party runtime deps only |
| `profile-service` | handler file only | `margana_costing` |
| `send-friend-invite` | handler file only | `margana_costing` |
| `submission` | handler file only | `margana_score`, `margana_metrics` |
| `terms-audit` | handler file only | `margana_costing` |
| `user-settings` | handler file only | `margana_costing` |
| `weekly-seeder` | handler file only | `margana_metrics`, `margana_costing` |
| `ses-sns-events-notification` | handler file only | no current internal shared package imports |

## Artifact Naming Convention

Each deployable Lambda produces one thin `.zip` artifact, and CI also produces one shared layer `.zip` artifact.

Artifact filename format:

`<logical-name>__<git-sha>.zip`

Examples:

- `submission__abc1234.zip`
- `leaderboard-service__abc1234.zip`
- `weekly-seeder__abc1234.zip`

Shared layer filename format:

`shared-python-deps-layer__<git-sha>.zip`

Example:

- `shared-python-deps-layer__abc1234.zip`

Recommended S3 key format for the Build Artifacts bucket:

`backend/<environment>/<git-sha>/<artifact-name>`

Examples:

- `backend/preprod/abc1234/submission__abc1234.zip`
- `backend/prod/abc1234/leaderboard-service__abc1234.zip`
- `backend/preprod/abc1234/shared-python-deps-layer__abc1234.zip`

This naming scheme keeps artifacts:

- unique per commit,
- predictable for CI/CD outputs,
- stable enough for `margana-infra` to consume in Phase 2.3.

## Packaging Notes

- The logical deployment name is the stable identifier for CI/CD and infrastructure references.
- The source filename does not need to match the logical name exactly.
- The shared layer should be versioned and published alongside the thin handler zips so infrastructure can attach a matching layer version to each Lambda deployment.
- `python/lambdas/lambda-process-margana-results.py` should keep the logical name `process-margana-results`, but the eventual packaging workflow should normalize how the handler module is addressed because hyphenated Python filenames are awkward for import-based handler references.
- If a new deployable Lambda is added under `python/lambdas/`, this inventory must be updated in the same change.
