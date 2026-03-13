# Deployment Inventory

This document defines the current backend deployment inventory for Phase 2.2.1 of the repository split plan. It is the source of truth for:

- which Python entrypoints are deployable Lambda artifacts,
- which paths are excluded from Lambda packaging,
- which shared code and assets belong in the Lambda layer,
- how Lambda artifacts are named for CI/CD and later Terraform integration.

## Deployable Lambda Entry Points

All deployable Lambda handlers currently live under `lambdas/`.

| Logical name | Source file | Handler symbol | Purpose / trigger |
| --- | --- | --- | --- |
| `process-margana-results` | `lambdas/lambda-process-margana-results.py` | `lambda_handler` | Processes Margana results and writes backend state |
| `athena-events-parquet-runner` | `lambdas/lambda_athena_events_parquet_runner.py` | `lambda_handler` | Runs an Athena named query workflow for parquet/event processing |
| `authorizer` | `lambdas/lambda_authorizer.py` | `lambda_handler` | API authorization for registered and guest users |
| `cognito-delete-user-audit` | `lambdas/lambda_cognito_delete_user_audit.py` | `lambda_handler` | Audits and cleans up backend state after Cognito user deletion |
| `dashboard-summary` | `lambdas/lambda_dashboard_summary.py` | `lambda_handler` | Returns dashboard summary API data |
| `leaderboard-service` | `lambdas/lambda_leaderboard_service.py` | `lambda_handler` | Serves leaderboard API requests |
| `leaderboard-snapshot` | `lambdas/lambda_leaderboard_snapshot.py` | `lambda_handler` | Builds weekly leaderboard snapshot records |
| `margana-metric` | `lambdas/lambda_margana_metric.py` | `lambda_handler` | Computes and returns metrics-related responses |
| `post-confirmation-action` | `lambdas/lambda_post_confirmation_action.py` | `lambda_handler` | Cognito post-confirmation trigger actions |
| `pre-auth-approval` | `lambdas/lambda_pre_auth_approval.py` | `lambda_handler` | Cognito pre-authentication approval gate |
| `profile-service` | `lambdas/lambda_profile_service.py` | `lambda_handler` | Serves profile API requests |
| `send-friend-invite` | `lambdas/lambda_send_friend_invite.py` | `lambda_handler` | Creates and sends friend/leaderboard invites |
| `submission` | `lambdas/lambda_submission.py` | `lambda_handler` | Scores live submissions and writes daily results |
| `terms-audit` | `lambdas/lambda_terms_audit.py` | `lambda_handler` | Records terms/audit events |
| `user-settings` | `lambdas/lambda_user_settings.py` | `lambda_handler` | Serves user settings API requests |
| `weekly-seeder` | `lambdas/process_weekly_seeder.py` | `lambda_handler` | Builds weekly seeder leaderboard data |
| `ses-sns-events-notification` | `lambdas/ses_sns_events_notification.py` | `lambda_handler` | Handles SES SNS notification events |

## Packaging Scope and Exclusions

Lambda packaging uses:

- one shared Lambda Layer artifact for common runtime code and assets,
- one thin `.zip` artifact per deployable Lambda handler.

The following paths are excluded from Lambda packaging:

- `ecs/`
- `tests/`
- `resources/`
- `postmarkTemplates/`
- local-only utilities, experiments, and one-off admin scripts outside `lambdas/`

`ecs/` is reserved for future puzzle-generation job/container entrypoints and must remain excluded from Lambda artifact builds.

## Shared Lambda Layer Contract

The codebase uses several shared internal packages under `layer-root/python/`. These should be packaged once into a shared Lambda Layer rather than duplicated into every handler artifact.

The shared Lambda Layer is the default home for:

- `margana_score`
- `margana_metrics`
- `margana_costing`
- pinned third-party runtime dependencies required by deployed Lambdas
- runtime data files that those shared packages load at runtime

The shared Lambda Layer should not include:

- `lambdas/`
- `ecs/`
- `tests/`
- handler-specific source files from `lambdas/`
- local fixtures from `resources/`
- email templates from `postmarkTemplates/`

Current shared layer contents:

| Item | Layer rule | Notes |
| --- | --- | --- |
| `margana_costing` | Include in shared layer | Used by many API and scheduled Lambdas for DynamoDB capacity logging |
| `margana_metrics` | Include in shared layer | Required by metrics, dashboard, leaderboard, and weekly seeder flows |
| `margana_score` | Include in shared layer | Required by submission scoring flows and bundled runtime word-list access |
| `margana_gen` | Exclude from Lambda layer by default | Reserved primarily for ECS/job workflows under `ecs/` unless a Lambda explicitly imports it later |
| `layer-root/python/margana_score/data/margana-word-list.txt` | Include in shared layer | Canonical build-time asset fetched by CI and bundled for runtime word-list loading |
| `layer-root/python/margana_metrics/badge-milestones.json` | Include in shared layer | Runtime config loaded by `margana_metrics.metrics_service` |
| Third-party runtime dependencies | Exclude from shared layer by default | Current CI copies only internal shared packages and relies on the AWS runtime for the SDK stack, matching the earlier monorepo approach |

Layer packaging notes:

- The layer should expose its Python content using the standard Lambda Python layer layout under `python/`.
- CI-fetched canonical runtime assets must remain out of Git where they are intended to be supplied during the build.
- Test fixtures under `tests/resources/` must stay out of the shared layer.
- Status (2026-03-13): implemented in CI for `preprod` using the repo-owned packaging script `packaging/build_lambda_artifacts.sh`.

## Thin Handler Zip Contract

Each deployable Lambda `.zip` should be thin and contain only:

- the specific handler source file for that Lambda,
- any Lambda-specific bootstrap or wrapper file if needed for handler naming,
- no duplicated shared packages that already live in the shared layer.

The thin handler zips should not contain:

- the other Lambda handler files,
- `ecs/`,
- `tests/`,
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

Thin handler packaging notes:

- Handler artifacts are now discovered dynamically from the `lambdas/` directory rather than from a hard-coded manifest in CI.
- The artifact logical name is derived from the filename by:
  - removing the `.py` suffix,
  - stripping a leading `lambda_` or `lambda-` prefix,
  - replacing `_` with `-`.
- Terraform remains the source of truth for which packaged handlers are actually deployed.

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
- `lambdas/lambda-process-margana-results.py` keeps the logical artifact name `process-margana-results`; Terraform continues to define the deployed handler string.
- If a new deployable Lambda is added under `lambdas/`, the deployment inventory and infrastructure mappings should be reviewed in the same change.
