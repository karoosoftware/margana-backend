# Deployment Inventory

This document defines the current backend deployment inventory for Phase 2.2.1 of the repository split plan. It is the source of truth for:

- which Python entrypoints are deployable Lambda artifacts,
- which paths are excluded from Lambda packaging,
- which shared internal packages must be bundled,
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

Lambda packaging includes deployable handler files from `python/lambdas/` and their runtime dependencies.

The following paths are excluded from Lambda packaging:

- `python/ecs/`
- `python/tests/`
- `resources/`
- `postmarkTemplates/`
- local-only utilities, experiments, and one-off admin scripts outside `python/lambdas/`

`python/ecs/` is reserved for future puzzle-generation job/container entrypoints and must remain excluded from Lambda artifact builds.

## Shared Internal Package Bundling Rules

The codebase uses several internal packages under `python/`. Packaging must include the handler file plus the internal packages required by that handler.

Packages that may be required by deployable Lambdas:

- `margana_score`
- `margana_gen`
- `margana_metrics`
- `margana_costing`

Current expected usage by package:

| Package | Bundle rule | Notes |
| --- | --- | --- |
| `margana_costing` | Bundle when the handler imports it | Used by many API and scheduled Lambdas for DynamoDB capacity logging |
| `margana_metrics` | Bundle when the handler imports it | Required by metrics, dashboard, leaderboard, and weekly seeder flows |
| `margana_score` | Bundle when the handler imports it | Required by submission scoring flows and any handler using bundled word-list access |
| `margana_gen` | Do not bundle by default | Reserved primarily for ECS/job workflows under `python/ecs/` unless a Lambda explicitly imports it later |

Runtime assets:

- Bundle `python/margana_score/data/margana-word-list.txt` with any artifact that includes `margana_score` runtime word-list loading.
- Keep test fixtures under `python/tests/resources/` out of deployable artifacts.
- Keep CI-fetched canonical assets out of Git where Phase 2.2.3 expects them to be supplied by the workflow.

## Artifact Naming Convention

Each deployable Lambda produces one `.zip` artifact.

Artifact filename format:

`<logical-name>__<git-sha>.zip`

Examples:

- `submission__abc1234.zip`
- `leaderboard-service__abc1234.zip`
- `weekly-seeder__abc1234.zip`

Recommended S3 key format for the Build Artifacts bucket:

`backend/<environment>/<git-sha>/<artifact-name>`

Examples:

- `backend/preprod/abc1234/submission__abc1234.zip`
- `backend/prod/abc1234/leaderboard-service__abc1234.zip`

This naming scheme keeps artifacts:

- unique per commit,
- predictable for CI/CD outputs,
- stable enough for `margana-infra` to consume in Phase 2.3.

## Packaging Notes

- The logical deployment name is the stable identifier for CI/CD and infrastructure references.
- The source filename does not need to match the logical name exactly.
- `python/lambdas/lambda-process-margana-results.py` should keep the logical name `process-margana-results`, but the eventual packaging workflow should normalize how the handler module is addressed because hyphenated Python filenames are awkward for import-based handler references.
- If a new deployable Lambda is added under `python/lambdas/`, this inventory must be updated in the same change.
