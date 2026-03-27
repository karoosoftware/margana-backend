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

The repository will be structured to support both the existing Lambda-based backend and the new ECS-based generation service.

```text
.
├── ecs/                            # ECS Container & Job Logic
│   ├── main.py                     # Entrypoint script for the ECS Task
│   ├── generator_wrapper.py        # Logic to invoke the generation scripts
│   ├── requirements.txt            # ECS-specific dependencies
│   ├── Dockerfile                  # Container definition
│   ├── generate-column-puzzle.py   # Existing generator script
│   └── generate-column-puzzle-madness.py # Existing madness generator script
├── lambdas/                        # Existing Lambda handlers
├── layer-root/                     # Shared Python Logic (Lambda Layer / Package)
│   └── python/
│       ├── margana_gen/            # Puzzle generation utilities
│       │   ├── puzzle_gen.py
│       │   ├── s3_utils.py         # S3 download/upload helpers
│       │   └── usage_log.py        # Usage tracking logic
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
    *   Invokes the generation logic (refactored from `ecs/generate-column-puzzle.py`).
    *   Validates output using `margana_score` logic to ensure grids are solvable.
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

### 6. Next Steps / Adjustments

*   [ ] Define the exact S3 prefix structure for weekly puzzles.
*   [ ] Specify validation criteria (e.g., minimum word count, no forbidden words).
*   [ ] Determine if the task should accept CLI arguments for target week/force flags.
*   [ ] Define the structure of the success/failure email report.
