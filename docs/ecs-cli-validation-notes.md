# ECS CLI Validation Notes

This document records the manual ECS setup and validation work completed so far for the Margana puzzle generator. The intent is to validate the AWS shape end-to-end with the AWS CLI first, then migrate the working setup into a shared Terraform module.

## Goal

Run the `margana-puzzle-generator` container as an EventBridge-triggered ECS Fargate task in preprod, with private networking and image storage in ECR.

## Completed So Far

### GitHub Actions

- Added `.github/workflows/backend-ecs.yml`.
- Scoped the workflow to:
  - branches: `develop`, `main`, `feature-*`
  - paths: `ecs/**`, `layer-root/python/**`, `.github/workflows/backend-ecs.yml`
- Verified GitHub OIDC role assumption with:
  - `aws-actions/configure-aws-credentials`
  - `aws sts get-caller-identity`
- Added ECR login in GitHub Actions.
- Successfully built and pushed the container image to ECR.

### Container

- Created the ECS container in [`ecs/Dockerfile`](/Users/paulbradbury/IdeaProjects/margana-backend/ecs/Dockerfile).
- Removed Docker `HEALTHCHECK` because this workload is a one-shot scheduled task, not a long-running service.
- Added a lightweight smoke test mode in [`ecs/main.py`](/Users/paulbradbury/IdeaProjects/margana-backend/ecs/main.py):
  - `python3 /app/ecs/main.py --smoke-test`
- Verified the container locally with:

```bash
docker run --rm margana-puzzle-generator --smoke-test
```

### IAM

- Verified GitHub Actions OIDC can assume the preprod backend role.
- Added ECR permissions needed for:
  - `ecr:GetAuthorizationToken`
  - image push actions on the `margana-preprod` repository
- Created ECS task trust policy for the task role using `ecs-tasks.amazonaws.com`.

### ECS / ECR

- Registered task definition family:
  - `margana-puzzle-generator`
- Verified task definition revisioning, for example:
  - `margana-puzzle-generator:1`
- Confirmed successful image push to ECR repository:
  - `margana-preprod`
- Created CloudWatch log group used by the ECS task:
  - `/ecs/margana-puzzle-generator`

### Networking

- ECS cluster:
  - `margana`
- VPC:
  - `vpc-02440e21b92afff6d`
- ECS task subnet:
  - `subnet-07a21be8c3ad7a2c6`
- ECS task security group:
  - `sg-061577e8cb4f419e8`
- Confirmed task SG already has outbound egress.
- Created VPC endpoint security group:
  - `sg-0a7016d94d2796436`
- Added ingress on the endpoint SG to allow HTTPS from the ECS task SG.
- Created interface VPC endpoints for:
  - `com.amazonaws.eu-west-2.ecr.api`
  - `com.amazonaws.eu-west-2.ecr.dkr`
  - `com.amazonaws.eu-west-2.logs`
- Created S3 gateway endpoint on route table:
  - `rtb-0e9c0106b1d1b225b`

## Manual Resources Created

The following resources now exist in AWS and should be considered for Terraform import or codification.

### Existing Resources Used

- ECS cluster:
  - `margana`
- VPC:
  - `vpc-02440e21b92afff6d`
- private subnet:
  - `subnet-07a21be8c3ad7a2c6`
- private route table:
  - `rtb-0e9c0106b1d1b225b`
- ECR repository:
  - `margana-preprod`
- ECS task execution role:
  - `arn:aws:iam::992468223519:role/ecsTaskExecutionRole`
- GitHub Actions preprod role used for OIDC/ECR push:
  - `margana-github-backend-preprod`

### Manually Created During Validation

- ECS task role:
  - `arn:aws:iam::992468223519:role/margana-puzzle-generator-task-role`
- ECS task definition family and revision:
  - `margana-puzzle-generator:1`
- ECS task security group:
  - `sg-061577e8cb4f419e8`
- VPC endpoint security group:
  - `sg-0a7016d94d2796436`
- Interface VPC endpoint:
  - `vpce-0e1b65d63a1404791` for `com.amazonaws.eu-west-2.ecr.api`
- Interface VPC endpoint:
  - `vpce-0a1cbfb1bc0884630` for `com.amazonaws.eu-west-2.ecr.dkr`
- Interface VPC endpoint:
  - CloudWatch Logs endpoint in `eu-west-2`
- Gateway VPC endpoint:
  - S3 endpoint attached to `rtb-0e9c0106b1d1b225b`
- CloudWatch log group:
  - `/ecs/margana-puzzle-generator`
- IAM managed policy for GitHub ECR access:
  - `arn:aws:iam::992468223519:policy/margana-github-backend-ecr-preprod`

### Task Definition Settings Validated

The current manually registered task definition shape is:

- family:
  - `margana-puzzle-generator`
- launch type:
  - `FARGATE`
- network mode:
  - `awsvpc`
- cpu:
  - `256`
- memory:
  - `512`
- execution role:
  - `arn:aws:iam::992468223519:role/ecsTaskExecutionRole`
- task role:
  - `arn:aws:iam::992468223519:role/margana-puzzle-generator-task-role`
- container name:
  - `margana-puzzle-generator`
- image:
  - `992468223519.dkr.ecr.eu-west-2.amazonaws.com/margana-preprod:latest`
- smoke test command:
  - `["--smoke-test"]`
- log group:
  - `/ecs/margana-puzzle-generator`
- log stream prefix:
  - `ecs`

## What We Learned

### Task Definitions Are Not Cluster-Bound

`aws ecs register-task-definition` only creates the task definition revision. The task definition is later run against a specific ECS cluster via `aws ecs run-task`.

### Fargate Task Networking Matters

The task runs in the specified subnet, not "inside the cluster" in a network sense. Because of that:

- the task needs outbound network access to reach ECR and CloudWatch Logs
- a private subnet with `assignPublicIp=DISABLED` requires either:
  - a NAT path, or
  - VPC endpoints for required AWS services

### VPC Endpoints Are the Correct Long-Term Model

For this internal scheduled workload, the preferred setup is:

- private subnet
- `assignPublicIp=DISABLED`
- VPC interface endpoints for AWS APIs used during startup/runtime
- S3 gateway endpoint if S3 access is required privately

### Private Image Pulls Need More Than ECR API Access

For private Fargate startup, successful image pull required:

- `ecr.api` interface endpoint
- `ecr.dkr` interface endpoint
- `logs` interface endpoint
- S3 gateway endpoint
- VPC DNS support enabled
- VPC DNS hostnames enabled
- endpoint SG ingress on TCP `443` from the ECS task SG

## Current Status

The smoke test has now been validated end-to-end.

Validated behavior:

- image build and push to ECR from GitHub Actions works
- ECS task can pull the image privately in the VPC
- CloudWatch Logs integration works
- the smoke test command runs successfully
- the task exits with exit code `0`

Validated ECS task evidence:

- stop reason was consistent with a one-shot container completing
- container exit code:
  - `0`
- CloudWatch Logs output:
  - `Starting Margana Puzzle Generator Task smoke test`
  - `Smoke test completed successfully.`

## Next Manual CLI Steps

1. Optionally test the real command path instead of `--smoke-test`.
2. Decide whether the task definition should continue to use `:latest` during manual testing, or move to immutable image tags.
3. Stop making further manual infrastructure changes and begin codifying the validated shape in Terraform.

## Terraform Migration Intention

Once the manual CLI path is proven, migrate this setup into a shared Terraform module covering:

- ECS cluster integration
- task definition
- task execution role
- task role
- CloudWatch log group
- ECR repository integration
- interface VPC endpoints
- S3 gateway endpoint
- security groups and rules
- environment-specific variables such as subnet IDs, repository names, and roles

The migration goal is not to redesign the working shape immediately. It is to codify the validated AWS configuration into reusable Terraform with minimal behavior change first.

## Terraform Import / Codification Candidates

The following should either be imported into Terraform state or recreated declaratively and then adopted carefully:

- ECS task role
- IAM policy for GitHub ECR access
- IAM role policy attachments related to ECR push
- ECS task definition
- CloudWatch log group
- ECS task security group
- VPC endpoint security group
- SG rules between task SG and endpoint SG
- ECR API interface endpoint
- ECR DKR interface endpoint
- CloudWatch Logs interface endpoint
- S3 gateway endpoint

Resources that may remain external inputs to the shared module, depending on your platform design:

- VPC
- subnets
- route tables
- ECS cluster
- existing ECR repository
- existing task execution role

Recommended Terraform approach:

1. Treat network primitives and shared platform resources as inputs where they already exist.
2. Put task-specific resources in the ECS module.
3. Import manually created resources first where practical, to avoid churn.
4. After import, make the Terraform code match the working AWS shape before attempting refactors such as immutable image promotion.
