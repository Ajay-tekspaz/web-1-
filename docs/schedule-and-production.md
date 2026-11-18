# Schedule and production

## Nightly dev build

`.github/workflows/dbt-scheduled.yml` runs the Snowflake dev build (`snowflake-build.yml`) at 02:00 UTC every day. It does nothing until the repository variable `SNOWFLAKE_SCHEDULE` is `on`. The Tekspaz platform sets it from "Run nightly" on the project's Deployments page; you can set it by hand under Settings > Secrets and variables > Actions > Variables. GitHub runs scheduled workflows from the default branch, so it starts after the project's pull request is merged. It is dev only: nothing schedules production.

## Production gate

`.github/workflows/dbt-production.yml` exists so production has a defined, reviewable path, and it cannot run until all four locks are opened:

| Lock | Who opens it |
|---|---|
| Repository variable `PRODUCTION_ENABLED` = `true` | the owner, after written sign-off |
| A person starts the run and types `PRODUCTION` | whoever deploys |
| GitHub environment `production` has required reviewers | the owner (Settings > Environments) |
| `production` environment secrets and variables (`SNOWFLAKE_ACCOUNT`, `_USER`, `_PRIVATE_KEY`, `SNOWFLAKE_ROLE`, `_DATABASE`, `_WAREHOUSE`) | the owner; the Tekspaz platform never writes them |

A run is a dry run (`dbt compile`) unless `apply` is ticked, and it refuses a database named like dev. The `prod` target in `dbt/profiles.yml` has no defaults, so a missing value fails the run instead of falling back to dev. `tests/test_workflows.py` fails if any other workflow targets production or if the gate gains a schedule or push trigger.
