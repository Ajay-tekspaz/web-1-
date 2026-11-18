# Working rules for AI assistants and people (daily-expense-tracker-fintech-application)

These rules apply to every AI assistant (Claude Code, Copilot, Cursor, ...) and every contributor. The owner is @Giveson-Tekspaz.

## Safety
- **Dry-run by default.** Anything that changes a system (Snowflake, Databricks, Airflow, cloud, Vercel) defaults to a dry run and needs an explicit flag to apply.
- **Never touch production or a customer account** without the owner's written sign-off. Use `DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX`, the mock datasets, or local services.
- **No secrets in the repo, logs, issues or PRs.** Credentials come from environment variables; never paste or print them.

## Code
- Tests ship with the code. A change without a test for its behaviour is not done.
- This is the reference lakehouse project: use synthetic or masked data only, never real customer data.
- Use the shared schemas and specs in this repo; do not invent a second format. Anything ported between repos needs golden vectors (versioned JSON files).
- Keep PRs small (one task, one issue). Reference the issue (`Closes #n`).
- You must be able to explain every line you submit, whoever or whatever wrote it.

## Workflow
- Never push straight to `main`. Branch, open a PR, wait for green CI and the CODEOWNERS review.
- Do not regenerate shared datasets or change shared schemas inside an unrelated PR.
- Do not delete or rewrite someone else's branch or issue.
