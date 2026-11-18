# Run the dbt part on your own dbt Cloud

The pipeline's dbt steps run with dbt Core by default, wherever the pipeline runs: GitHub Actions, Tekspaz's Airflow or your own Airflow. If you run dbt in **dbt Cloud**, one of your dbt Cloud jobs can do them instead. The rest of the pipeline is unchanged: the raw data and the forecast still run in the pipeline, and only the dbt commands go to dbt Cloud.

| Step | dbt Core (default) | Your dbt Cloud job |
|---|---|---|
| dbt build (staging + marts) | `dbt build --exclude tag:forecast`, here | the same command, as a run of your job |
| forecast | Python here, then `dbt build --select tag:forecast` here | Python here, then that command as a run of your job |
| production | `dbt build` (or `dbt compile` for a dry run), here | the same, as a run of your **production** job |

[`python/loaders/dbt_cloud_run.py`](../python/loaders/dbt_cloud_run.py) handles each dbt Cloud step:
- It starts your job with the step's command (dbt Cloud's `steps_override`) and waits for the run.
- If the run fails, the pipeline fails, and the log shows which step failed and the end of its dbt output.
- It saves the run's `run_results.json` to `dbt/target/`, the same place dbt Core writes it, so the checks that read it keep working.

**Branches:**
- **Dev:** the dev build runs your job on the branch being built (`git_branch`), so dbt Cloud builds the same code as the pipeline.
- **Production:** never overrides the branch. Your production job runs what its dbt Cloud environment is set to, which should be the default branch. A change still reaches production only through a reviewed `dbt Production` run.

## Set it up in dbt Cloud

1. **A project on this repository.** In dbt Cloud, create a project connected to this GitHub repository, with **project subdirectory `dbt`**.
2. **A Snowflake connection:**
   - **Dev:** the project's dev database (`DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV`), with the same role and warehouse as the Snowflake connector in Tekspaz.
   - **Production:** the production database, in a separate deployment environment.
   - **Schema:** `gold`.
3. **Deployment environments.** One for dev, plus one for production if you use it. Use dbt **1.8** or "Latest".
4. **A job in each environment.** Name it, for example, "Tekspaz pipeline (dev)". It needs no schedule and its commands don't matter: the pipeline gives each run its own command.
5. **A service token.** Under Account settings › Service tokens, create a token with the **Job Runner** permission (or Job Admin) on the project. The Job Viewer permission is enough for watching, but not for running.

## Connect it in Tekspaz

1. **Connectors › dbt Cloud:** the account ID (the number in your account settings URL), the service token, and the dbt Cloud address if yours isn't `cloud.getdbt.com` (for example `emea.dbt.com`, or your account's access URL such as `ab123.us1.dbt.com`).
2. **Project › TeksInfra › Deployments:**
   - Under **Snowflake (dev) › dbt runs on**, choose your dev job. Then press **Build again on Snowflake**.
   - Under **Production › dbt runs on**, choose your production job. It applies from the next production build.

Tekspaz writes `DBT_CLOUD_ACCOUNT_ID`, `DBT_CLOUD_JOB_ID`, `DBT_CLOUD_TOKEN` and `DBT_CLOUD_HOST` as secrets of the repository's `dev` (or `production`) environment. Both Airflow runners pass the same settings. Choosing **dbt Core** again removes them.

Check a job without running anything:

```bash
DBT_CLOUD_ACCOUNT_ID=… DBT_CLOUD_JOB_ID=… DBT_CLOUD_TOKEN=… python -m loaders.dbt_cloud_run --check
```
