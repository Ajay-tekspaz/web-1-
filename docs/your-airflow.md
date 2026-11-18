# Run the pipeline on your own Airflow

The pipeline runs once a day on one of three runners. You choose which in Tekspaz, under TeksInfra › Deployments:

| Runner | Where it runs | What you set up |
|---|---|---|
| GitHub Actions nightly | GitHub's machines | Nothing |
| Tekspaz's Airflow | Tekspaz's hosted Airflow | Nothing |
| **Your Airflow** | Your Airflow 2.4+ or 3 (self-hosted, Astronomer, Cloud Composer, MWAA) | The steps below |

Only one of them runs a given environment: turning one on turns the others off.

## What your Airflow runs

`airflow/dags/tekspaz_daily.py` defines two DAGs, named after the project (`name` in `pyproject.toml`):

| DAG | Steps |
|---|---|
| `tekspaz__<project>__dev` | `raw_data` (your Airbyte, your Postgres, or the sample data) → `dbt_build` → `forecast` |
| `tekspaz__<project>__production` | `dbt_build`. A database that looks like dev (`*_DEV`) is refused. |

These are the same steps as the GitHub Actions workflows. The file needs nothing but Airflow itself: no providers and no private packages.

Each step works like this:
- It runs in a fresh copy of the repository, which is removed afterwards.
- The project's own packages (`requirements.txt`: dbt, pandas and the rest) go into a virtualenv of their own. Each worker builds it once per version of `requirements.txt`, under `$TEKSPAZ_CACHE_DIR` (default: the system temp folder plus `/tekspaz`). The first run on a worker takes longer: installing dbt and the data packages took about 14 minutes in a test on Airflow 2.10.5.
- Project code sees only its own settings. Your Airflow's environment (its database URL, its secrets) isn't passed to it. Proxy and certificate settings (`HTTPS_PROXY`, `REQUESTS_CA_BUNDLE`, `PIP_INDEX_URL`, …) are.

## Set it up

1. **Deploy the repository to your Airflow**, the whole repository rather than just `airflow/dags`:
   - git-sync or a mounted checkout: point it at the repository. The DAG finds the repository two folders above itself.
   - Astronomer or your own image: copy the repository into the image. If it isn't at `<dags folder>/../..`, set `TEKSPAZ_PROJECT_DIR` to where it is.
   - Cloud Composer or MWAA: upload the repository into the DAGs bucket, under `dags/<project>/`. Install nothing extra; the steps build their own virtualenv. The workers need Python 3.10 or newer and internet access to PyPI (or your `PIP_INDEX_URL`).

   The repository's `.airflowignore` makes Airflow load only `tekspaz_daily.py` from it, so the template's demo DAG and its tests don't show up as import errors. This was checked on Airflow 2.10.5 and 3.3.2.
2. **Connect your Airflow in Tekspaz** (Connectors › Airflow), with a user that has the **Op** role. Tekspaz needs to write two Variables, unpause the DAG and start runs. A Viewer user is enough for watching, but not for this.
3. **Turn it on:** TeksInfra › Deployments › Snowflake (dev) or Production › **Run on your Airflow**. Tekspaz then:
   - checks the DAG is there (Airflow can take a few minutes to list a new file);
   - writes the settings into the Variable `tekspaz__<project>__<env>__secret`: the Snowflake key pair and, for dev, your Airbyte or Postgres source. Airflow hides values of Variables named like this, in the UI and in logs, and each value is masked in the task log as well;
   - writes the time into `tekspaz__<project>__<env>__schedule` (cron, UTC);
   - unpauses the DAG, and turns off the GitHub Actions nightly and Tekspaz's Airflow for that environment.

Turning it off pauses the DAG and deletes the settings Variable. If you change the Snowflake connector or the raw data, press **Send the settings again**.

## Check it

- **Run now**, on the same panel, starts a run. Its result shows on the panel and in TeksInfra › Pipelines.
- In Airflow: the DAG's runs, and each step's log. `raw_data` logs the rows it loaded, and the dbt steps log dbt's own output.
- If a step says the Variable isn't set, turn **Run on your Airflow** on again: it writes the Variable.
