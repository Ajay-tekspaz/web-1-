FROM apache/superset:4.1.1

USER root
# DuckDB (local stand-in) and Snowflake drivers; duckdb must match the version that wrote the file
RUN pip install --no-cache-dir "duckdb~=1.1.0" "duckdb-engine==0.13.6" "snowflake-sqlalchemy~=1.6"
USER superset
