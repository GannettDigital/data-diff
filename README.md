<p align="center">
  <img alt="data-diff logo" src="docs/data-diff-logo.png" width="280" />
</p>

<h1 align="center">data-diff</h1>

<p align="center">Compare datasets fast, within or across SQL databases.</p>

`data-diff` was originally developed by Datafold. This repository is an improved fork maintained by the Gannett / USA TODAY LOCALiQ Data Engineering team.

# What's a Data Diff?

A data diff is a value-level comparison between two tables. It helps identify critical data changes, validate transformations, and verify that data moved between systems still matches expectations.

You can use `data-diff` to compare development or staging data to production, validate migrations, or investigate discrepancies between systems without pulling entire tables into application memory.

# Use Cases

### Data Development Testing

When developing SQL code, `data-diff` helps validate and preview changes by comparing data between development or staging environments and production.

1. Make a change to your SQL code.
2. Run the SQL code to create a new dataset.
3. Compare that dataset with its production version or another revision.

### Data Migration & Replication Testing

`data-diff` is useful when moving data between systems. Typical examples include:

- Migrating to a new data warehouse, such as Oracle to Snowflake.
- Validating SQL transformations when replacing stored procedures with dbt.
- Continuously replicating data from OLTP systems to OLAP warehouses, such as MySQL to Redshift.

# dbt Integration

`data-diff` integrates with [dbt Core](https://github.com/dbt-labs/dbt-core) to compare local development datasets to production datasets.

Useful references:

- [Archived dbt workflow guide](https://web.archive.org/web/20240621071607/https://docs.datafold.com/development_testing/cli/)
- [GitHub discussions](https://github.com/GannettDigital/data-diff/discussions)

# Getting Started

### Validating dbt model changes between dev and prod

If you are using `data-diff` with dbt, start with the archived dbt workflow guide:

- [data-diff + dbt documentation](https://web.archive.org/web/20240621071607/https://docs.datafold.com/development_testing/cli/)

### Compare data tables between databases

1. Install `data-diff` with adapters.

To compare data between databases, install `data-diff` with the database adapters you need. For example, for PostgreSQL and Snowflake:

```bash
pip install data-diff 'data-diff[postgresql,snowflake]' -U
```

To install all open source supported adapters:

```bash
pip install data-diff 'data-diff[all-dbs]' -U
```

2. Run `data-diff` with connection URIs.

The example below compares PostgreSQL and Snowflake using the hashdiff algorithm:

```bash
data-diff \
  postgresql://<username>:'<password>'@localhost:5432/<database> \
  <table> \
  "snowflake://<username>:<password>@<account>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>" \
  <TABLE> \
  -k <primary_key_column> \
  -c <columns_to_compare> \
  -w <filter_condition>
```

3. Set up a configuration file.

You can also define databases and runs in a `toml` config file. This example compares MotherDuck and Snowflake using hashdiff:

```toml
## DATABASE CONNECTION ##
[database.duckdb_connection]
  driver = "duckdb"
  # filepath = "demo.duckdb" # local duckdb file example
  # filepath = "md:" # default motherduck connection example
  filepath = "md:demo?motherduck_token=${MOTHERDUCK_TOKEN}" # token recommended for MotherDuck

[database.snowflake_connection]
  driver = "snowflake"
  database = "DEV"
  user = "${SNOWFLAKE_USER}"
  password = "${SNOWFLAKE_PASSWORD}"
  account = "${SNOWFLAKE_ACCOUNT}"
  schema = "DEVELOPMENT"
  warehouse = "DEMO"
  role = "DEMO_ROLE"

## RUN PARAMETERS ##
[run.default]
  verbose = true

## EXAMPLE DATA DIFF JOB ##
[run.demo_xdb_diff]
  1.database = "duckdb_connection"
  1.table = "development.raw_orders"

  2.database = "snowflake_connection"
  2.table = "RAW_ORDERS"

  verbose = false
```

4. Run the configured job.

Export any required environment variables and then run the configured diff:

```bash
export MOTHERDUCK_TOKEN=<MOTHERDUCK_TOKEN>

data-diff --conf datadiff.toml \
  --run demo_xdb_diff \
  -k "id" \
  -c status

# output example
- 1, completed
+ 1, returned
```

5. Review the output.

Review the diff output to identify and analyze data changes.

Additional references:

- [Archived CLI reference](https://web.archive.org/web/20240525044139/https://docs.datafold.com/reference/open_source/cli/)
- [Technical explanation](docs/technical-explanation.md)
- [Python API reference](docs/python-api.rst)
- [Python examples](docs/python_examples.rst)

# Supported Databases

| Database | Status | Connection string |
| --- | --- | --- |
| PostgreSQL >=10 | 🟢 | `postgresql://<user>:<password>@<host>:5432/<database>` |
| MySQL | 🟢 | `mysql://<user>:<password>@<hostname>:5432/<database>` |
| Snowflake | 🟢 | `"snowflake://<user>[:<password>]@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<role>[&authenticator=externalbrowser]"` |
| BigQuery | 🟢 | `bigquery://<project>/<dataset>` |
| Redshift | 🟢 | `redshift://<username>:<password>@<hostname>:5439/<database>` |
| DuckDB | 🟢 | `duckdb://<filepath>` |
| MotherDuck | 🟢 | `duckdb://<filepath>` |
| Microsoft SQL Server* | 🟢 | `mssql://<user>:<password>@<host>/<database>/<schema>` |
| Oracle | 🟡 | `oracle://<username>:<password>@<hostname>/<service_or_sid>` |
| Presto | 🟡 | `presto://<username>:<password>@<hostname>:8080/<database>` |
| Databricks | 🟡 | `databricks://<http_path>:<access_token>@<server_hostname>/<catalog>/<schema>` |
| Trino | 🟡 | `trino://<username>:<password>@<hostname>:8080/<database>` |
| Clickhouse | 🟡 | `clickhouse://<username>:<password>@<hostname>:9000/<database>` |
| Vertica | 🟡 | `vertica://<username>:<password>@<hostname>:5433/<database>` |

*Microsoft SQL Server support is limited and has known performance issues.

* 🟢: Implemented and thoroughly tested.
* 🟡: Implemented, but not thoroughly tested yet.

Vertica support remains available in code, but the default CI and local Docker stack do not currently provision a working Vertica instance. To test Vertica, supply your own connection via `DATADIFF_VERTICA_URI`.

Your database not listed here?

- Contribute a [new database adapter](docs/new-database-driver-guide.rst).
- Open an [issue](https://github.com/GannettDigital/data-diff/issues) to discuss support.

# How It Works

`data-diff` efficiently compares data using two modes:

**joindiff**: Ideal for comparing data within the same database, using outer joins for efficient row comparison. It relies on the database engine for computation and has consistent performance.

**hashdiff**: Recommended for comparing datasets across different databases or large tables with minimal differences. It uses hashing and binary search and can diff data across distinct database engines.

<details>
<summary>Click here to learn more about joindiff and hashdiff</summary>

### `joindiff`

- Recommended for comparing data within the same database.
- Uses outer joins to diff rows efficiently.
- Fully relies on the underlying database engine for computation.
- Requires both datasets to be queryable with a single SQL query.
- Time complexity approximates a `JOIN` operation and is largely independent of the number of differences.

### `hashdiff`

- Recommended for comparing datasets across different databases.
- Also useful for very large tables with few expected differences in the same database.
- Uses a divide-and-conquer algorithm based on hashing and binary search.
- Can diff data across distinct database engines, such as PostgreSQL and Snowflake.
- Time complexity approximates `COUNT(*)` when there are few differences.
- Performance degrades when datasets have a large number of differences.

</details>

For more detail, see the [technical explanation](docs/technical-explanation.md). The original hosted explainer is also available in the [Internet Archive](https://web.archive.org/web/20240804072404/https://docs.datafold.com/data_diff/how-datafold-diffs-data/).

## Contributors

We thank everyone who has contributed so far.

Contributing guide: [CONTRIBUTING.md](CONTRIBUTING.md)

<a href="https://github.com/GannettDigital/data-diff/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=GannettDigital/data-diff" />
</a>

## Analytics

- [Usage Analytics & Data Privacy](docs/usage_analytics.md)

## License

This project is licensed under the terms of the [MIT License](LICENSE).
