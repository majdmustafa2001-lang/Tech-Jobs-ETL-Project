# Tech Jobs ETL Project Engineering Rules

## Current repository state

- This Azure Databricks portfolio project supports hiring and skills trend analysis from Hacker News and comparisons of jobs, companies, and role categories across selected Greenhouse boards. Both sources have limited coverage. Greenhouse history starts with the snapshots collected in October 2026.
- **Hacker News** uses Python, PySpark, and SQL in notebooks. Auto Loader ingests historical Parquet and API JSON into Bronze Delta tables. Silver cleans and quarantines records, enriches API posts, and unifies both sources by `post_id`, preferring the historical record on overlap. Gold rebuilds yearly hiring and skill aggregates for the dashboard. The API workflow uses `new_posts` to skip downstream tasks when there are no new posts.
- **Greenhouse** uses a Python notebook to fetch full snapshots per configured board and land timestamped JSON in an ADLS Gen2-backed Unity Catalog volume. In `greenhouse_sdp/transformations/`, Lakeflow Spark Declarative Pipelines (SDP) incrementally ingest new files with Auto Loader, flatten snapshots, and apply quality checks, quarantine, and title-based classification. `AUTO CDC` reads flattened observations before classification and tracks selected attributes using `(board_token, job_id)` and `snapshot_at`. The Gold Tech and Other materialized views join the open SCD Type 2 version to the latest classification and exclude quarantined jobs. These views represent the last known state. Job removal detection is a known gap.
- `resources/` contains workspace-specific Lakeflow Job YAML exports. Verify notebook paths, task dependencies, and the target workspace revision when using an export. The Greenhouse export runs extraction on an existing Classic cluster before the triggered SDP pipeline described as Serverless in the README.
- Validation evidence in this checkout consists of notebook checks and development observations documented in `README.md`, including Delta Change Data Feed (CDF) and Time Travel checks. The checkout has no standalone automated test suite or portable deployment definition.

## Architecture and business value

- Define the business problem, intended users, expected benefit, and success criteria before choosing a technology or changing a data product. State source coverage, freshness, and metric limits where they affect interpretation.
- Preserve the existing medallion responsibilities. Bronze retains source data and provenance, Silver owns validation and conformed records, and Gold serves defined analytical grain and metrics. Trace changes from the source and orchestration through Bronze, Silver, Gold, and dashboard consumers. Verify assumptions against the notebooks, SQL, and YAML.
- Record consequential decisions in an Architecture Decision Record (ADR). Include the context, options, decision, trade-offs, and consequences. Cover changes to dataset contracts, pipeline boundaries, or state management when they affect consumers or recovery.

## Data engineering

- Use the existing Python, PySpark, SQL, Delta Lake, and Lakeflow SDP patterns where they fit. Reuse suitable helpers and prefer simple transformations that preserve existing contracts.
- For incremental or stateful changes, define the input and output grain, business keys, ordering, and replay behavior. Verify idempotency for reruns, retries, and backfills, including partial failure and concurrent execution where relevant. For SCD Type 2 changes, check duplicate and out-of-order observations, unchanged tracked attributes, and one open version per key. Keep publication, source update, snapshot, and ingestion timestamps distinct.
- Define data quality checks at the affected layer. Validate required fields, keys, uniqueness, and source completeness as applicable. Make quarantine reasons and rescued fields observable, and reconcile counts across layers at the intended grain. Use bounded retries for transient errors and surface actionable failures. Assess incomplete extracts before publishing downstream results.
- Before risky schema evolution or checkpoint changes, document compatibility with existing Delta data and consumers, effects on replay and streaming state, the backfill scope, and the recovery and rollback plan. Treat checkpoint resets and full refreshes as explicit migrations requiring owner approval before execution.
- Run relevant checks for normal inputs and the affected failure or regression cases. Use existing notebook or SQL checks where appropriate and add focused tests where coverage is needed. For stateful changes, include replay or restart checks and verify downstream results before claiming validation.

## Databricks and security

- Use the existing OAuth configuration with `--profile DEFAULT` for Databricks CLI access. Keep diagnostic queries read-only and obtain any required approval for the specific command before executing it. Keep the profile and login configuration intact.
- Obtain the repository owner's explicit approval before any workspace mutation or compute execution. This covers creating, changing, or running jobs, pipeline updates, clusters, deployments, and changes to data, schemas, or checkpoints. Include the expected Serverless and Classic compute cost implications when proposing a run, refresh, or backfill.
- Keep secrets, tokens, OAuth material, and credentials out of Git. Sanitize logs and command output before sharing.

## Git, quality, and documentation

- Work on a feature branch and keep changes focused. Review the diff, run the relevant checks, and obtain review before merging. Obtain the repository owner's explicit approval before committing, pushing, or merging.
- Report implemented changes, executed checks and their results, and proposed work separately. Identify skipped or blocked checks. Document known weaknesses, technical debt, and trade-offs. Reserve production-ready claims for verified operational readiness. Describe production-like validation by its actual environment and scope.
- Use applicable repository skills. For failed Databricks jobs, tasks, or Lakeflow pipeline updates, follow [databricks-troubleshooter](.agents/skills/databricks-troubleshooter/SKILL.md) within the approved access scope.
