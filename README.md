# Tech Jobs ETL Project

End-to-end Data Engineering project analyzing the evolution of tech job postings from 2011 to 2026 using Azure Databricks, PySpark, Delta Lake, and ADLS Gen2.

## Dashboard Preview

### Hiring Posts & Remote Work Trends

![Hiring Posts and Remote Trends](dashboard-hiring-remote.png)

### Technology Trends (2019)

![Technology Trends](dashboard-skills-2019.png)

## Tech Stack

- Azure Databricks
- Apache Spark / PySpark
- Delta Lake
- Azure Data Lake Storage Gen2
- Databricks Workflows
- Git & GitHub

## Data Pipeline

The project follows the Medallion Architecture:

- **Landing:** Raw source files.
- **Bronze:** Data ingestion from historical datasets and APIs.
- **Silver:** Data cleaning, quality checks, standardization, and enrichment.
- **Gold:** Aggregated data for analytics and dashboards.

## Data Sources

Hacker News historical hiring data and Hacker News API.

## Project Status

In development — pipeline orchestration, testing, and CI/CD are planned.

*Note: Hacker News hiring posts are not representative of the entire technology job market.*
