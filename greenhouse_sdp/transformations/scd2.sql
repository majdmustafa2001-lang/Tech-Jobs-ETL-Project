
-- 1. SCD Type 2 target
CREATE OR REFRESH STREAMING TABLE
job_market.silver.greenhouse_jobs_scd2
COMMENT 'Historical job versions from Greenhouse';


-- 2. Process incoming job observations
CREATE FLOW greenhouse_scd2_flow AS
AUTO CDC INTO
    job_market.silver.greenhouse_jobs_scd2

FROM STREAM(
    job_market.silver.greenhouse_jobs_snapshots
)

KEYS (board_token, job_id)

SEQUENCE BY snapshot_at

COLUMNS
    board_token,
    job_id,
    title,
    location_raw,
    offices,
    departments,
    job_metadata

STORED AS SCD TYPE 2

TRACK HISTORY ON
    title,
    location_raw,
    offices,
    departments,
    job_metadata;
