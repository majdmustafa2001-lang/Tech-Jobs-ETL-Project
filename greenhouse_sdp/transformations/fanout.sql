
-- 1. Gültige technische Stellenanzeigen

CREATE OR REFRESH MATERIALIZED VIEW
job_market.gold.greenhouse_current_tech_jobs
AS

WITH latest_classification AS (
    SELECT
        board_token,
        job_id,
        is_tech,
        is_quarantined,
        role_family,

        ROW_NUMBER() OVER (
            PARTITION BY board_token, job_id
            ORDER BY snapshot_at DESC, snapshot_id DESC
        ) AS rn

    FROM job_market.silver.greenhouse_jobs_classified
)

SELECT
    scd.board_token,
    scd.job_id,
    scd.title,
    job.role_family,
    scd.location_raw,
    scd.offices,
    scd.__START_AT AS valid_from

FROM job_market.silver.greenhouse_jobs_scd2 scd

INNER JOIN latest_classification job
    ON scd.board_token = job.board_token
   AND scd.job_id = job.job_id

WHERE scd.__END_AT IS NULL
  AND job.rn = 1
  AND job.is_tech = TRUE
  AND job.is_quarantined = FALSE;


-- 2. Gültige nichttechnische Stellenanzeigen

CREATE OR REFRESH MATERIALIZED VIEW
job_market.gold.greenhouse_current_other_jobs
AS

WITH latest_classification AS (
    SELECT
        board_token,
        job_id,
        is_tech,
        is_quarantined,
        role_family,

        ROW_NUMBER() OVER (
            PARTITION BY board_token, job_id
            ORDER BY snapshot_at DESC, snapshot_id DESC
        ) AS rn

    FROM job_market.silver.greenhouse_jobs_classified
)

SELECT
    scd.board_token,
    scd.job_id,
    scd.title,
    job.role_family,
    scd.location_raw,
    scd.offices,
    scd.__START_AT AS valid_from

FROM job_market.silver.greenhouse_jobs_scd2 scd

INNER JOIN latest_classification job
    ON scd.board_token = job.board_token
   AND scd.job_id = job.job_id

WHERE scd.__END_AT IS NULL
  AND job.rn = 1
  AND job.is_tech = FALSE
  AND job.is_quarantined = FALSE;


-- 3. Fehlerhafte Stellenanzeigen
CREATE OR REFRESH STREAMING TABLE job_market.silver.greenhouse_quarantine
AS
SELECT *
FROM STREAM(job_market.silver.greenhouse_jobs_classified)
WHERE is_quarantined = true;
