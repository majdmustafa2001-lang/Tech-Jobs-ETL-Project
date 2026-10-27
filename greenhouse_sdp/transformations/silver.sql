
CREATE OR REFRESH STREAMING TABLE
job_market.silver.greenhouse_jobs_snapshots
(
    CONSTRAINT valid_job_id
        EXPECT (job_id IS NOT NULL AND job_id > 0),

    CONSTRAINT valid_board_token
        EXPECT (
            board_token IS NOT NULL
            AND TRIM(board_token) <> ''
        )
)
COMMENT 'Flattened Greenhouse API job snapshots'
AS

SELECT
    s.board_token,
    s.snapshot_id,
    TRY_CAST(s.snapshot_at AS TIMESTAMP) AS snapshot_at,

    j.job.id AS job_id,
    j.job.internal_job_id,
    j.job.title,

    TRY_CAST(j.job.first_published AS TIMESTAMP)
        AS first_published,

    TRY_CAST(j.job.updated_at AS TIMESTAMP)
        AS updated_at,

    j.job.content AS content_html,
    j.job.location.name AS location_raw,

    j.job.departments,
    j.job.offices,
    j.job.metadata AS job_metadata,

    s.source_file,
    s.ingested_at

FROM STREAM(job_market.bronze.greenhouse_snapshots_clean) AS s,
LATERAL EXPLODE(s.jobs) AS j(job);
