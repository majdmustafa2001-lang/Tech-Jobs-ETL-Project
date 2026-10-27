
CREATE OR REFRESH STREAMING TABLE
job_market.silver.greenhouse_jobs_classified

COMMENT 'Greenhouse job classification and quality checks'

AS


WITH checked AS (

    SELECT
        *,

        NULLIF(
            CONCAT_WS(' | ',

                CASE
                    WHEN job_id IS NULL OR job_id <= 0
                    THEN 'INVALID_JOB_ID'
                END,

                CASE
                    WHEN board_token IS NULL
                         OR TRIM(board_token) = ''
                    THEN 'INVALID_BOARD_TOKEN'
                END,

                CASE
                    WHEN snapshot_at IS NULL
                    THEN 'INVALID_SNAPSHOT_DATE'
                END,

                CASE
                    WHEN title IS NULL OR TRIM(title) = ''
                    THEN 'MISSING_TITLE'
                END

            ),
            ''
        ) AS quarantine_reason

    FROM STREAM(
        job_market.silver.greenhouse_jobs_snapshots
    )
)


SELECT
    *,

    quarantine_reason IS NOT NULL AS is_quarantined,

    COALESCE(
        LOWER(title) RLIKE
        r'data engineer|data scientist|data analyst|analytics engineer|machine learning|artificial intelligence|\bai\b|software engineer|software developer|backend|frontend|full.stack|devops|site reliability|platform engineer|security engineer|cloud engineer|big data engineer',
        FALSE
    ) AS is_tech,

    CASE
        WHEN is_quarantined THEN 'Quarantine'

        WHEN NOT is_tech THEN 'Non-Tech'

        WHEN LOWER(title) RLIKE
            r'\b(data engineer|analytics engineer|big data engineer|data platform engineer)\b'
            THEN 'Data Engineering'

        WHEN LOWER(title) RLIKE
            r'\b(data analyst|business intelligence analyst|bi analyst)\b'
            THEN 'Data Analytics'

        WHEN LOWER(title) RLIKE
            r'\b(data scientist|machine learning|ml engineer|ai engineer|ai scientist|ai researcher|applied scientist)\b'
            THEN 'Data Science & ML'

        WHEN LOWER(title) RLIKE
            r'\b(security engineer|security researcher|cybersecurity engineer|application security engineer)\b'
            THEN 'Cybersecurity'

        WHEN LOWER(title) RLIKE
            r'\b(devops|site reliability|platform engineer|cloud engineer|infrastructure engineer)\b'
            THEN 'Cloud & DevOps'

        WHEN LOWER(title) RLIKE
            r'\b(software engineer|software developer|backend|frontend|full.stack)\b'
            THEN 'Software Engineering'

        ELSE 'Other Technical'
    END AS role_family

FROM checked;
