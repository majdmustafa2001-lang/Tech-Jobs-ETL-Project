CREATE OR REFRESH STREAMING TABLE
job_market.bronze.greenhouse_snapshots_clean
AS
SELECT
    *,
    _metadata.file_path AS source_file,
    current_timestamp() AS ingested_at
FROM STREAM read_files(
    '/Volumes/job_market/landing/raw_files/greenhouse_snapshots/',
    format => 'json',
    multiLine => true,
    inferColumnTypes => true,
    schemaEvolutionMode => 'rescue',
    rescuedDataColumn => '_rescued_data'
);