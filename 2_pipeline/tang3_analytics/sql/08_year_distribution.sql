SELECT
    COALESCE(CAST(posted_year AS STRING), 'Không xác định') AS year,
    COUNT(*) AS job_count,
    ROUND(SUM(application_count), 0) AS application_count
FROM tang2_jobs
GROUP BY COALESCE(CAST(posted_year AS STRING), 'Không xác định')
ORDER BY year;

