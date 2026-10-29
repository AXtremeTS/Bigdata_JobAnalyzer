SELECT
    CAST(posted_month AS STRING) AS month,
    COUNT(*) AS job_count,
    ROUND(SUM(application_count), 0) AS application_count
FROM tang2_jobs
WHERE posted_month IS NOT NULL AND posted_month BETWEEN 1 AND 12
GROUP BY posted_month
ORDER BY posted_month;
