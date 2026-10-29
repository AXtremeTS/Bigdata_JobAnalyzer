-- etl/sql/02_hot_jobs.sql
-- Top hot jobs ranked by real application volume when the source provides it.
SELECT 
    job_title,
    category,
    COUNT(*) AS job_count,
    ROUND(SUM(application_count), 0) AS application_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS demand_share_pct,
    ROUND(AVG(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END), 0) AS avg_salary_vnd,
    ROUND(AVG(CASE WHEN salary_avg_usd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_usd END), 2) AS avg_salary_usd
FROM tang2_jobs
WHERE job_title IS NOT NULL
GROUP BY job_title, category
HAVING COUNT(*) >= 5
ORDER BY application_count DESC, job_count DESC, avg_salary_vnd DESC
LIMIT 20;

