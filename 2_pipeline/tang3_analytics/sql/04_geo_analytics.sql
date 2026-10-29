-- etl/sql/04_geo_analytics.sql
-- Geographic distribution of job postings and salary medians by Province and Region
SELECT 
    province,
    region,
    COUNT(*) AS job_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS share_pct,
    COUNT(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable THEN 1 END) AS salary_samples,
    ROUND(percentile_approx(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END, 0.50), 0) AS median_salary_vnd,
    ROUND(AVG(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END), 0) AS avg_salary_vnd
FROM tang2_jobs
WHERE province IS NOT NULL
GROUP BY province, region
HAVING COUNT(*) >= 5
ORDER BY job_count DESC;

