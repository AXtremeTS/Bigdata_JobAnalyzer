-- etl/sql/03_salary_analytics.sql
-- Salary distribution by universal role segment inferred from job titles.
SELECT 
    role_segment AS segment,
    role_segment AS level,
    COUNT(*) AS sample_size,
    ROUND(MIN(salary_avg_vnd), 0) AS min_salary_vnd,
    ROUND(percentile_approx(salary_avg_vnd, 0.25), 0) AS p25_salary_vnd,
    ROUND(percentile_approx(salary_avg_vnd, 0.50), 0) AS median_salary_vnd,
    ROUND(percentile_approx(salary_avg_vnd, 0.75), 0) AS p75_salary_vnd,
    ROUND(percentile_approx(salary_avg_vnd, 0.90), 0) AS p90_salary_vnd,
    ROUND(MAX(salary_avg_vnd), 0) AS max_salary_vnd,
    ROUND(AVG(salary_avg_vnd), 0) AS avg_salary_vnd
FROM tang2_jobs
WHERE salary_avg_vnd IS NOT NULL 
  AND NOT is_salary_negotiable 
  AND NOT is_salary_outlier
GROUP BY role_segment
ORDER BY median_salary_vnd DESC;
