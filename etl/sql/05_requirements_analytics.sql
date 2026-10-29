-- etl/sql/05_requirements_analytics.sql
-- Breakdown of hiring requirements: Seniority Level, Education, and Experience
SELECT 
    level,
    education,
    COUNT(*) AS job_count,
    ROUND(AVG(exp_min_years), 1) AS avg_min_exp_years,
    ROUND(AVG(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END), 0) AS avg_salary_vnd
FROM tang2_jobs
GROUP BY level, education
ORDER BY job_count DESC;

