-- etl/sql/01_overview.sql
-- Overview KPI metrics across the entire dataset
SELECT 
    COUNT(*) AS total_jobs,
    COUNT(DISTINCT CASE WHEN company IS NOT NULL AND company != 'N/A' THEN company END) AS total_companies,
    COUNT(DISTINCT province) AS total_provinces,
    ROUND(COUNT(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable THEN 1 END) * 100.0 / COUNT(*), 1) AS salary_coverage_pct,
    ROUND(AVG(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END), 0) AS avg_salary_vnd,
    ROUND(AVG(CASE WHEN salary_avg_usd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_usd END), 2) AS avg_salary_usd
FROM tang2_jobs;

