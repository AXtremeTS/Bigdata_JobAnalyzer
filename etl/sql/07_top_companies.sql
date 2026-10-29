-- etl/sql/07_top_companies.sql
-- Top employers with the highest number of active job postings
SELECT 
    company,
    province,
    COUNT(*) AS active_jobs,
    ROUND(AVG(CASE WHEN salary_avg_vnd IS NOT NULL AND NOT is_salary_negotiable AND NOT is_salary_outlier THEN salary_avg_vnd END), 0) AS avg_salary_vnd
FROM tang2_jobs
WHERE company IS NOT NULL AND company != 'N/A'
GROUP BY company, province
ORDER BY active_jobs DESC
LIMIT 20;

