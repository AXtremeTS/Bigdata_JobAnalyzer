-- etl/sql/06_skills_analytics.sql
-- Top technical skills in demand across job postings
SELECT 
    skill,
    COUNT(*) AS frequency,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tang2_jobs WHERE size(technical_skills) > 0), 2) AS prevalence_pct
FROM (
    SELECT EXPLODE(technical_skills) AS skill
    FROM tang2_jobs
    WHERE technical_skills IS NOT NULL AND size(technical_skills) > 0
)
GROUP BY skill
ORDER BY frequency DESC
LIMIT 30;

