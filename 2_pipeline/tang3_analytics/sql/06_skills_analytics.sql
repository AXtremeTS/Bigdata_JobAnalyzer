-- etl/sql/06_skills_analytics.sql
-- All reported skills in demand across job postings, not only technology skills.
SELECT 
    skill,
    COUNT(*) AS frequency,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tang2_jobs WHERE size(skills) > 0), 2) AS prevalence_pct
FROM (
    SELECT EXPLODE(skills) AS skill
    FROM tang2_jobs
    WHERE skills IS NOT NULL AND size(skills) > 0
)
GROUP BY skill
ORDER BY frequency DESC
LIMIT 30;
