-- Top jobs by normalized province/bang and real application volume.
-- application_count is populated only when the source dataset provides an
-- application/applicant field; zero means that metric is unavailable.
SELECT
    province,
    job_title,
    SUM(application_count) AS application_count,
    COUNT(*) AS job_count,
    COUNT(DISTINCT posted_year) AS active_years,
    COUNT(DISTINCT posted_month) AS active_months
FROM tang2_jobs
WHERE province IS NOT NULL
  AND job_title IS NOT NULL
GROUP BY province, job_title
ORDER BY application_count DESC, job_count DESC, province, job_title
