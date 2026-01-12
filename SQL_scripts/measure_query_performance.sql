-- ============================================================================
-- QUERY PERFORMANCE MEASUREMENT SCRIPT
-- ============================================================================
-- Purpose: Compare OLTP vs Star Schema query performance
-- Measures: Execution time for 4 common analytical queries
-- ============================================================================

-- Enable performance schema
UPDATE performance_schema.setup_instruments 
SET ENABLED = 'YES', TIMED = 'YES' 
WHERE NAME LIKE '%statement%';

UPDATE performance_schema.setup_consumers 
SET ENABLED = 'YES' 
WHERE NAME LIKE '%events_statements%';

-- Clear previous measurements
TRUNCATE TABLE performance_schema.events_statements_history_long;

SELECT 'Starting performance comparison...' AS status;
SELECT '' AS blank_line;

-- ============================================================================
-- QUERY 1: Monthly Encounters by Specialty
-- ============================================================================

SELECT 'Running Query 1: Monthly Encounters by Specialty...' AS status;

-- OLTP VERSION
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(e.encounter_id) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
WHERE e.encounter_date >= '2022-01-01'
GROUP BY 
    DATE_FORMAT(e.encounter_date, '%Y-%m'),
    s.specialty_name,
    e.encounter_type
ORDER BY month DESC, specialty_name, encounter_type
LIMIT 10;

SET @q1_oltp_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM encounters e%JOIN providers p%JOIN specialties s%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q1_oltp_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('OLTP completed: ', ROUND(@q1_oltp_time * 1000, 2), ' ms') AS progress;
SELECT SLEEP(0.5);

-- STAR SCHEMA VERSION
SELECT 
    d.month_year,
    s.specialty_name,
    et.encounter_type_name,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT f.patient_key) AS unique_patients
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
WHERE d.year >= 2022
GROUP BY 
    d.month_year,
    s.specialty_name,
    et.encounter_type_name
ORDER BY d.month_year DESC, s.specialty_name, et.encounter_type_name
LIMIT 10;

SET @q1_star_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM fact_encounters f%JOIN dim_date d%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q1_star_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('Star completed: ', ROUND(@q1_star_time * 1000, 2), ' ms') AS progress;
SELECT CONCAT('Query 1 Speedup: ', ROUND(@q1_oltp_time / NULLIF(@q1_star_time, 0), 2), 'x') AS result;
SELECT '' AS blank_line;

-- ============================================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs
-- ============================================================================

SELECT 'Running Query 2: Top Diagnosis-Procedure Pairs...' AS status;

-- OLTP VERSION
SELECT 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description,
    COUNT(*) AS combination_count
FROM encounter_diagnoses ed
JOIN diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN encounter_procedures ep ON ed.encounter_id = ep.encounter_id
JOIN procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY 
    d.icd10_code,
    d.icd10_description,
    pr.cpt_code,
    pr.cpt_description
ORDER BY combination_count DESC
LIMIT 20;

SET @q2_oltp_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM encounter_diagnoses ed%JOIN diagnoses d%JOIN encounter_procedures ep%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q2_oltp_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('OLTP completed: ', ROUND(@q2_oltp_time * 1000, 2), ' ms') AS progress;
SELECT SLEEP(0.5);

-- STAR SCHEMA VERSION
SELECT 
    diag.icd10_code,
    diag.icd10_description,
    proc.cpt_code,
    proc.cpt_description,
    COUNT(DISTINCT f.encounter_key) AS encounter_count
FROM fact_encounters f
JOIN bridge_encounter_diagnoses bd ON f.encounter_key = bd.encounter_key
JOIN dim_diagnosis diag ON bd.diagnosis_key = diag.diagnosis_key
JOIN bridge_encounter_procedures bp ON f.encounter_key = bp.encounter_key
JOIN dim_procedure proc ON bp.procedure_key = proc.procedure_key
GROUP BY 
    diag.icd10_code,
    diag.icd10_description,
    proc.cpt_code,
    proc.cpt_description
ORDER BY encounter_count DESC
LIMIT 20;

SET @q2_star_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM fact_encounters f%JOIN bridge_encounter_diagnoses bd%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q2_star_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('Star completed: ', ROUND(@q2_star_time * 1000, 2), ' ms') AS progress;
SELECT CONCAT('Query 2 Speedup: ', ROUND(@q2_oltp_time / NULLIF(@q2_star_time, 0), 2), 'x') AS result;
SELECT '' AS blank_line;

-- ============================================================================
-- QUERY 3: 30-Day Readmission Rate
-- ============================================================================

SELECT 'Running Query 3: 30-Day Readmission Rate...' AS status;

-- OLTP VERSION
SELECT 
    s.specialty_name,
    COUNT(DISTINCT e1.encounter_id) AS total_inpatient_discharges,
    COUNT(DISTINCT e2.encounter_id) AS readmissions_within_30_days,
    ROUND(100.0 * COUNT(DISTINCT e2.encounter_id) / 
          NULLIF(COUNT(DISTINCT e1.encounter_id), 0), 2) AS readmission_rate_pct
FROM encounters e1
JOIN providers p1 ON e1.provider_id = p1.provider_id
JOIN specialties s ON p1.specialty_id = s.specialty_id
LEFT JOIN encounters e2 ON 
    e2.patient_id = e1.patient_id
    AND e2.encounter_type = 'Inpatient'
    AND e2.encounter_date > e1.discharge_date
    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
    AND e2.encounter_id != e1.encounter_id
WHERE 
    e1.encounter_type = 'Inpatient'
    AND e1.discharge_date IS NOT NULL
    AND e1.encounter_date >= '2022-01-01'
GROUP BY s.specialty_name
HAVING COUNT(DISTINCT e1.encounter_id) >= 10
ORDER BY readmission_rate_pct DESC;

SET @q3_oltp_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM encounters e1%LEFT JOIN encounters e2%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q3_oltp_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('OLTP completed: ', ROUND(@q3_oltp_time * 1000, 2), ' ms') AS progress;
SELECT SLEEP(0.5);

-- STAR SCHEMA VERSION
SELECT 
    s.specialty_name,
    COUNT(DISTINCT e1.encounter_key) AS total_inpatient_discharges,
    COUNT(DISTINCT e2.encounter_key) AS readmissions_within_30_days,
    ROUND(100.0 * COUNT(DISTINCT e2.encounter_key) / 
          NULLIF(COUNT(DISTINCT e1.encounter_key), 0), 2) AS readmission_rate_pct
FROM fact_encounters e1
JOIN dim_specialty s ON e1.specialty_key = s.specialty_key
JOIN dim_encounter_type et1 ON e1.encounter_type_key = et1.encounter_type_key
JOIN dim_date d1 ON e1.date_key = d1.date_key
LEFT JOIN fact_encounters e2 ON 
    e2.patient_key = e1.patient_key
    AND e2.encounter_type_key = (SELECT encounter_type_key FROM dim_encounter_type WHERE encounter_type_code = 'IP')
    AND e2.encounter_date > e1.discharge_date
    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
    AND e2.encounter_key != e1.encounter_key
WHERE 
    et1.encounter_type_code = 'IP'
    AND e1.discharge_date IS NOT NULL
    AND d1.year >= 2022
GROUP BY s.specialty_name
HAVING COUNT(DISTINCT e1.encounter_key) >= 10
ORDER BY readmission_rate_pct DESC;

SET @q3_star_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM fact_encounters e1%LEFT JOIN fact_encounters e2%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q3_star_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('Star completed: ', ROUND(@q3_star_time * 1000, 2), ' ms') AS progress;
SELECT CONCAT('Query 3 Speedup: ', ROUND(@q3_oltp_time / NULLIF(@q3_star_time, 0), 2), 'x') AS result;
SELECT '' AS blank_line;

-- ============================================================================
-- QUERY 4: Revenue by Specialty & Month
-- ============================================================================

SELECT 'Running Query 4: Revenue by Specialty & Month...' AS status;

-- OLTP VERSION
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT b.billing_id) AS total_claims,
    SUM(b.claim_amount) AS total_billed,
    SUM(b.allowed_amount) AS total_revenue,
    ROUND(AVG(b.allowed_amount), 2) AS avg_claim_value
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
LEFT JOIN billing b ON e.encounter_id = b.encounter_id AND b.claim_status = 'Paid'
WHERE e.encounter_date >= '2022-01-01'
GROUP BY 
    DATE_FORMAT(e.encounter_date, '%Y-%m'),
    s.specialty_name
ORDER BY month DESC, total_revenue DESC
LIMIT 20;

SET @q4_oltp_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM encounters e%JOIN providers p%LEFT JOIN billing b%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q4_oltp_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('OLTP completed: ', ROUND(@q4_oltp_time * 1000, 2), ' ms') AS progress;
SELECT SLEEP(0.5);

-- STAR SCHEMA VERSION
SELECT 
    d.month_year,
    s.specialty_name,
    COUNT(*) AS total_encounters,
    COUNT(CASE WHEN f.total_allowed_amount IS NOT NULL THEN 1 END) AS billed_encounters,
    SUM(f.total_claim_amount) AS total_billed,
    SUM(f.total_allowed_amount) AS total_revenue,
    ROUND(AVG(f.total_allowed_amount), 2) AS avg_revenue_per_encounter
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
WHERE d.year >= 2022
  AND f.total_allowed_amount IS NOT NULL
GROUP BY 
    d.month_year,
    s.specialty_name
ORDER BY d.month_year DESC, total_revenue DESC
LIMIT 20;

SET @q4_star_time = (
    SELECT TIMER_WAIT / 1000000000000
    FROM performance_schema.events_statements_history_long
    WHERE SQL_TEXT LIKE '%FROM fact_encounters f%f.total_allowed_amount%'
      AND SQL_TEXT NOT LIKE '%performance_schema%'
      AND SQL_TEXT NOT LIKE '%SET @q4_star_time%'
    ORDER BY TIMER_START DESC
    LIMIT 1
);

SELECT CONCAT('Star completed: ', ROUND(@q4_star_time * 1000, 2), ' ms') AS progress;
SELECT CONCAT('Query 4 Speedup: ', ROUND(@q4_oltp_time / NULLIF(@q4_star_time, 0), 2), 'x') AS result;
SELECT '' AS blank_line;

-- ============================================================================
-- PERFORMANCE SUMMARY
-- ============================================================================

SELECT 'PERFORMANCE COMPARISON SUMMARY' AS title;
SELECT '' AS blank_line;

SELECT 'Query' AS query, 
       'OLTP (ms)' AS oltp_time, 
       'Star (ms)' AS star_time, 
       'Speedup' AS speedup, 
       'Saved (ms)' AS time_saved
UNION ALL 
SELECT REPEAT('-', 35), REPEAT('-', 10), REPEAT('-', 10), REPEAT('-', 10), REPEAT('-', 10)
UNION ALL 
SELECT 'Q1: Monthly Encounters',
    CAST(ROUND(@q1_oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@q1_star_time * 1000, 2) AS CHAR),
    CONCAT(ROUND(@q1_oltp_time / NULLIF(@q1_star_time, 0), 2), 'x'),
    CAST(ROUND((@q1_oltp_time - @q1_star_time) * 1000, 2) AS CHAR)
UNION ALL 
SELECT 'Q2: Diagnosis-Procedure Pairs',
    CAST(ROUND(@q2_oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@q2_star_time * 1000, 2) AS CHAR),
    CONCAT(ROUND(@q2_oltp_time / NULLIF(@q2_star_time, 0), 2), 'x'),
    CAST(ROUND((@q2_oltp_time - @q2_star_time) * 1000, 2) AS CHAR)
UNION ALL 
SELECT 'Q3: 30-Day Readmissions',
    CAST(ROUND(@q3_oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@q3_star_time * 1000, 2) AS CHAR),
    CONCAT(ROUND(@q3_oltp_time / NULLIF(@q3_star_time, 0), 2), 'x'),
    CAST(ROUND((@q3_oltp_time - @q3_star_time) * 1000, 2) AS CHAR)
UNION ALL 
SELECT 'Q4: Revenue Analysis',
    CAST(ROUND(@q4_oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@q4_star_time * 1000, 2) AS CHAR),
    CONCAT(ROUND(@q4_oltp_time / NULLIF(@q4_star_time, 0), 2), 'x'),
    CAST(ROUND((@q4_oltp_time - @q4_star_time) * 1000, 2) AS CHAR)
UNION ALL 
SELECT REPEAT('-', 35), REPEAT('-', 10), REPEAT('-', 10), REPEAT('-', 10), REPEAT('-', 10)
UNION ALL 
SELECT 'AVERAGE',
    CAST(ROUND((@q1_oltp_time + @q2_oltp_time + @q3_oltp_time + @q4_oltp_time) / 4 * 1000, 2) AS CHAR),
    CAST(ROUND((@q1_star_time + @q2_star_time + @q3_star_time + @q4_star_time) / 4 * 1000, 2) AS CHAR),
    CONCAT(ROUND(
        (@q1_oltp_time + @q2_oltp_time + @q3_oltp_time + @q4_oltp_time) / 
        NULLIF((@q1_star_time + @q2_star_time + @q3_star_time + @q4_star_time), 0), 2), 'x'),
    CAST(ROUND(((@q1_oltp_time + @q2_oltp_time + @q3_oltp_time + @q4_oltp_time) - 
                (@q1_star_time + @q2_star_time + @q3_star_time + @q4_star_time)) / 4 * 1000, 2) AS CHAR);

SELECT '' AS blank_line;
SELECT 'Performance testing complete!' AS final_status;