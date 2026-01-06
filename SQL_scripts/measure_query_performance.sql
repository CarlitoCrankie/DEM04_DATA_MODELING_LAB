-- ============================================================================
-- QUERY PERFORMANCE MEASUREMENT SCRIPT
-- ============================================================================
-- Purpose: Measure and compare query performance between OLTP and Star Schema
-- This script measures execution time and analyzes query plans
-- ============================================================================

-- Enable timing
SET profiling = 1;
SET profiling_history_size = 100;

-- ============================================================================
-- QUERY 1: Monthly Encounters by Specialty
-- ============================================================================

-- --- NORMALIZED OLTP VERSION ---
SELECT 'OLTP VERSION - Starting...' AS status;

SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(e.encounter_id) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    DATE_FORMAT(e.encounter_date, '%Y-%m'),
    s.specialty_name,
    e.encounter_type
ORDER BY month, specialty_name, encounter_type
LIMIT 10;

-- Get execution time for OLTP
SELECT 
    'OLTP Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- --- STAR SCHEMA VERSION ---
SELECT 'STAR SCHEMA VERSION - Starting...' AS status;

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
GROUP BY 
    d.month_year,
    s.specialty_name,
    et.encounter_type_name
ORDER BY d.month_year, s.specialty_name, et.encounter_type_name
LIMIT 10;

-- Get execution time for Star Schema
SELECT 
    'Star Schema Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- Compare performance
SELECT '
QUERY 1 PERFORMANCE COMPARISON
' AS '';

SET @oltp_time = (SELECT SUM(duration) FROM information_schema.profiling 
                   WHERE query_id = (SELECT MAX(query_id) - 2 FROM information_schema.profiling));
SET @star_time = (SELECT SUM(duration) FROM information_schema.profiling 
                   WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling));

SELECT 'Metric' AS metric, 'OLTP' AS oltp_value, 'Star Schema' AS star_value, 'Improvement' AS improvement
UNION ALL SELECT '---', '---', '---', '---'
UNION ALL SELECT 'Execution Time (seconds)', 
    CAST(ROUND(@oltp_time, 4) AS CHAR),
    CAST(ROUND(@star_time, 4) AS CHAR),
    CONCAT(ROUND(@oltp_time / @star_time, 1), 'x faster')
UNION ALL SELECT 'Execution Time (ms)',
    CAST(ROUND(@oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time * 1000, 2) AS CHAR),
    CONCAT('Saved ', ROUND((@oltp_time - @star_time) * 1000, 2), ' ms');

-- ============================================================================
-- QUERY 2: Top Diagnosis-Procedure Pairs
-- ============================================================================

-- --- NORMALIZED OLTP VERSION ---
SELECT 'OLTP VERSION - Starting...' AS status;

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

-- Get execution time
SELECT 
    'OLTP Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- --- STAR SCHEMA VERSION ---
SELECT 'STAR SCHEMA VERSION - Starting...' AS status;

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

-- Get execution time
SELECT 
    'Star Schema Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- Compare performance
SELECT '
QUERY 2 PERFORMANCE COMPARISON
' AS '';

SET @oltp_time2 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) - 2 FROM information_schema.profiling));
SET @star_time2 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling));

SELECT 'Metric' AS metric, 'OLTP' AS oltp_value, 'Star Schema' AS star_value, 'Improvement' AS improvement
UNION ALL SELECT '---', '---', '---', '---'
UNION ALL SELECT 'Execution Time (seconds)', 
    CAST(ROUND(@oltp_time2, 4) AS CHAR),
    CAST(ROUND(@star_time2, 4) AS CHAR),
    CONCAT(ROUND(@oltp_time2 / @star_time2, 1), 'x faster')
UNION ALL SELECT 'Execution Time (ms)',
    CAST(ROUND(@oltp_time2 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time2 * 1000, 2) AS CHAR),
    CONCAT('Saved ', ROUND((@oltp_time2 - @star_time2) * 1000, 2), ' ms');

-- ============================================================================
-- QUERY 3: 30-Day Readmission Rate
-- ============================================================================


-- --- NORMALIZED OLTP VERSION ---
SELECT 'OLTP VERSION - Starting...' AS status;

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
GROUP BY s.specialty_name
ORDER BY readmission_rate_pct DESC;

-- Get execution time
SELECT 
    'OLTP Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- --- STAR SCHEMA VERSION ---
SELECT 'STAR SCHEMA VERSION - Starting...' AS status;

SELECT 
    s.specialty_name,
    COUNT(DISTINCT e1.encounter_key) AS total_inpatient_discharges,
    COUNT(DISTINCT e2.encounter_key) AS readmissions_within_30_days,
    ROUND(100.0 * COUNT(DISTINCT e2.encounter_key) / 
          NULLIF(COUNT(DISTINCT e1.encounter_key), 0), 2) AS readmission_rate_pct
FROM fact_encounters e1
JOIN dim_specialty s ON e1.specialty_key = s.specialty_key
JOIN dim_encounter_type et1 ON e1.encounter_type_key = et1.encounter_type_key
LEFT JOIN fact_encounters e2 ON 
    e2.patient_key = e1.patient_key
    AND e2.encounter_type_key = (SELECT encounter_type_key FROM dim_encounter_type WHERE encounter_type_code = 'IP')
    AND e2.encounter_date > e1.discharge_date
    AND e2.encounter_date <= DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
    AND e2.encounter_key != e1.encounter_key
WHERE 
    et1.encounter_type_code = 'IP'
    AND e1.discharge_date IS NOT NULL
GROUP BY s.specialty_name
ORDER BY readmission_rate_pct DESC;

-- Get execution time
SELECT 
    'Star Schema Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- Compare performance
SELECT '
QUERY 3 PERFORMANCE COMPARISON
' AS '';

SET @oltp_time3 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) - 2 FROM information_schema.profiling));
SET @star_time3 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling));

SELECT 'Metric' AS metric, 'OLTP' AS oltp_value, 'Star Schema' AS star_value, 'Improvement' AS improvement
UNION ALL SELECT '---', '---', '---', '---'
UNION ALL SELECT 'Execution Time (seconds)', 
    CAST(ROUND(@oltp_time3, 4) AS CHAR),
    CAST(ROUND(@star_time3, 4) AS CHAR),
    CONCAT(ROUND(@oltp_time3 / @star_time3, 1), 'x faster')
UNION ALL SELECT 'Execution Time (ms)',
    CAST(ROUND(@oltp_time3 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time3 * 1000, 2) AS CHAR),
    CONCAT('Saved ', ROUND((@oltp_time3 - @star_time3) * 1000, 2), ' ms');

-- ============================================================================
-- QUERY 4: Revenue by Specialty & Month
-- ============================================================================

-- --- NORMALIZED OLTP VERSION ---
SELECT 'OLTP VERSION - Starting...' AS status;

SELECT 
    DATE_FORMAT(b.claim_date, '%Y-%m') AS month,
    s.specialty_name,
    COUNT(DISTINCT b.billing_id) AS total_claims,
    SUM(b.claim_amount) AS total_billed,
    SUM(b.allowed_amount) AS total_revenue,
    ROUND(AVG(b.allowed_amount), 2) AS avg_claim_value
FROM billing b
JOIN encounters e ON b.encounter_id = e.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
WHERE b.claim_status = 'Paid'
GROUP BY 
    DATE_FORMAT(b.claim_date, '%Y-%m'),
    s.specialty_name
ORDER BY month, total_revenue DESC
LIMIT 20;

-- Get execution time
SELECT 
    'OLTP Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- --- STAR SCHEMA VERSION ---
SELECT 'STAR SCHEMA VERSION - Starting...' AS status;

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
WHERE f.total_allowed_amount IS NOT NULL
GROUP BY 
    d.month_year,
    s.specialty_name
ORDER BY d.month_year, total_revenue DESC
LIMIT 20;

-- Get execution time
SELECT 
    'Star Schema Execution Time' AS metric,
    CONCAT(ROUND(SUM(duration), 4), ' seconds') AS value,
    CONCAT(ROUND(SUM(duration) * 1000, 2), ' ms') AS milliseconds
FROM information_schema.profiling 
WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling);

-- Compare performance
SELECT '
QUERY 4 PERFORMANCE COMPARISON
' AS '';

SET @oltp_time4 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) - 2 FROM information_schema.profiling));
SET @star_time4 = (SELECT SUM(duration) FROM information_schema.profiling 
                    WHERE query_id = (SELECT MAX(query_id) FROM information_schema.profiling));

SELECT 'Metric' AS metric, 'OLTP' AS oltp_value, 'Star Schema' AS star_value, 'Improvement' AS improvement
UNION ALL SELECT '---', '---', '---', '---'
UNION ALL SELECT 'Execution Time (seconds)', 
    CAST(ROUND(@oltp_time4, 4) AS CHAR),
    CAST(ROUND(@star_time4, 4) AS CHAR),
    CONCAT(ROUND(@oltp_time4 / @star_time4, 1), 'x faster')
UNION ALL SELECT 'Execution Time (ms)',
    CAST(ROUND(@oltp_time4 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time4 * 1000, 2) AS CHAR),
    CONCAT('Saved ', ROUND((@oltp_time4 - @star_time4) * 1000, 2), ' ms');

-- ============================================================================
-- OVERALL PERFORMANCE SUMMARY
-- ============================================================================


SELECT 'Query' AS query, 'OLTP Time (ms)' AS oltp_time, 'Star Time (ms)' AS star_time, 
       'Speedup' AS speedup, 'Time Saved (ms)' AS time_saved
UNION ALL SELECT '---', '---', '---', '---', '---'
UNION ALL SELECT 'Q1: Monthly Encounters',
    CAST(ROUND(@oltp_time * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time * 1000, 2) AS CHAR),
    CONCAT(ROUND(@oltp_time / @star_time, 1), 'x'),
    CAST(ROUND((@oltp_time - @star_time) * 1000, 2) AS CHAR)
UNION ALL SELECT 'Q2: Diagnosis-Procedure',
    CAST(ROUND(@oltp_time2 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time2 * 1000, 2) AS CHAR),
    CONCAT(ROUND(@oltp_time2 / @star_time2, 1), 'x'),
    CAST(ROUND((@oltp_time2 - @star_time2) * 1000, 2) AS CHAR)
UNION ALL SELECT 'Q3: Readmissions',
    CAST(ROUND(@oltp_time3 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time3 * 1000, 2) AS CHAR),
    CONCAT(ROUND(@oltp_time3 / @star_time3, 1), 'x'),
    CAST(ROUND((@oltp_time3 - @star_time3) * 1000, 2) AS CHAR)
UNION ALL SELECT 'Q4: Revenue',
    CAST(ROUND(@oltp_time4 * 1000, 2) AS CHAR),
    CAST(ROUND(@star_time4 * 1000, 2) AS CHAR),
    CONCAT(ROUND(@oltp_time4 / @star_time4, 1), 'x'),
    CAST(ROUND((@oltp_time4 - @star_time4) * 1000, 2) AS CHAR)
UNION ALL SELECT '---', '---', '---', '---', '---'
UNION ALL SELECT 'AVERAGE',
    CAST(ROUND((@oltp_time + @oltp_time2 + @oltp_time3 + @oltp_time4) / 4 * 1000, 2) AS CHAR),
    CAST(ROUND((@star_time + @star_time2 + @star_time3 + @star_time4) / 4 * 1000, 2) AS CHAR),
    CONCAT(ROUND((@oltp_time + @oltp_time2 + @oltp_time3 + @oltp_time4) / 
                 (@star_time + @star_time2 + @star_time3 + @star_time4), 1), 'x'),
    CAST(ROUND(((@oltp_time + @oltp_time2 + @oltp_time3 + @oltp_time4) - 
                (@star_time + @star_time2 + @star_time3 + @star_time4)) / 4 * 1000, 2) AS CHAR);

-- ============================================================================
-- EXPLAIN ANALYSIS (for rows scanned estimation)
-- ============================================================================

-- Query 1 OLTP
SELECT 'Query 1 - OLTP (Estimated Rows)' AS analysis_type;
EXPLAIN FORMAT=JSON
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(e.encounter_id) AS total_encounters
FROM encounters e
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY DATE_FORMAT(e.encounter_date, '%Y-%m'), s.specialty_name, e.encounter_type;

-- Query 1 Star Schema
SELECT 'Query 1 - Star Schema (Estimated Rows)' AS analysis_type;
EXPLAIN FORMAT=JSON
SELECT 
    d.month_year,
    s.specialty_name,
    et.encounter_type_name,
    COUNT(*) AS total_encounters
FROM fact_encounters f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_specialty s ON f.specialty_key = s.specialty_key
JOIN dim_encounter_type et ON f.encounter_type_key = et.encounter_type_key
GROUP BY d.month_year, s.specialty_name, et.encounter_type_name;

-- Disable profiling
SET profiling = 0;