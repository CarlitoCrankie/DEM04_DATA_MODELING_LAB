-- -- ============================================================================
-- -- OPTIMIZED ETL PIPELINE: OLTP → STAR SCHEMA (BATCH PROCESSING)
-- -- ============================================================================
-- -- Purpose: Transform and load data from normalized schema to star schema
-- -- Optimized for: 500K+ encounters with batch processing and commits
-- -- Run after: clean_star_schema.sql
-- -- ============================================================================

-- SET @etl_start_time = NOW();
-- SET FOREIGN_KEY_CHECKS = 0;

-- -- ============================================================================
-- -- STEP 1: LOAD DIMENSION TABLES (Small tables - no batching needed)
-- -- ============================================================================

-- SELECT 'STEP 1: Loading dimension tables...' AS status;

-- -- Load dim_date (for date range covering your data)
-- INSERT INTO dim_date (
--     date_key, calendar_date, year, quarter, quarter_name,
--     month, month_name, month_year, day_of_month, day_of_week,
--     day_name, week_of_year, is_weekend, is_holiday,
--     fiscal_year, fiscal_quarter, fiscal_period
-- )
-- SELECT 
--     DATE_FORMAT(calendar_date, '%Y%m%d') AS date_key,
--     calendar_date,
--     YEAR(calendar_date) AS year,
--     QUARTER(calendar_date) AS quarter,
--     CONCAT('Q', QUARTER(calendar_date), ' ', YEAR(calendar_date)) AS quarter_name,
--     MONTH(calendar_date) AS month,
--     DATE_FORMAT(calendar_date, '%M') AS month_name,
--     DATE_FORMAT(calendar_date, '%M %Y') AS month_year,
--     DAY(calendar_date) AS day_of_month,
--     DAYOFWEEK(calendar_date) AS day_of_week,
--     DATE_FORMAT(calendar_date, '%W') AS day_name,
--     WEEK(calendar_date) AS week_of_year,
--     CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
--     FALSE AS is_holiday,
--     YEAR(calendar_date) AS fiscal_year,
--     QUARTER(calendar_date) AS fiscal_quarter,
--     CONCAT('FY', YEAR(calendar_date), '-Q', QUARTER(calendar_date)) AS fiscal_period
-- FROM (
--     SELECT DATE('2020-01-01') + INTERVAL n DAY AS calendar_date
--     FROM (
--         SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
--         FROM 
--             (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
--              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
--             (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
--              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
--             (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
--              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
--             (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
--     ) numbers
--     WHERE DATE('2020-01-01') + INTERVAL n DAY <= DATE('2030-12-31')
-- ) dates;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' dates') AS status FROM dim_date;

-- -- Load dim_specialty
-- INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
-- SELECT specialty_id, specialty_name, specialty_code
-- FROM specialties;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' specialties') AS status FROM dim_specialty;

-- -- Load dim_department
-- INSERT INTO dim_department (department_id, department_name, floor, capacity)
-- SELECT department_id, department_name, floor, capacity
-- FROM departments;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' departments') AS status FROM dim_department;

-- -- Load dim_encounter_type
-- INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_code, requires_admission, average_duration_hours)
-- VALUES
--     ('Outpatient', 'OP', FALSE, 1.5),
--     ('Inpatient', 'IP', TRUE, 96.0),
--     ('ER', 'ER', FALSE, 6.5);

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter types') AS status FROM dim_encounter_type;

-- -- Load dim_diagnosis
-- INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
-- SELECT diagnosis_id, icd10_code, icd10_description
-- FROM diagnoses;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' diagnoses') AS status FROM dim_diagnosis;

-- -- Load dim_procedure
-- INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
-- SELECT procedure_id, cpt_code, cpt_description
-- FROM procedures;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' procedures') AS status FROM dim_procedure;

-- -- Load dim_patient (batched for safety with 100K patients)
-- DROP PROCEDURE IF EXISTS load_dim_patient_batch;
-- DELIMITER //
-- CREATE PROCEDURE load_dim_patient_batch(IN start_id INT, IN end_id INT)
-- BEGIN
--     START TRANSACTION;
    
--     INSERT INTO dim_patient (
--         patient_id, mrn, first_name, last_name, full_name,
--         date_of_birth, gender, age_group, source_system, effective_date, is_current
--     )
--     SELECT 
--         patient_id,
--         mrn,
--         first_name,
--         last_name,
--         CONCAT(first_name, ' ', last_name) AS full_name,
--         date_of_birth,
--         gender,
--         CASE 
--             WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
--             WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
--             WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 50 THEN '35-49'
--             WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 65 THEN '50-64'
--             ELSE '65+'
--         END AS age_group,
--         'OLTP' AS source_system,
--         CURDATE() AS effective_date,
--         TRUE AS is_current
--     FROM patients
--     WHERE patient_id BETWEEN start_id AND end_id;
    
--     COMMIT;
--     SELECT CONCAT('✓ Patient batch complete: ', start_id, ' to ', end_id) AS progress;
-- END//
-- DELIMITER ;

-- -- Load patients in batches of 20,000
-- CALL load_dim_patient_batch(1, 20000);
-- CALL load_dim_patient_batch(20001, 40000);
-- CALL load_dim_patient_batch(40001, 60000);
-- CALL load_dim_patient_batch(60001, 80000);
-- CALL load_dim_patient_batch(80001, 100000);

-- DROP PROCEDURE load_dim_patient_batch;
-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' patients') AS status FROM dim_patient;

-- -- Load dim_provider (with denormalized specialty)
-- INSERT INTO dim_provider (
--     provider_id, first_name, last_name, full_name, credential,
--     specialty_id, specialty_name, specialty_code
-- )
-- SELECT 
--     p.provider_id,
--     p.first_name,
--     p.last_name,
--     CONCAT('Dr. ', p.first_name, ' ', p.last_name) AS full_name,
--     p.credential,
--     p.specialty_id,
--     s.specialty_name,
--     s.specialty_code
-- FROM providers p
-- JOIN specialties s ON p.specialty_id = s.specialty_id;

-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' providers') AS status FROM dim_provider;

-- -- ============================================================================
-- -- STEP 2: LOAD FACT TABLE (BATCHED - CRITICAL FOR PERFORMANCE)
-- -- ============================================================================

-- SELECT 'STEP 2: Loading fact table (batched)...' AS status;

-- -- Create lookup tables for faster joins (avoids subqueries in main insert)
-- DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
-- CREATE TEMPORARY TABLE temp_patient_lookup (
--     patient_id INT PRIMARY KEY,
--     patient_key INT,
--     INDEX idx_patient_key (patient_key)
-- );

-- INSERT INTO temp_patient_lookup
-- SELECT patient_id, patient_key FROM dim_patient;

-- SELECT CONCAT('✓ Created patient lookup: ', COUNT(*), ' records') AS status FROM temp_patient_lookup;

-- DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
-- CREATE TEMPORARY TABLE temp_provider_lookup (
--     provider_id INT PRIMARY KEY,
--     provider_key INT,
--     specialty_key INT,
--     INDEX idx_provider_key (provider_key)
-- );

-- INSERT INTO temp_provider_lookup
-- SELECT 
--     p.provider_id,
--     dp.provider_key,
--     ds.specialty_key
-- FROM providers p
-- JOIN dim_provider dp ON p.provider_id = dp.provider_id
-- JOIN dim_specialty ds ON p.specialty_id = ds.specialty_id;

-- SELECT CONCAT('✓ Created provider lookup: ', COUNT(*), ' records') AS status FROM temp_provider_lookup;

-- DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
-- CREATE TEMPORARY TABLE temp_department_lookup (
--     department_id INT PRIMARY KEY,
--     department_key INT,
--     INDEX idx_department_key (department_key)
-- );

-- INSERT INTO temp_department_lookup
-- SELECT department_id, department_key FROM dim_department;

-- SELECT CONCAT('✓ Created department lookup: ', COUNT(*), ' records') AS status FROM temp_department_lookup;

-- DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
-- CREATE TEMPORARY TABLE temp_encounter_type_lookup (
--     encounter_type_name VARCHAR(50) PRIMARY KEY,
--     encounter_type_key INT,
--     INDEX idx_encounter_type_key (encounter_type_key)
-- );

-- INSERT INTO temp_encounter_type_lookup
-- SELECT encounter_type_name, encounter_type_key FROM dim_encounter_type;

-- SELECT CONCAT('✓ Created encounter type lookup: ', COUNT(*), ' records') AS status FROM temp_encounter_type_lookup;

-- -- Create aggregation temp tables for diagnoses and procedures counts
-- DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
-- CREATE TEMPORARY TABLE temp_diagnosis_counts (
--     encounter_id INT PRIMARY KEY,
--     diagnosis_count INT,
--     INDEX idx_encounter_id (encounter_id)
-- );

-- INSERT INTO temp_diagnosis_counts
-- SELECT encounter_id, COUNT(*) AS diagnosis_count
-- FROM encounter_diagnoses
-- GROUP BY encounter_id;

-- SELECT CONCAT('✓ Pre-aggregated diagnosis counts: ', COUNT(*), ' encounters') AS status FROM temp_diagnosis_counts;

-- DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
-- CREATE TEMPORARY TABLE temp_procedure_counts (
--     encounter_id INT PRIMARY KEY,
--     procedure_count INT,
--     INDEX idx_encounter_id (encounter_id)
-- );

-- INSERT INTO temp_procedure_counts
-- SELECT encounter_id, COUNT(*) AS procedure_count
-- FROM encounter_procedures
-- GROUP BY encounter_id;

-- SELECT CONCAT('✓ Pre-aggregated procedure counts: ', COUNT(*), ' encounters') AS status FROM temp_procedure_counts;

-- DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
-- CREATE TEMPORARY TABLE temp_billing_totals (
--     encounter_id INT PRIMARY KEY,
--     total_claim_amount DECIMAL(12,2),
--     total_allowed_amount DECIMAL(12,2),
--     claim_date DATE,
--     INDEX idx_encounter_id (encounter_id)
-- );

-- INSERT INTO temp_billing_totals
-- SELECT 
--     encounter_id,
--     SUM(claim_amount) AS total_claim_amount,
--     SUM(allowed_amount) AS total_allowed_amount,
--     claim_date
-- FROM billing
-- GROUP BY encounter_id;

-- SELECT CONCAT('✓ Pre-aggregated billing totals: ', COUNT(*), ' encounters') AS status FROM temp_billing_totals;

-- -- Now load fact_encounters in batches
-- DROP PROCEDURE IF EXISTS load_fact_encounters_batch;
-- DELIMITER //
-- CREATE PROCEDURE load_fact_encounters_batch(IN start_id INT, IN end_id INT)
-- BEGIN
--     START TRANSACTION;
    
--     INSERT INTO fact_encounters (
--         encounter_id,
--         date_key,
--         patient_key,
--         provider_key,
--         specialty_key,
--         department_key,
--         encounter_type_key,
--         encounter_date,
--         discharge_date,
--         diagnosis_count,
--         procedure_count,
--         total_claim_amount,
--         total_allowed_amount,
--         length_of_stay_days
--     )
--     SELECT 
--         e.encounter_id,
--         DATE_FORMAT(e.encounter_date, '%Y%m%d') AS date_key,
--         tpl.patient_key,
--         tprl.provider_key,
--         tprl.specialty_key,
--         tdl.department_key,
--         tetl.encounter_type_key,
--         e.encounter_date,
--         e.discharge_date,
--         COALESCE(tdc.diagnosis_count, 0) AS diagnosis_count,
--         COALESCE(tpc.procedure_count, 0) AS procedure_count,
--         COALESCE(tbt.total_claim_amount, 0) AS total_claim_amount,
--         COALESCE(tbt.total_allowed_amount, 0) AS total_allowed_amount,
--         CASE 
--             WHEN e.encounter_type = 'Inpatient' AND e.discharge_date IS NOT NULL
--             THEN DATEDIFF(e.discharge_date, e.encounter_date)
--             ELSE NULL
--         END AS length_of_stay_days
--     FROM encounters e
--     INNER JOIN temp_patient_lookup tpl ON e.patient_id = tpl.patient_id
--     INNER JOIN temp_provider_lookup tprl ON e.provider_id = tprl.provider_id
--     INNER JOIN temp_department_lookup tdl ON e.department_id = tdl.department_id
--     INNER JOIN temp_encounter_type_lookup tetl ON e.encounter_type = tetl.encounter_type_name
--     LEFT JOIN temp_diagnosis_counts tdc ON e.encounter_id = tdc.encounter_id
--     LEFT JOIN temp_procedure_counts tpc ON e.encounter_id = tpc.encounter_id
--     LEFT JOIN temp_billing_totals tbt ON e.encounter_id = tbt.encounter_id
--     WHERE e.encounter_id BETWEEN start_id AND end_id;
    
--     COMMIT;
--     SELECT CONCAT('✓ Fact encounters batch complete: ', start_id, ' to ', end_id) AS progress;
-- END//
-- DELIMITER ;

-- -- Load fact_encounters in batches of 100,000
-- CALL load_fact_encounters_batch(1, 100000);
-- CALL load_fact_encounters_batch(100001, 200000);
-- CALL load_fact_encounters_batch(200001, 300000);
-- CALL load_fact_encounters_batch(300001, 400000);
-- CALL load_fact_encounters_batch(400001, 500000);

-- DROP PROCEDURE load_fact_encounters_batch;
-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounters to fact table') AS status FROM fact_encounters;

-- -- ============================================================================
-- -- STEP 3: LOAD BRIDGE TABLES (BATCHED)
-- -- ============================================================================

-- SELECT 'STEP 3: Loading bridge tables (batched)...' AS status;

-- -- Create encounter key lookup for bridge tables
-- DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;
-- CREATE TEMPORARY TABLE temp_encounter_lookup (
--     encounter_id INT PRIMARY KEY,
--     encounter_key INT,
--     INDEX idx_encounter_key (encounter_key)
-- );

-- INSERT INTO temp_encounter_lookup
-- SELECT encounter_id, encounter_key FROM fact_encounters;

-- SELECT CONCAT('✓ Created encounter lookup: ', COUNT(*), ' records') AS status FROM temp_encounter_lookup;

-- -- Load bridge_encounter_diagnoses (batched)
-- DROP PROCEDURE IF EXISTS load_bridge_diagnoses_batch;
-- DELIMITER //
-- CREATE PROCEDURE load_bridge_diagnoses_batch(IN start_enc INT, IN end_enc INT)
-- BEGIN
--     START TRANSACTION;
    
--     INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
--     SELECT 
--         tel.encounter_key,
--         dd.diagnosis_key,
--         ed.diagnosis_sequence
--     FROM encounter_diagnoses ed
--     INNER JOIN temp_encounter_lookup tel ON ed.encounter_id = tel.encounter_id
--     INNER JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
--     WHERE ed.encounter_id BETWEEN start_enc AND end_enc;
    
--     COMMIT;
--     SELECT CONCAT('✓ Bridge diagnoses batch complete: ', start_enc, ' to ', end_enc) AS progress;
-- END//
-- DELIMITER ;

-- -- Load in batches of 100,000 encounters
-- CALL load_bridge_diagnoses_batch(1, 100000);
-- CALL load_bridge_diagnoses_batch(100001, 200000);
-- CALL load_bridge_diagnoses_batch(200001, 300000);
-- CALL load_bridge_diagnoses_batch(300001, 400000);
-- CALL load_bridge_diagnoses_batch(400001, 500000);

-- DROP PROCEDURE load_bridge_diagnoses_batch;
-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter-diagnosis relationships') AS status 
-- FROM bridge_encounter_diagnoses;

-- -- Load bridge_encounter_procedures (batched)
-- DROP PROCEDURE IF EXISTS load_bridge_procedures_batch;
-- DELIMITER //
-- CREATE PROCEDURE load_bridge_procedures_batch(IN start_enc INT, IN end_enc INT)
-- BEGIN
--     START TRANSACTION;
    
--     INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date, procedure_sequence)
--     SELECT 
--         tel.encounter_key,
--         dp.procedure_key,
--         ep.procedure_date,
--         ROW_NUMBER() OVER (PARTITION BY tel.encounter_key ORDER BY ep.procedure_date, ep.encounter_procedure_id) AS procedure_sequence
--     FROM encounter_procedures ep
--     INNER JOIN temp_encounter_lookup tel ON ep.encounter_id = tel.encounter_id
--     INNER JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
--     WHERE ep.encounter_id BETWEEN start_enc AND end_enc;
    
--     COMMIT;
--     SELECT CONCAT('✓ Bridge procedures batch complete: ', start_enc, ' to ', end_enc) AS progress;
-- END//
-- DELIMITER ;

-- -- Load in batches of 100,000 encounters
-- CALL load_bridge_procedures_batch(1, 100000);
-- CALL load_bridge_procedures_batch(100001, 200000);
-- CALL load_bridge_procedures_batch(200001, 300000);
-- CALL load_bridge_procedures_batch(300001, 400000);
-- CALL load_bridge_procedures_batch(400001, 500000);

-- DROP PROCEDURE load_bridge_procedures_batch;
-- SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter-procedure relationships') AS status 
-- FROM bridge_encounter_procedures;

-- -- Clean up temporary tables
-- DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
-- DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
-- DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
-- DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
-- DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
-- DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
-- DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
-- DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;

-- SET FOREIGN_KEY_CHECKS = 1;

-- -- ============================================================================
-- -- STEP 4: VALIDATION AND SUMMARY
-- -- ============================================================================

-- --SELECT '=' AS separator;
-- SELECT 'ETL PIPELINE COMPLETED - VALIDATION SUMMARY' AS status;
-- --SELECT '=' AS separator;

-- SELECT 'Table' AS table_name, 'Record Count' AS count
-- UNION ALL SELECT '---', '---'
-- UNION ALL SELECT 'dim_date', CAST(COUNT(*) AS CHAR) FROM dim_date
-- UNION ALL SELECT 'dim_patient', CAST(COUNT(*) AS CHAR) FROM dim_patient
-- UNION ALL SELECT 'dim_provider', CAST(COUNT(*) AS CHAR) FROM dim_provider
-- UNION ALL SELECT 'dim_specialty', CAST(COUNT(*) AS CHAR) FROM dim_specialty
-- UNION ALL SELECT 'dim_department', CAST(COUNT(*) AS CHAR) FROM dim_department
-- UNION ALL SELECT 'dim_encounter_type', CAST(COUNT(*) AS CHAR) FROM dim_encounter_type
-- UNION ALL SELECT 'dim_diagnosis', CAST(COUNT(*) AS CHAR) FROM dim_diagnosis
-- UNION ALL SELECT 'dim_procedure', CAST(COUNT(*) AS CHAR) FROM dim_procedure
-- UNION ALL SELECT 'fact_encounters', CAST(COUNT(*) AS CHAR) FROM fact_encounters
-- UNION ALL SELECT 'bridge_encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_diagnoses
-- UNION ALL SELECT 'bridge_encounter_procedures', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_procedures;

-- -- Validation checks
-- --SELECT '' AS separator;
-- SELECT 'VALIDATION CHECKS' AS check_type;
-- --SELECT '' AS separator;

-- SELECT 
--     'Encounters match' AS check_name,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM encounters) = (SELECT COUNT(*) FROM fact_encounters)
--         THEN '✓ PASS' ELSE '✗ FAIL'
--     END AS status,
--     CONCAT((SELECT COUNT(*) FROM encounters), ' OLTP vs ', 
--            (SELECT COUNT(*) FROM fact_encounters), ' Star') AS details;

-- SELECT 
--     'Patients match' AS check_name,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM patients) = (SELECT COUNT(*) FROM dim_patient)
--         THEN '✓ PASS' ELSE '✗ FAIL'
--     END AS status,
--     CONCAT((SELECT COUNT(*) FROM patients), ' OLTP vs ', 
--            (SELECT COUNT(*) FROM dim_patient), ' Star') AS details;

-- SELECT 
--     'Providers match' AS check_name,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM providers) = (SELECT COUNT(*) FROM dim_provider)
--         THEN '✓ PASS' ELSE '✗ FAIL'
--     END AS status,
--     CONCAT((SELECT COUNT(*) FROM providers), ' OLTP vs ', 
--            (SELECT COUNT(*) FROM dim_provider), ' Star') AS details;

-- SELECT 
--     'Diagnoses relationships match' AS check_name,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM encounter_diagnoses) = (SELECT COUNT(*) FROM bridge_encounter_diagnoses)
--         THEN '✓ PASS' ELSE '✗ FAIL'
--     END AS status,
--     CONCAT((SELECT COUNT(*) FROM encounter_diagnoses), ' OLTP vs ', 
--            (SELECT COUNT(*) FROM bridge_encounter_diagnoses), ' Star') AS details;

-- SELECT 
--     'Procedures relationships match' AS check_name,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM encounter_procedures) = (SELECT COUNT(*) FROM bridge_encounter_procedures)
--         THEN '✓ PASS' ELSE '✗ FAIL'
--     END AS status,
--     CONCAT((SELECT COUNT(*) FROM encounter_procedures), ' OLTP vs ', 
--            (SELECT COUNT(*) FROM bridge_encounter_procedures), ' Star') AS details;

-- -- Performance summary
-- --SELECT '' AS separator;
-- SELECT 'PERFORMANCE SUMMARY' AS metric_type;
-- --SELECT '' AS separator;

-- SELECT 
--     CONCAT('Total ETL execution time: ', 
--            TIMESTAMPDIFF(SECOND, @etl_start_time, NOW()), 
--            ' seconds (', 
--            ROUND(TIMESTAMPDIFF(SECOND, @etl_start_time, NOW()) / 60, 2),
--            ' minutes)') AS summary;

-- SELECT '✓ ETL Pipeline Execution Complete!' AS final_status;



-- ============================================================================
-- ETL PIPELINE: OLTP TO STAR SCHEMA
-- ============================================================================
-- Purpose: Transform and load data from normalized OLTP to star schema
-- Prerequisites: 
--   1. Run star_schema.sql (creates dimension/fact/bridge tables)
--   2. Run etl_logging_setup.sql (creates logging infrastructure)
-- Author: [Your Name]
-- Date: [Date]
-- ============================================================================

-- ============================================================================
-- INITIALIZATION
-- ============================================================================

SET @etl_start_time = NOW();
SET @batch_id = CONCAT(DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s'), '_', LPAD(FLOOR(RAND() * 1000), 3, '0'));
SET @step_start_time = NOW();
SET FOREIGN_KEY_CHECKS = 0;

-- Start ETL batch
CALL start_etl_batch(@batch_id, 'FULL_REFRESH');
SELECT CONCAT('ETL Started - Batch ID: ', @batch_id) AS status;

-- ============================================================================
-- STEP 1: LOAD DIMENSION TABLES
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'DIMENSIONS_START', 'Beginning dimension loads', NULL, NULL);

-- ----------------------------------------------------------------------------
-- 1.1: dim_date (One-time load for 2020-2030)
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_date (
    date_key, calendar_date, year, quarter, quarter_name,
    month, month_name, month_year, day_of_month, day_of_week,
    day_name, week_of_year, is_weekend, is_holiday,
    fiscal_year, fiscal_quarter, fiscal_period
)
SELECT 
    CAST(DATE_FORMAT(calendar_date, '%Y%m%d') AS UNSIGNED) AS date_key,
    calendar_date,
    YEAR(calendar_date),
    QUARTER(calendar_date),
    CONCAT('Q', QUARTER(calendar_date), ' ', YEAR(calendar_date)),
    MONTH(calendar_date),
    DATE_FORMAT(calendar_date, '%M'),
    DATE_FORMAT(calendar_date, '%M %Y'),
    DAY(calendar_date),
    DAYOFWEEK(calendar_date),
    DATE_FORMAT(calendar_date, '%W'),
    WEEK(calendar_date),
    DAYOFWEEK(calendar_date) IN (1, 7),
    FALSE,
    YEAR(calendar_date),
    QUARTER(calendar_date),
    CONCAT('FY', YEAR(calendar_date), '-Q', QUARTER(calendar_date))
FROM (
    SELECT DATE('2020-01-01') + INTERVAL n DAY AS calendar_date
    FROM (
        SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
        FROM 
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) d
    ) numbers
    WHERE DATE('2020-01-01') + INTERVAL n DAY <= DATE('2030-12-31')
) dates;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_date', CONCAT('Loaded ', @rows, ' dates'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.2: dim_specialty
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code FROM specialties;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_specialty', CONCAT('Loaded ', @rows, ' specialties'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.3: dim_department
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity FROM departments;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_department', CONCAT('Loaded ', @rows, ' departments'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.4: dim_encounter_type
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_code, requires_admission, average_duration_hours)
VALUES
    ('Outpatient', 'OP', FALSE, 1.5),
    ('Inpatient', 'IP', TRUE, 96.0),
    ('ER', 'ER', FALSE, 6.5);

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_encounter_type', CONCAT('Loaded ', @rows, ' encounter types'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.5: dim_diagnosis
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description FROM diagnoses;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_diagnosis', CONCAT('Loaded ', @rows, ' diagnoses'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.6: dim_procedure
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description FROM procedures;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_procedure', CONCAT('Loaded ', @rows, ' procedures'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ----------------------------------------------------------------------------
-- 1.7: dim_patient (Batched with validation)
-- ----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS load_dim_patient_batch;
DELIMITER //
CREATE PROCEDURE load_dim_patient_batch(IN p_batch_id VARCHAR(50), IN p_start INT, IN p_end INT)
BEGIN
    DECLARE v_start DATETIME DEFAULT NOW();
    DECLARE v_rows INT DEFAULT 0;
    DECLARE v_errors INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        CALL log_etl_event(p_batch_id, 'ERROR', 'load_dim_patient_batch', CONCAT('Failed: ', p_start, '-', p_end), 0, TIMESTAMPDIFF(SECOND, v_start, NOW()));
        ROLLBACK;
        RESIGNAL;
    END;
    
    START TRANSACTION;
    
    -- Log invalid patients
    INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
    SELECT p_batch_id, 'patients', patient_id, 'INVALID_DOB', 'NULL or future DOB', 'date_of_birth', CAST(date_of_birth AS CHAR)
    FROM patients
    WHERE patient_id BETWEEN p_start AND p_end
      AND (date_of_birth IS NULL OR date_of_birth > CURDATE());
    
    SET v_errors = ROW_COUNT();
    
    -- Load valid patients
    INSERT INTO dim_patient (patient_id, mrn, first_name, last_name, full_name, date_of_birth, gender, age_group, source_system, effective_date, is_current)
    SELECT 
        patient_id, mrn, first_name, last_name,
        CONCAT(first_name, ' ', last_name),
        date_of_birth, gender,
        CASE 
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 50 THEN '35-49'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 65 THEN '50-64'
            ELSE '65+'
        END,
        'OLTP', CURDATE(), TRUE
    FROM patients
    WHERE patient_id BETWEEN p_start AND p_end
      AND date_of_birth IS NOT NULL AND date_of_birth <= CURDATE();
    
    SET v_rows = ROW_COUNT();
    COMMIT;
    
    CALL log_etl_event(p_batch_id, 
        CASE WHEN v_errors > 0 THEN 'WARNING' ELSE 'INFO' END,
        'load_dim_patient_batch', 
        CONCAT(p_start, '-', p_end, ': ', v_rows, ' loaded, ', v_errors, ' errors'),
        v_rows, TIMESTAMPDIFF(SECOND, v_start, NOW()));
END//
DELIMITER ;

CALL load_dim_patient_batch(@batch_id, 1, 20000);
CALL load_dim_patient_batch(@batch_id, 20001, 40000);
CALL load_dim_patient_batch(@batch_id, 40001, 60000);
CALL load_dim_patient_batch(@batch_id, 60001, 80000);
CALL load_dim_patient_batch(@batch_id, 80001, 100000);

DROP PROCEDURE IF EXISTS load_dim_patient_batch;

-- ----------------------------------------------------------------------------
-- 1.8: dim_provider (with denormalized specialty)
-- ----------------------------------------------------------------------------
SET @step_start_time = NOW();

-- Log providers with missing specialty
INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
SELECT @batch_id, 'providers', p.provider_id, 'MISSING_SPECIALTY', 'Specialty not found', 'specialty_id', CAST(p.specialty_id AS CHAR)
FROM providers p LEFT JOIN specialties s ON p.specialty_id = s.specialty_id
WHERE s.specialty_id IS NULL;

INSERT INTO dim_provider (provider_id, first_name, last_name, full_name, credential, specialty_id, specialty_name, specialty_code)
SELECT p.provider_id, p.first_name, p.last_name, CONCAT('Dr. ', p.first_name, ' ', p.last_name),
       p.credential, p.specialty_id, s.specialty_name, s.specialty_code
FROM providers p INNER JOIN specialties s ON p.specialty_id = s.specialty_id;

SET @rows = ROW_COUNT();
CALL log_etl_event(@batch_id, 'INFO', 'load_dim_provider', CONCAT('Loaded ', @rows, ' providers'), @rows, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

CALL log_etl_event(@batch_id, 'INFO', 'DIMENSIONS_COMPLETE', 'All dimensions loaded', NULL, NULL);

-- ============================================================================
-- STEP 2: CREATE LOOKUP TABLES
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'LOOKUPS_START', 'Creating lookup tables', NULL, NULL);
SET @step_start_time = NOW();

DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
CREATE TEMPORARY TABLE temp_patient_lookup (patient_id INT PRIMARY KEY, patient_key INT);
INSERT INTO temp_patient_lookup SELECT patient_id, patient_key FROM dim_patient;

DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
CREATE TEMPORARY TABLE temp_provider_lookup (provider_id INT PRIMARY KEY, provider_key INT, specialty_key INT);
INSERT INTO temp_provider_lookup
SELECT p.provider_id, dp.provider_key, ds.specialty_key
FROM providers p
JOIN dim_provider dp ON p.provider_id = dp.provider_id
JOIN dim_specialty ds ON p.specialty_id = ds.specialty_id;

DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
CREATE TEMPORARY TABLE temp_department_lookup (department_id INT PRIMARY KEY, department_key INT);
INSERT INTO temp_department_lookup SELECT department_id, department_key FROM dim_department;

DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
CREATE TEMPORARY TABLE temp_encounter_type_lookup (encounter_type_name VARCHAR(50) PRIMARY KEY, encounter_type_key INT);
INSERT INTO temp_encounter_type_lookup SELECT encounter_type_name, encounter_type_key FROM dim_encounter_type;

DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
CREATE TEMPORARY TABLE temp_diagnosis_counts (encounter_id INT PRIMARY KEY, diagnosis_count INT);
INSERT INTO temp_diagnosis_counts SELECT encounter_id, COUNT(*) FROM encounter_diagnoses GROUP BY encounter_id;

DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
CREATE TEMPORARY TABLE temp_procedure_counts (encounter_id INT PRIMARY KEY, procedure_count INT);
INSERT INTO temp_procedure_counts SELECT encounter_id, COUNT(*) FROM encounter_procedures GROUP BY encounter_id;

DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
CREATE TEMPORARY TABLE temp_billing_totals (encounter_id INT PRIMARY KEY, total_claim DECIMAL(12,2), total_allowed DECIMAL(12,2));
INSERT INTO temp_billing_totals SELECT encounter_id, SUM(claim_amount), SUM(allowed_amount) FROM billing GROUP BY encounter_id;

CALL log_etl_event(@batch_id, 'INFO', 'LOOKUPS_COMPLETE', 'Lookup tables created', NULL, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ============================================================================
-- STEP 3: VALIDATE DIMENSION KEYS
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'VALIDATION_START', 'Checking for missing dimension keys', NULL, NULL);
SET @step_start_time = NOW();

-- Log missing patients
INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
SELECT @batch_id, 'encounters', e.encounter_id, 'MISSING_PATIENT', CONCAT('Patient ', e.patient_id, ' not found'), 'patient_id', CAST(e.patient_id AS CHAR)
FROM encounters e LEFT JOIN temp_patient_lookup t ON e.patient_id = t.patient_id WHERE t.patient_key IS NULL;

-- Log missing providers
INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
SELECT @batch_id, 'encounters', e.encounter_id, 'MISSING_PROVIDER', CONCAT('Provider ', e.provider_id, ' not found'), 'provider_id', CAST(e.provider_id AS CHAR)
FROM encounters e LEFT JOIN temp_provider_lookup t ON e.provider_id = t.provider_id WHERE t.provider_key IS NULL;

-- Log missing departments
INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
SELECT @batch_id, 'encounters', e.encounter_id, 'MISSING_DEPARTMENT', CONCAT('Department ', e.department_id, ' not found'), 'department_id', CAST(e.department_id AS CHAR)
FROM encounters e LEFT JOIN temp_department_lookup t ON e.department_id = t.department_id WHERE t.department_key IS NULL;

-- Log missing encounter types
INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
SELECT @batch_id, 'encounters', e.encounter_id, 'MISSING_ENCOUNTER_TYPE', CONCAT('Type "', e.encounter_type, '" not found'), 'encounter_type', e.encounter_type
FROM encounters e LEFT JOIN temp_encounter_type_lookup t ON e.encounter_type = t.encounter_type_name WHERE t.encounter_type_key IS NULL;

SET @validation_errors = (SELECT COUNT(*) FROM etl_error_records WHERE batch_id = @batch_id AND source_table = 'encounters');
CALL log_etl_event(@batch_id, 
    CASE WHEN @validation_errors > 0 THEN 'WARNING' ELSE 'INFO' END,
    'VALIDATION_COMPLETE', 
    CONCAT('Found ', @validation_errors, ' encounters with missing keys'),
    @validation_errors, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ============================================================================
-- STEP 4: LOAD FACT TABLE (BATCHED)
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'FACT_START', 'Loading fact_encounters', NULL, NULL);

DROP PROCEDURE IF EXISTS load_fact_batch;
DELIMITER //
CREATE PROCEDURE load_fact_batch(IN p_batch_id VARCHAR(50), IN p_start INT, IN p_end INT)
BEGIN
    DECLARE v_start DATETIME DEFAULT NOW();
    DECLARE v_rows INT DEFAULT 0;
    DECLARE v_expected INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        CALL log_etl_event(p_batch_id, 'ERROR', 'load_fact_batch', CONCAT('Failed: ', p_start, '-', p_end), 0, TIMESTAMPDIFF(SECOND, v_start, NOW()));
        ROLLBACK;
        RESIGNAL;
    END;
    
    SELECT COUNT(*) INTO v_expected
    FROM encounters e
    INNER JOIN temp_patient_lookup tp ON e.patient_id = tp.patient_id
    INNER JOIN temp_provider_lookup tpr ON e.provider_id = tpr.provider_id
    INNER JOIN temp_department_lookup td ON e.department_id = td.department_id
    INNER JOIN temp_encounter_type_lookup te ON e.encounter_type = te.encounter_type_name
    WHERE e.encounter_id BETWEEN p_start AND p_end;
    
    START TRANSACTION;
    
    INSERT INTO fact_encounters (
        encounter_id, date_key, patient_key, provider_key, specialty_key,
        department_key, encounter_type_key, encounter_date, discharge_date,
        diagnosis_count, procedure_count, total_claim_amount, total_allowed_amount, length_of_stay_days
    )
    SELECT 
        e.encounter_id,
        CAST(DATE_FORMAT(e.encounter_date, '%Y%m%d') AS UNSIGNED),
        tp.patient_key, tpr.provider_key, tpr.specialty_key, td.department_key, te.encounter_type_key,
        e.encounter_date, e.discharge_date,
        COALESCE(tdc.diagnosis_count, 0),
        COALESCE(tpc.procedure_count, 0),
        COALESCE(tb.total_claim, 0),
        COALESCE(tb.total_allowed, 0),
        CASE WHEN e.encounter_type = 'Inpatient' AND e.discharge_date IS NOT NULL
             THEN DATEDIFF(e.discharge_date, e.encounter_date) ELSE NULL END
    FROM encounters e
    INNER JOIN temp_patient_lookup tp ON e.patient_id = tp.patient_id
    INNER JOIN temp_provider_lookup tpr ON e.provider_id = tpr.provider_id
    INNER JOIN temp_department_lookup td ON e.department_id = td.department_id
    INNER JOIN temp_encounter_type_lookup te ON e.encounter_type = te.encounter_type_name
    LEFT JOIN temp_diagnosis_counts tdc ON e.encounter_id = tdc.encounter_id
    LEFT JOIN temp_procedure_counts tpc ON e.encounter_id = tpc.encounter_id
    LEFT JOIN temp_billing_totals tb ON e.encounter_id = tb.encounter_id
    WHERE e.encounter_id BETWEEN p_start AND p_end;
    
    SET v_rows = ROW_COUNT();
    COMMIT;
    
    UPDATE etl_batch_control SET last_successful_encounter_id = p_end WHERE batch_id = p_batch_id;
    
    CALL log_etl_event(p_batch_id,
        CASE WHEN v_rows = v_expected THEN 'INFO' ELSE 'WARNING' END,
        'load_fact_batch',
        CONCAT(p_start, '-', p_end, ': Expected ', v_expected, ', Loaded ', v_rows),
        v_rows, TIMESTAMPDIFF(SECOND, v_start, NOW()));
END//
DELIMITER ;

-- Execute fact table batches
CALL load_fact_batch(@batch_id, 1, 100000);
CALL load_fact_batch(@batch_id, 100001, 200000);
CALL load_fact_batch(@batch_id, 200001, 300000);
CALL load_fact_batch(@batch_id, 300001, 400000);
CALL load_fact_batch(@batch_id, 400001, 500000);

DROP PROCEDURE IF EXISTS load_fact_batch;

SET @fact_count = (SELECT COUNT(*) FROM fact_encounters);
CALL log_etl_event(@batch_id, 'INFO', 'FACT_COMPLETE', CONCAT('Loaded ', @fact_count, ' encounters'), @fact_count, NULL);

-- ============================================================================
-- STEP 5: LOAD BRIDGE TABLES
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'BRIDGE_START', 'Loading bridge tables', NULL, NULL);

-- Create encounter lookup for bridge tables
DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;
CREATE TEMPORARY TABLE temp_encounter_lookup (encounter_id INT PRIMARY KEY, encounter_key INT);
INSERT INTO temp_encounter_lookup SELECT encounter_id, encounter_key FROM fact_encounters;

-- ----------------------------------------------------------------------------
-- 5.1: bridge_encounter_diagnoses (Batched)
-- ----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS load_bridge_diagnoses_batch;
DELIMITER //
CREATE PROCEDURE load_bridge_diagnoses_batch(IN p_batch_id VARCHAR(50), IN p_start INT, IN p_end INT)
BEGIN
    DECLARE v_start DATETIME DEFAULT NOW();
    DECLARE v_rows INT DEFAULT 0;
    DECLARE v_orphans INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        CALL log_etl_event(p_batch_id, 'ERROR', 'load_bridge_diagnoses_batch', CONCAT('Failed: ', p_start, '-', p_end), 0, TIMESTAMPDIFF(SECOND, v_start, NOW()));
        ROLLBACK;
        RESIGNAL;
    END;
    
    -- Log orphaned records (encounter not in fact table)
    INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
    SELECT p_batch_id, 'encounter_diagnoses', ed.encounter_diagnosis_id, 'ORPHAN_DIAGNOSIS',
           CONCAT('Encounter ', ed.encounter_id, ' not in fact table'), 'encounter_id', CAST(ed.encounter_id AS CHAR)
    FROM encounter_diagnoses ed
    LEFT JOIN temp_encounter_lookup tel ON ed.encounter_id = tel.encounter_id
    WHERE ed.encounter_id BETWEEN p_start AND p_end AND tel.encounter_key IS NULL;
    
    SET v_orphans = ROW_COUNT();
    
    -- Log missing diagnosis keys
    INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
    SELECT p_batch_id, 'encounter_diagnoses', ed.encounter_diagnosis_id, 'MISSING_DIAGNOSIS_KEY',
           CONCAT('Diagnosis ', ed.diagnosis_id, ' not found'), 'diagnosis_id', CAST(ed.diagnosis_id AS CHAR)
    FROM encounter_diagnoses ed
    INNER JOIN temp_encounter_lookup tel ON ed.encounter_id = tel.encounter_id
    LEFT JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
    WHERE ed.encounter_id BETWEEN p_start AND p_end AND dd.diagnosis_key IS NULL;
    
    SET v_orphans = v_orphans + ROW_COUNT();
    
    START TRANSACTION;
    
    INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
    SELECT tel.encounter_key, dd.diagnosis_key, ed.diagnosis_sequence
    FROM encounter_diagnoses ed
    INNER JOIN temp_encounter_lookup tel ON ed.encounter_id = tel.encounter_id
    INNER JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
    WHERE ed.encounter_id BETWEEN p_start AND p_end;
    
    SET v_rows = ROW_COUNT();
    COMMIT;
    
    CALL log_etl_event(p_batch_id,
        CASE WHEN v_orphans > 0 THEN 'WARNING' ELSE 'INFO' END,
        'load_bridge_diagnoses_batch',
        CONCAT(p_start, '-', p_end, ': ', v_rows, ' loaded, ', v_orphans, ' orphans'),
        v_rows, TIMESTAMPDIFF(SECOND, v_start, NOW()));
END//
DELIMITER ;

-- Execute diagnosis bridge batches
CALL load_bridge_diagnoses_batch(@batch_id, 1, 100000);
CALL load_bridge_diagnoses_batch(@batch_id, 100001, 200000);
CALL load_bridge_diagnoses_batch(@batch_id, 200001, 300000);
CALL load_bridge_diagnoses_batch(@batch_id, 300001, 400000);
CALL load_bridge_diagnoses_batch(@batch_id, 400001, 500000);

DROP PROCEDURE IF EXISTS load_bridge_diagnoses_batch;

SET @diag_count = (SELECT COUNT(*) FROM bridge_encounter_diagnoses);
CALL log_etl_event(@batch_id, 'INFO', 'bridge_diagnoses_complete', CONCAT('Loaded ', @diag_count, ' diagnosis links'), @diag_count, NULL);

-- ----------------------------------------------------------------------------
-- 5.2: bridge_encounter_procedures (Batched)
-- ----------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS load_bridge_procedures_batch;
DELIMITER //
CREATE PROCEDURE load_bridge_procedures_batch(IN p_batch_id VARCHAR(50), IN p_start INT, IN p_end INT)
BEGIN
    DECLARE v_start DATETIME DEFAULT NOW();
    DECLARE v_rows INT DEFAULT 0;
    DECLARE v_orphans INT DEFAULT 0;
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        CALL log_etl_event(p_batch_id, 'ERROR', 'load_bridge_procedures_batch', CONCAT('Failed: ', p_start, '-', p_end), 0, TIMESTAMPDIFF(SECOND, v_start, NOW()));
        ROLLBACK;
        RESIGNAL;
    END;
    
    -- Log orphaned records
    INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
    SELECT p_batch_id, 'encounter_procedures', ep.encounter_procedure_id, 'ORPHAN_PROCEDURE',
           CONCAT('Encounter ', ep.encounter_id, ' not in fact table'), 'encounter_id', CAST(ep.encounter_id AS CHAR)
    FROM encounter_procedures ep
    LEFT JOIN temp_encounter_lookup tel ON ep.encounter_id = tel.encounter_id
    WHERE ep.encounter_id BETWEEN p_start AND p_end AND tel.encounter_key IS NULL;
    
    SET v_orphans = ROW_COUNT();
    
    -- Log missing procedure keys
    INSERT INTO etl_error_records (batch_id, source_table, source_id, error_type, error_message, field_name, field_value)
    SELECT p_batch_id, 'encounter_procedures', ep.encounter_procedure_id, 'MISSING_PROCEDURE_KEY',
           CONCAT('Procedure ', ep.procedure_id, ' not found'), 'procedure_id', CAST(ep.procedure_id AS CHAR)
    FROM encounter_procedures ep
    INNER JOIN temp_encounter_lookup tel ON ep.encounter_id = tel.encounter_id
    LEFT JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
    WHERE ep.encounter_id BETWEEN p_start AND p_end AND dp.procedure_key IS NULL;
    
    SET v_orphans = v_orphans + ROW_COUNT();
    
    START TRANSACTION;
    
    INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date, procedure_sequence)
    SELECT 
        tel.encounter_key, 
        dp.procedure_key, 
        ep.procedure_date,
        ROW_NUMBER() OVER (PARTITION BY tel.encounter_key ORDER BY ep.procedure_date, ep.encounter_procedure_id)
    FROM encounter_procedures ep
    INNER JOIN temp_encounter_lookup tel ON ep.encounter_id = tel.encounter_id
    INNER JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
    WHERE ep.encounter_id BETWEEN p_start AND p_end;
    
    SET v_rows = ROW_COUNT();
    COMMIT;
    
    CALL log_etl_event(p_batch_id,
        CASE WHEN v_orphans > 0 THEN 'WARNING' ELSE 'INFO' END,
        'load_bridge_procedures_batch',
        CONCAT(p_start, '-', p_end, ': ', v_rows, ' loaded, ', v_orphans, ' orphans'),
        v_rows, TIMESTAMPDIFF(SECOND, v_start, NOW()));
END//
DELIMITER ;

-- Execute procedure bridge batches
CALL load_bridge_procedures_batch(@batch_id, 1, 100000);
CALL load_bridge_procedures_batch(@batch_id, 100001, 200000);
CALL load_bridge_procedures_batch(@batch_id, 200001, 300000);
CALL load_bridge_procedures_batch(@batch_id, 300001, 400000);
CALL load_bridge_procedures_batch(@batch_id, 400001, 500000);

DROP PROCEDURE IF EXISTS load_bridge_procedures_batch;

SET @proc_count = (SELECT COUNT(*) FROM bridge_encounter_procedures);
CALL log_etl_event(@batch_id, 'INFO', 'bridge_procedures_complete', CONCAT('Loaded ', @proc_count, ' procedure links'), @proc_count, NULL);

CALL log_etl_event(@batch_id, 'INFO', 'BRIDGE_COMPLETE', 'All bridge tables loaded', NULL, NULL);

-- ============================================================================
-- STEP 6: CLEANUP TEMPORARY TABLES
-- ============================================================================

DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;

SET FOREIGN_KEY_CHECKS = 1;

CALL log_etl_event(@batch_id, 'INFO', 'CLEANUP_COMPLETE', 'Temporary tables dropped', NULL, NULL);

-- ============================================================================
-- STEP 7: DATA QUALITY VALIDATION
-- ============================================================================

CALL log_etl_event(@batch_id, 'INFO', 'DQ_START', 'Running data quality checks', NULL, NULL);
SET @step_start_time = NOW();

-- Check 1: Encounter counts
SET @oltp_enc = (SELECT COUNT(*) FROM encounters);
SET @star_enc = (SELECT COUNT(*) FROM fact_encounters);
SET @enc_match = (@oltp_enc = @star_enc);

CALL log_etl_event(@batch_id,
    CASE WHEN @enc_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_encounter_count',
    CONCAT('OLTP: ', @oltp_enc, ', Star: ', @star_enc, ' - ', CASE WHEN @enc_match THEN 'MATCH' ELSE 'MISMATCH' END),
    @star_enc, NULL);

-- Check 2: Patient counts
SET @oltp_pat = (SELECT COUNT(*) FROM patients);
SET @star_pat = (SELECT COUNT(*) FROM dim_patient);
SET @pat_match = (@oltp_pat = @star_pat);

CALL log_etl_event(@batch_id,
    CASE WHEN @pat_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_patient_count',
    CONCAT('OLTP: ', @oltp_pat, ', Star: ', @star_pat, ' - ', CASE WHEN @pat_match THEN 'MATCH' ELSE 'MISMATCH' END),
    @star_pat, NULL);

-- Check 3: Provider counts
SET @oltp_prov = (SELECT COUNT(*) FROM providers);
SET @star_prov = (SELECT COUNT(*) FROM dim_provider);
SET @prov_match = (@oltp_prov = @star_prov);

CALL log_etl_event(@batch_id,
    CASE WHEN @prov_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_provider_count',
    CONCAT('OLTP: ', @oltp_prov, ', Star: ', @star_prov, ' - ', CASE WHEN @prov_match THEN 'MATCH' ELSE 'MISMATCH' END),
    @star_prov, NULL);

-- Check 4: Diagnosis relationship counts
SET @oltp_diag = (SELECT COUNT(*) FROM encounter_diagnoses);
SET @star_diag = (SELECT COUNT(*) FROM bridge_encounter_diagnoses);
SET @diag_match = (@oltp_diag = @star_diag);

CALL log_etl_event(@batch_id,
    CASE WHEN @diag_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_diagnosis_links',
    CONCAT('OLTP: ', @oltp_diag, ', Star: ', @star_diag, ' - ', CASE WHEN @diag_match THEN 'MATCH' ELSE 'MISMATCH' END),
    @star_diag, NULL);

-- Check 5: Procedure relationship counts
SET @oltp_proc = (SELECT COUNT(*) FROM encounter_procedures);
SET @star_proc = (SELECT COUNT(*) FROM bridge_encounter_procedures);
SET @proc_match = (@oltp_proc = @star_proc);

CALL log_etl_event(@batch_id,
    CASE WHEN @proc_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_procedure_links',
    CONCAT('OLTP: ', @oltp_proc, ', Star: ', @star_proc, ' - ', CASE WHEN @proc_match THEN 'MATCH' ELSE 'MISMATCH' END),
    @star_proc, NULL);

-- Check 6: Revenue totals
SET @oltp_rev = (SELECT COALESCE(SUM(allowed_amount), 0) FROM billing);
SET @star_rev = (SELECT COALESCE(SUM(total_allowed_amount), 0) FROM fact_encounters);
SET @rev_diff = ABS(@oltp_rev - @star_rev);
SET @rev_match = (@rev_diff < 0.01);

CALL log_etl_event(@batch_id,
    CASE WHEN @rev_match THEN 'INFO' ELSE 'ERROR' END,
    'DQ_revenue_totals',
    CONCAT('OLTP: $', FORMAT(@oltp_rev, 2), ', Star: $', FORMAT(@star_rev, 2), ', Diff: $', FORMAT(@rev_diff, 2)),
    NULL, NULL);

-- Check 7: NULL foreign keys in fact table
SET @null_fks = (
    SELECT COUNT(*) FROM fact_encounters 
    WHERE date_key IS NULL OR patient_key IS NULL OR provider_key IS NULL 
       OR specialty_key IS NULL OR department_key IS NULL OR encounter_type_key IS NULL
);

CALL log_etl_event(@batch_id,
    CASE WHEN @null_fks = 0 THEN 'INFO' ELSE 'ERROR' END,
    'DQ_null_foreign_keys',
    CONCAT('Fact records with NULL FKs: ', @null_fks, ' - ', CASE WHEN @null_fks = 0 THEN 'PASS' ELSE 'FAIL' END),
    @null_fks, NULL);

-- Check 8: Orphaned bridge records
SET @orphan_diag = (
    SELECT COUNT(*) FROM bridge_encounter_diagnoses bd
    LEFT JOIN fact_encounters f ON bd.encounter_key = f.encounter_key
    WHERE f.encounter_key IS NULL
);
SET @orphan_proc = (
    SELECT COUNT(*) FROM bridge_encounter_procedures bp
    LEFT JOIN fact_encounters f ON bp.encounter_key = f.encounter_key
    WHERE f.encounter_key IS NULL
);

CALL log_etl_event(@batch_id,
    CASE WHEN (@orphan_diag + @orphan_proc) = 0 THEN 'INFO' ELSE 'ERROR' END,
    'DQ_orphan_bridge',
    CONCAT('Orphaned - Diagnoses: ', @orphan_diag, ', Procedures: ', @orphan_proc),
    @orphan_diag + @orphan_proc, NULL);

-- Check 9: Sample diagnosis count verification
SET @sample_enc = (SELECT encounter_id FROM encounters ORDER BY RAND() LIMIT 1);
SET @oltp_sample = (SELECT COUNT(*) FROM encounter_diagnoses WHERE encounter_id = @sample_enc);
SET @star_sample = (SELECT diagnosis_count FROM fact_encounters WHERE encounter_id = @sample_enc);
SET @sample_match = (@oltp_sample = COALESCE(@star_sample, 0));

CALL log_etl_event(@batch_id,
    CASE WHEN @sample_match THEN 'INFO' ELSE 'WARNING' END,
    'DQ_sample_diagnosis',
    CONCAT('Encounter ', @sample_enc, ': OLTP=', @oltp_sample, ', Star=', COALESCE(@star_sample, 0)),
    NULL, NULL);

CALL log_etl_event(@batch_id, 'INFO', 'DQ_COMPLETE', 'Data quality checks finished', NULL, TIMESTAMPDIFF(SECOND, @step_start_time, NOW()));

-- ============================================================================
-- STEP 8: COMPLETE ETL BATCH
-- ============================================================================

SET @total_errors = (SELECT COUNT(*) FROM etl_error_records WHERE batch_id = @batch_id);
SET @critical_errors = (SELECT COUNT(*) FROM etl_log WHERE batch_id = @batch_id AND log_level = 'ERROR');

SET @final_status = CASE 
    WHEN @critical_errors > 0 THEN 'FAILED'
    WHEN @total_errors > 0 THEN 'COMPLETED_WITH_WARNINGS'
    ELSE 'COMPLETED'
END;

SET @duration_sec = TIMESTAMPDIFF(SECOND, @etl_start_time, NOW());
SET @duration_min = ROUND(@duration_sec / 60, 2);

SET @final_notes = CONCAT(
    'Duration: ', @duration_min, ' min. ',
    'Encounters: ', (SELECT COUNT(*) FROM fact_encounters), ', ',
    'Patients: ', (SELECT COUNT(*) FROM dim_patient), ', ',
    'Errors: ', @total_errors, ', Critical: ', @critical_errors
);

CALL complete_etl_batch(@batch_id, @final_status, @final_notes);

-- ============================================================================
-- STEP 9: FINAL SUMMARY OUTPUT
-- ============================================================================

SELECT '============================================' AS '';
SELECT 'ETL PIPELINE EXECUTION SUMMARY' AS title;
SELECT '============================================' AS '';

SELECT 
    @batch_id AS batch_id,
    @final_status AS status,
    @duration_min AS duration_minutes,
    @total_errors AS total_errors,
    @critical_errors AS critical_errors;

SELECT '--- TABLE COUNTS ---' AS '';

SELECT 'dim_date' AS table_name, COUNT(*) AS records FROM dim_date
UNION ALL SELECT 'dim_patient', COUNT(*) FROM dim_patient
UNION ALL SELECT 'dim_provider', COUNT(*) FROM dim_provider
UNION ALL SELECT 'dim_specialty', COUNT(*) FROM dim_specialty
UNION ALL SELECT 'dim_department', COUNT(*) FROM dim_department
UNION ALL SELECT 'dim_encounter_type', COUNT(*) FROM dim_encounter_type
UNION ALL SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis
UNION ALL SELECT 'dim_procedure', COUNT(*) FROM dim_procedure
UNION ALL SELECT 'fact_encounters', COUNT(*) FROM fact_encounters
UNION ALL SELECT 'bridge_encounter_diagnoses', COUNT(*) FROM bridge_encounter_diagnoses
UNION ALL SELECT 'bridge_encounter_procedures', COUNT(*) FROM bridge_encounter_procedures;

SELECT '--- DATA QUALITY SUMMARY ---' AS '';

SELECT 
    'Encounters' AS check_name, @oltp_enc AS oltp, @star_enc AS star, CASE WHEN @enc_match THEN 'PASS' ELSE 'FAIL' END AS result
UNION ALL SELECT 'Patients', @oltp_pat, @star_pat, CASE WHEN @pat_match THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT 'Providers', @oltp_prov, @star_prov, CASE WHEN @prov_match THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT 'Diagnosis Links', @oltp_diag, @star_diag, CASE WHEN @diag_match THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT 'Procedure Links', @oltp_proc, @star_proc, CASE WHEN @proc_match THEN 'PASS' ELSE 'FAIL' END;

SELECT '--- ERROR SUMMARY ---' AS '';

SELECT error_type, COUNT(*) AS count
FROM etl_error_records
WHERE batch_id = @batch_id
GROUP BY error_type
ORDER BY count DESC;

SELECT '--- RECENT LOG ENTRIES ---' AS '';

SELECT 
    DATE_FORMAT(log_timestamp, '%H:%i:%s') AS time,
    log_level,
    step_name,
    LEFT(message, 60) AS message,
    rows_affected,
    execution_time_seconds AS seconds
FROM etl_log
WHERE batch_id = @batch_id
ORDER BY log_id DESC
LIMIT 15;

SELECT '============================================' AS '';
SELECT CONCAT('ETL Complete: ', @final_status) AS final_status;
SELECT CONCAT('Batch ID: ', @batch_id) AS batch_reference;
SELECT CONCAT('Duration: ', @duration_min, ' minutes') AS duration;
SELECT '============================================' AS '';

