-- ============================================================================
-- OPTIMIZED ETL PIPELINE: OLTP → STAR SCHEMA (BATCH PROCESSING)
-- ============================================================================
-- Purpose: Transform and load data from normalized schema to star schema
-- Optimized for: 500K+ encounters with batch processing and commits
-- Run after: clean_star_schema.sql
-- ============================================================================

SET @etl_start_time = NOW();
SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================================
-- STEP 1: LOAD DIMENSION TABLES (Small tables - no batching needed)
-- ============================================================================

SELECT 'STEP 1: Loading dimension tables...' AS status;

-- Load dim_date (for date range covering your data)
INSERT INTO dim_date (
    date_key, calendar_date, year, quarter, quarter_name,
    month, month_name, month_year, day_of_month, day_of_week,
    day_name, week_of_year, is_weekend, is_holiday,
    fiscal_year, fiscal_quarter, fiscal_period
)
SELECT 
    DATE_FORMAT(calendar_date, '%Y%m%d') AS date_key,
    calendar_date,
    YEAR(calendar_date) AS year,
    QUARTER(calendar_date) AS quarter,
    CONCAT('Q', QUARTER(calendar_date), ' ', YEAR(calendar_date)) AS quarter_name,
    MONTH(calendar_date) AS month,
    DATE_FORMAT(calendar_date, '%M') AS month_name,
    DATE_FORMAT(calendar_date, '%M %Y') AS month_year,
    DAY(calendar_date) AS day_of_month,
    DAYOFWEEK(calendar_date) AS day_of_week,
    DATE_FORMAT(calendar_date, '%W') AS day_name,
    WEEK(calendar_date) AS week_of_year,
    CASE WHEN DAYOFWEEK(calendar_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    YEAR(calendar_date) AS fiscal_year,
    QUARTER(calendar_date) AS fiscal_quarter,
    CONCAT('FY', YEAR(calendar_date), '-Q', QUARTER(calendar_date)) AS fiscal_period
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

SELECT CONCAT('✓ Loaded ', COUNT(*), ' dates') AS status FROM dim_date;

-- Load dim_specialty
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code
FROM specialties;

SELECT CONCAT('✓ Loaded ', COUNT(*), ' specialties') AS status FROM dim_specialty;

-- Load dim_department
INSERT INTO dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity
FROM departments;

SELECT CONCAT('✓ Loaded ', COUNT(*), ' departments') AS status FROM dim_department;

-- Load dim_encounter_type
INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_code, requires_admission, average_duration_hours)
VALUES
    ('Outpatient', 'OP', FALSE, 1.5),
    ('Inpatient', 'IP', TRUE, 96.0),
    ('ER', 'ER', FALSE, 6.5);

SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter types') AS status FROM dim_encounter_type;

-- Load dim_diagnosis
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description
FROM diagnoses;

SELECT CONCAT('✓ Loaded ', COUNT(*), ' diagnoses') AS status FROM dim_diagnosis;

-- Load dim_procedure
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description
FROM procedures;

SELECT CONCAT('✓ Loaded ', COUNT(*), ' procedures') AS status FROM dim_procedure;

-- Load dim_patient (batched for safety with 100K patients)
DROP PROCEDURE IF EXISTS load_dim_patient_batch;
DELIMITER //
CREATE PROCEDURE load_dim_patient_batch(IN start_id INT, IN end_id INT)
BEGIN
    START TRANSACTION;
    
    INSERT INTO dim_patient (
        patient_id, mrn, first_name, last_name, full_name,
        date_of_birth, gender, age_group, source_system, effective_date, is_current
    )
    SELECT 
        patient_id,
        mrn,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        date_of_birth,
        gender,
        CASE 
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 18 THEN '0-17'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 35 THEN '18-34'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 50 THEN '35-49'
            WHEN TIMESTAMPDIFF(YEAR, date_of_birth, CURDATE()) < 65 THEN '50-64'
            ELSE '65+'
        END AS age_group,
        'OLTP' AS source_system,
        CURDATE() AS effective_date,
        TRUE AS is_current
    FROM patients
    WHERE patient_id BETWEEN start_id AND end_id;
    
    COMMIT;
    SELECT CONCAT('✓ Patient batch complete: ', start_id, ' to ', end_id) AS progress;
END//
DELIMITER ;

-- Load patients in batches of 20,000
CALL load_dim_patient_batch(1, 20000);
CALL load_dim_patient_batch(20001, 40000);
CALL load_dim_patient_batch(40001, 60000);
CALL load_dim_patient_batch(60001, 80000);
CALL load_dim_patient_batch(80001, 100000);

DROP PROCEDURE load_dim_patient_batch;
SELECT CONCAT('✓ Loaded ', COUNT(*), ' patients') AS status FROM dim_patient;

-- Load dim_provider (with denormalized specialty)
INSERT INTO dim_provider (
    provider_id, first_name, last_name, full_name, credential,
    specialty_id, specialty_name, specialty_code
)
SELECT 
    p.provider_id,
    p.first_name,
    p.last_name,
    CONCAT('Dr. ', p.first_name, ' ', p.last_name) AS full_name,
    p.credential,
    p.specialty_id,
    s.specialty_name,
    s.specialty_code
FROM providers p
JOIN specialties s ON p.specialty_id = s.specialty_id;

SELECT CONCAT('✓ Loaded ', COUNT(*), ' providers') AS status FROM dim_provider;

-- ============================================================================
-- STEP 2: LOAD FACT TABLE (BATCHED - CRITICAL FOR PERFORMANCE)
-- ============================================================================

SELECT 'STEP 2: Loading fact table (batched)...' AS status;

-- Create lookup tables for faster joins (avoids subqueries in main insert)
DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
CREATE TEMPORARY TABLE temp_patient_lookup (
    patient_id INT PRIMARY KEY,
    patient_key INT,
    INDEX idx_patient_key (patient_key)
);

INSERT INTO temp_patient_lookup
SELECT patient_id, patient_key FROM dim_patient;

SELECT CONCAT('✓ Created patient lookup: ', COUNT(*), ' records') AS status FROM temp_patient_lookup;

DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
CREATE TEMPORARY TABLE temp_provider_lookup (
    provider_id INT PRIMARY KEY,
    provider_key INT,
    specialty_key INT,
    INDEX idx_provider_key (provider_key)
);

INSERT INTO temp_provider_lookup
SELECT 
    p.provider_id,
    dp.provider_key,
    ds.specialty_key
FROM providers p
JOIN dim_provider dp ON p.provider_id = dp.provider_id
JOIN dim_specialty ds ON p.specialty_id = ds.specialty_id;

SELECT CONCAT('✓ Created provider lookup: ', COUNT(*), ' records') AS status FROM temp_provider_lookup;

DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
CREATE TEMPORARY TABLE temp_department_lookup (
    department_id INT PRIMARY KEY,
    department_key INT,
    INDEX idx_department_key (department_key)
);

INSERT INTO temp_department_lookup
SELECT department_id, department_key FROM dim_department;

SELECT CONCAT('✓ Created department lookup: ', COUNT(*), ' records') AS status FROM temp_department_lookup;

DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
CREATE TEMPORARY TABLE temp_encounter_type_lookup (
    encounter_type_name VARCHAR(50) PRIMARY KEY,
    encounter_type_key INT,
    INDEX idx_encounter_type_key (encounter_type_key)
);

INSERT INTO temp_encounter_type_lookup
SELECT encounter_type_name, encounter_type_key FROM dim_encounter_type;

SELECT CONCAT('✓ Created encounter type lookup: ', COUNT(*), ' records') AS status FROM temp_encounter_type_lookup;

-- Create aggregation temp tables for diagnoses and procedures counts
DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
CREATE TEMPORARY TABLE temp_diagnosis_counts (
    encounter_id INT PRIMARY KEY,
    diagnosis_count INT,
    INDEX idx_encounter_id (encounter_id)
);

INSERT INTO temp_diagnosis_counts
SELECT encounter_id, COUNT(*) AS diagnosis_count
FROM encounter_diagnoses
GROUP BY encounter_id;

SELECT CONCAT('✓ Pre-aggregated diagnosis counts: ', COUNT(*), ' encounters') AS status FROM temp_diagnosis_counts;

DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
CREATE TEMPORARY TABLE temp_procedure_counts (
    encounter_id INT PRIMARY KEY,
    procedure_count INT,
    INDEX idx_encounter_id (encounter_id)
);

INSERT INTO temp_procedure_counts
SELECT encounter_id, COUNT(*) AS procedure_count
FROM encounter_procedures
GROUP BY encounter_id;

SELECT CONCAT('✓ Pre-aggregated procedure counts: ', COUNT(*), ' encounters') AS status FROM temp_procedure_counts;

DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
CREATE TEMPORARY TABLE temp_billing_totals (
    encounter_id INT PRIMARY KEY,
    total_claim_amount DECIMAL(12,2),
    total_allowed_amount DECIMAL(12,2),
    claim_date DATE,
    INDEX idx_encounter_id (encounter_id)
);

INSERT INTO temp_billing_totals
SELECT 
    encounter_id,
    SUM(claim_amount) AS total_claim_amount,
    SUM(allowed_amount) AS total_allowed_amount,
    claim_date
FROM billing
GROUP BY encounter_id;

SELECT CONCAT('✓ Pre-aggregated billing totals: ', COUNT(*), ' encounters') AS status FROM temp_billing_totals;

-- Now load fact_encounters in batches
DROP PROCEDURE IF EXISTS load_fact_encounters_batch;
DELIMITER //
CREATE PROCEDURE load_fact_encounters_batch(IN start_id INT, IN end_id INT)
BEGIN
    START TRANSACTION;
    
    INSERT INTO fact_encounters (
        encounter_id,
        date_key,
        patient_key,
        provider_key,
        specialty_key,
        department_key,
        encounter_type_key,
        encounter_date,
        discharge_date,
        diagnosis_count,
        procedure_count,
        total_claim_amount,
        total_allowed_amount,
        length_of_stay_days
    )
    SELECT 
        e.encounter_id,
        DATE_FORMAT(e.encounter_date, '%Y%m%d') AS date_key,
        tpl.patient_key,
        tprl.provider_key,
        tprl.specialty_key,
        tdl.department_key,
        tetl.encounter_type_key,
        e.encounter_date,
        e.discharge_date,
        COALESCE(tdc.diagnosis_count, 0) AS diagnosis_count,
        COALESCE(tpc.procedure_count, 0) AS procedure_count,
        COALESCE(tbt.total_claim_amount, 0) AS total_claim_amount,
        COALESCE(tbt.total_allowed_amount, 0) AS total_allowed_amount,
        CASE 
            WHEN e.encounter_type = 'Inpatient' AND e.discharge_date IS NOT NULL
            THEN DATEDIFF(e.discharge_date, e.encounter_date)
            ELSE NULL
        END AS length_of_stay_days
    FROM encounters e
    INNER JOIN temp_patient_lookup tpl ON e.patient_id = tpl.patient_id
    INNER JOIN temp_provider_lookup tprl ON e.provider_id = tprl.provider_id
    INNER JOIN temp_department_lookup tdl ON e.department_id = tdl.department_id
    INNER JOIN temp_encounter_type_lookup tetl ON e.encounter_type = tetl.encounter_type_name
    LEFT JOIN temp_diagnosis_counts tdc ON e.encounter_id = tdc.encounter_id
    LEFT JOIN temp_procedure_counts tpc ON e.encounter_id = tpc.encounter_id
    LEFT JOIN temp_billing_totals tbt ON e.encounter_id = tbt.encounter_id
    WHERE e.encounter_id BETWEEN start_id AND end_id;
    
    COMMIT;
    SELECT CONCAT('✓ Fact encounters batch complete: ', start_id, ' to ', end_id) AS progress;
END//
DELIMITER ;

-- Load fact_encounters in batches of 100,000
CALL load_fact_encounters_batch(1, 100000);
CALL load_fact_encounters_batch(100001, 200000);
CALL load_fact_encounters_batch(200001, 300000);
CALL load_fact_encounters_batch(300001, 400000);
CALL load_fact_encounters_batch(400001, 500000);

DROP PROCEDURE load_fact_encounters_batch;
SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounters to fact table') AS status FROM fact_encounters;

-- ============================================================================
-- STEP 3: LOAD BRIDGE TABLES (BATCHED)
-- ============================================================================

SELECT 'STEP 3: Loading bridge tables (batched)...' AS status;

-- Create encounter key lookup for bridge tables
DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;
CREATE TEMPORARY TABLE temp_encounter_lookup (
    encounter_id INT PRIMARY KEY,
    encounter_key INT,
    INDEX idx_encounter_key (encounter_key)
);

INSERT INTO temp_encounter_lookup
SELECT encounter_id, encounter_key FROM fact_encounters;

SELECT CONCAT('✓ Created encounter lookup: ', COUNT(*), ' records') AS status FROM temp_encounter_lookup;

-- Load bridge_encounter_diagnoses (batched)
DROP PROCEDURE IF EXISTS load_bridge_diagnoses_batch;
DELIMITER //
CREATE PROCEDURE load_bridge_diagnoses_batch(IN start_enc INT, IN end_enc INT)
BEGIN
    START TRANSACTION;
    
    INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
    SELECT 
        tel.encounter_key,
        dd.diagnosis_key,
        ed.diagnosis_sequence
    FROM encounter_diagnoses ed
    INNER JOIN temp_encounter_lookup tel ON ed.encounter_id = tel.encounter_id
    INNER JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
    WHERE ed.encounter_id BETWEEN start_enc AND end_enc;
    
    COMMIT;
    SELECT CONCAT('✓ Bridge diagnoses batch complete: ', start_enc, ' to ', end_enc) AS progress;
END//
DELIMITER ;

-- Load in batches of 100,000 encounters
CALL load_bridge_diagnoses_batch(1, 100000);
CALL load_bridge_diagnoses_batch(100001, 200000);
CALL load_bridge_diagnoses_batch(200001, 300000);
CALL load_bridge_diagnoses_batch(300001, 400000);
CALL load_bridge_diagnoses_batch(400001, 500000);

DROP PROCEDURE load_bridge_diagnoses_batch;
SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter-diagnosis relationships') AS status 
FROM bridge_encounter_diagnoses;

-- Load bridge_encounter_procedures (batched)
DROP PROCEDURE IF EXISTS load_bridge_procedures_batch;
DELIMITER //
CREATE PROCEDURE load_bridge_procedures_batch(IN start_enc INT, IN end_enc INT)
BEGIN
    START TRANSACTION;
    
    INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date, procedure_sequence)
    SELECT 
        tel.encounter_key,
        dp.procedure_key,
        ep.procedure_date,
        ROW_NUMBER() OVER (PARTITION BY tel.encounter_key ORDER BY ep.procedure_date, ep.encounter_procedure_id) AS procedure_sequence
    FROM encounter_procedures ep
    INNER JOIN temp_encounter_lookup tel ON ep.encounter_id = tel.encounter_id
    INNER JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
    WHERE ep.encounter_id BETWEEN start_enc AND end_enc;
    
    COMMIT;
    SELECT CONCAT('✓ Bridge procedures batch complete: ', start_enc, ' to ', end_enc) AS progress;
END//
DELIMITER ;

-- Load in batches of 100,000 encounters
CALL load_bridge_procedures_batch(1, 100000);
CALL load_bridge_procedures_batch(100001, 200000);
CALL load_bridge_procedures_batch(200001, 300000);
CALL load_bridge_procedures_batch(300001, 400000);
CALL load_bridge_procedures_batch(400001, 500000);

DROP PROCEDURE load_bridge_procedures_batch;
SELECT CONCAT('✓ Loaded ', COUNT(*), ' encounter-procedure relationships') AS status 
FROM bridge_encounter_procedures;

-- Clean up temporary tables
DROP TEMPORARY TABLE IF EXISTS temp_patient_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_provider_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_department_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_encounter_type_lookup;
DROP TEMPORARY TABLE IF EXISTS temp_diagnosis_counts;
DROP TEMPORARY TABLE IF EXISTS temp_procedure_counts;
DROP TEMPORARY TABLE IF EXISTS temp_billing_totals;
DROP TEMPORARY TABLE IF EXISTS temp_encounter_lookup;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- STEP 4: VALIDATION AND SUMMARY
-- ============================================================================

--SELECT '=' AS separator;
SELECT 'ETL PIPELINE COMPLETED - VALIDATION SUMMARY' AS status;
--SELECT '=' AS separator;

SELECT 'Table' AS table_name, 'Record Count' AS count
UNION ALL SELECT '---', '---'
UNION ALL SELECT 'dim_date', CAST(COUNT(*) AS CHAR) FROM dim_date
UNION ALL SELECT 'dim_patient', CAST(COUNT(*) AS CHAR) FROM dim_patient
UNION ALL SELECT 'dim_provider', CAST(COUNT(*) AS CHAR) FROM dim_provider
UNION ALL SELECT 'dim_specialty', CAST(COUNT(*) AS CHAR) FROM dim_specialty
UNION ALL SELECT 'dim_department', CAST(COUNT(*) AS CHAR) FROM dim_department
UNION ALL SELECT 'dim_encounter_type', CAST(COUNT(*) AS CHAR) FROM dim_encounter_type
UNION ALL SELECT 'dim_diagnosis', CAST(COUNT(*) AS CHAR) FROM dim_diagnosis
UNION ALL SELECT 'dim_procedure', CAST(COUNT(*) AS CHAR) FROM dim_procedure
UNION ALL SELECT 'fact_encounters', CAST(COUNT(*) AS CHAR) FROM fact_encounters
UNION ALL SELECT 'bridge_encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_diagnoses
UNION ALL SELECT 'bridge_encounter_procedures', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_procedures;

-- Validation checks
--SELECT '' AS separator;
SELECT 'VALIDATION CHECKS' AS check_type;
--SELECT '' AS separator;

SELECT 
    'Encounters match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM encounters) = (SELECT COUNT(*) FROM fact_encounters)
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM encounters), ' OLTP vs ', 
           (SELECT COUNT(*) FROM fact_encounters), ' Star') AS details;

SELECT 
    'Patients match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM patients) = (SELECT COUNT(*) FROM dim_patient)
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM patients), ' OLTP vs ', 
           (SELECT COUNT(*) FROM dim_patient), ' Star') AS details;

SELECT 
    'Providers match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM providers) = (SELECT COUNT(*) FROM dim_provider)
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM providers), ' OLTP vs ', 
           (SELECT COUNT(*) FROM dim_provider), ' Star') AS details;

SELECT 
    'Diagnoses relationships match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM encounter_diagnoses) = (SELECT COUNT(*) FROM bridge_encounter_diagnoses)
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM encounter_diagnoses), ' OLTP vs ', 
           (SELECT COUNT(*) FROM bridge_encounter_diagnoses), ' Star') AS details;

SELECT 
    'Procedures relationships match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM encounter_procedures) = (SELECT COUNT(*) FROM bridge_encounter_procedures)
        THEN '✓ PASS' ELSE '✗ FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM encounter_procedures), ' OLTP vs ', 
           (SELECT COUNT(*) FROM bridge_encounter_procedures), ' Star') AS details;

-- Performance summary
--SELECT '' AS separator;
SELECT 'PERFORMANCE SUMMARY' AS metric_type;
--SELECT '' AS separator;

SELECT 
    CONCAT('Total ETL execution time: ', 
           TIMESTAMPDIFF(SECOND, @etl_start_time, NOW()), 
           ' seconds (', 
           ROUND(TIMESTAMPDIFF(SECOND, @etl_start_time, NOW()) / 60, 2),
           ' minutes)') AS summary;

SELECT '✓ ETL Pipeline Execution Complete!' AS final_status;