-- ============================================================================
-- COMPLETE ETL PIPELINE: OLTP → STAR SCHEMA
-- ============================================================================
-- Purpose: Transform and load data from normalized schema to star schema
-- Run after: clean_star_schema.sql
-- ============================================================================

SET FOREIGN_KEY_CHECKS = 0;

-- ============================================================================
-- STEP 1: LOAD DIMENSION TABLES
-- ============================================================================

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

SELECT CONCAT('Loaded ', COUNT(*), ' dates') AS status FROM dim_date;

-- Load dim_specialty
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code
FROM specialties;

SELECT CONCAT('Loaded ', COUNT(*), ' specialties') AS status FROM dim_specialty;

-- Load dim_department
INSERT INTO dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity
FROM departments;

SELECT CONCAT('Loaded ', COUNT(*), ' departments') AS status FROM dim_department;

-- Load dim_encounter_type
INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_code, requires_admission, average_duration_hours)
VALUES
    ('Outpatient', 'OP', FALSE, 1.5),
    ('Inpatient', 'IP', TRUE, 96.0),
    ('Emergency', 'ER', FALSE, 6.5);

SELECT CONCAT('Loaded ', COUNT(*), ' encounter types') AS status FROM dim_encounter_type;

-- Load dim_diagnosis
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description
FROM diagnoses;

SELECT CONCAT('Loaded ', COUNT(*), ' diagnoses') AS status FROM dim_diagnosis;

-- Load dim_procedure
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description
FROM procedures;

SELECT CONCAT('Loaded ', COUNT(*), ' procedures') AS status FROM dim_procedure;

-- Load dim_patient
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
FROM patients;

SELECT CONCAT('Loaded ', COUNT(*), ' patients') AS status FROM dim_patient;

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
    s.specialty_name,  -- DENORMALIZED
    s.specialty_code   -- DENORMALIZED
FROM providers p
JOIN specialties s ON p.specialty_id = s.specialty_id;

SELECT CONCAT('Loaded ', COUNT(*), ' providers') AS status FROM dim_provider;

-- ============================================================================
-- STEP 2: LOAD FACT TABLE
-- ============================================================================

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
    -- Date key lookup
    DATE_FORMAT(e.encounter_date, '%Y%m%d') AS date_key,
    
    -- Patient key lookup
    (SELECT patient_key FROM dim_patient WHERE patient_id = e.patient_id) AS patient_key,
    
    -- Provider key lookup
    (SELECT provider_key FROM dim_provider WHERE provider_id = e.provider_id) AS provider_key,
    
    -- Specialty key lookup (from provider's specialty)
    (SELECT specialty_key FROM dim_specialty WHERE specialty_id = 
        (SELECT specialty_id FROM providers WHERE provider_id = e.provider_id)
    ) AS specialty_key,
    
    -- Department key lookup
    (SELECT department_key FROM dim_department WHERE department_id = e.department_id) AS department_key,
    
    -- Encounter type key lookup
    (SELECT encounter_type_key FROM dim_encounter_type WHERE encounter_type_name = e.encounter_type) AS encounter_type_key,
    
    -- Degenerate dimensions
    e.encounter_date,
    e.discharge_date,
    
    -- PRE-AGGREGATED METRICS
    -- Diagnosis count
    (SELECT COUNT(*) FROM encounter_diagnoses WHERE encounter_id = e.encounter_id) AS diagnosis_count,
    
    -- Procedure count
    (SELECT COUNT(*) FROM encounter_procedures WHERE encounter_id = e.encounter_id) AS procedure_count,
    
    -- Total claim amount
    (SELECT SUM(claim_amount) FROM billing WHERE encounter_id = e.encounter_id) AS total_claim_amount,
    
    -- Total allowed amount
    (SELECT SUM(allowed_amount) FROM billing WHERE encounter_id = e.encounter_id) AS total_allowed_amount,
    
    -- Length of stay (for inpatient only)
    CASE 
        WHEN e.encounter_type = 'Inpatient' AND e.discharge_date IS NOT NULL
        THEN DATEDIFF(e.discharge_date, e.encounter_date)
        ELSE NULL
    END AS length_of_stay_days
FROM encounters e
WHERE EXISTS (SELECT 1 FROM dim_patient WHERE patient_id = e.patient_id)
  AND EXISTS (SELECT 1 FROM dim_provider WHERE provider_id = e.provider_id);

SELECT CONCAT('Loaded ', COUNT(*), ' encounters') AS status FROM fact_encounters;

-- ============================================================================
-- STEP 3: LOAD BRIDGE TABLES
-- ============================================================================

-- Load bridge_encounter_diagnoses
INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT 
    f.encounter_key,
    d.diagnosis_key,
    ed.diagnosis_sequence
FROM encounter_diagnoses ed
JOIN fact_encounters f ON ed.encounter_id = f.encounter_id
JOIN dim_diagnosis d ON ed.diagnosis_id = d.diagnosis_id;

SELECT CONCAT('Loaded ', COUNT(*), ' encounter-diagnosis relationships') AS status 
FROM bridge_encounter_diagnoses;

-- Load bridge_encounter_procedures
INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date, procedure_sequence)
SELECT 
    f.encounter_key,
    p.procedure_key,
    ep.procedure_date,
    ROW_NUMBER() OVER (PARTITION BY f.encounter_key ORDER BY ep.procedure_date) AS procedure_sequence
FROM encounter_procedures ep
JOIN fact_encounters f ON ep.encounter_id = f.encounter_id
JOIN dim_procedure p ON ep.procedure_id = p.procedure_id;

SELECT CONCAT('Loaded ', COUNT(*), ' encounter-procedure relationships') AS status 
FROM bridge_encounter_procedures;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- STEP 4: VALIDATION
-- ============================================================================

SELECT 'ETL PIPELINE COMPLETED - VALIDATION SUMMARY' AS status;
SELECT '' AS separator;

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

-- Quick validation check
SELECT '' AS separator;
SELECT 'VALIDATION CHECKS' AS check_type;

SELECT 
    'Encounters match' AS check_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM encounters) = (SELECT COUNT(*) FROM fact_encounters)
        THEN 'PASS' ELSE 'FAIL'
    END AS status,
    CONCAT((SELECT COUNT(*) FROM encounters), ' OLTP vs ', 
           (SELECT COUNT(*) FROM fact_encounters), ' Star') AS details;

SELECT 'ETL Pipeline Execution Complete!' AS final_status;