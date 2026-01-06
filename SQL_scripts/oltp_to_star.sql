-- ============================================================================
-- COMPLETE DATA MIGRATION: OLTP → STAR SCHEMA
-- ============================================================================
-- Purpose: Migrate all data from normalized OLTP tables to star schema
-- This script performs a full ETL transformation
-- ============================================================================

-- ============================================================================
-- STEP 0: CLEAR EXISTING STAR SCHEMA DATA (if re-running)
-- ============================================================================

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE bridge_encounter_procedures;
TRUNCATE TABLE bridge_encounter_diagnoses;
TRUNCATE TABLE fact_encounters;
TRUNCATE TABLE dim_procedure;
TRUNCATE TABLE dim_diagnosis;
TRUNCATE TABLE dim_encounter_type;
TRUNCATE TABLE dim_department;
TRUNCATE TABLE dim_provider;
TRUNCATE TABLE dim_specialty;
TRUNCATE TABLE dim_patient;
TRUNCATE TABLE dim_date;

SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- STEP 1: POPULATE DIMENSIONS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- dim_date: Generate dates from earliest to latest encounter date
-- ----------------------------------------------------------------------------

INSERT INTO dim_date 
(date_key, calendar_date, year, quarter, quarter_name, month, month_name, 
 month_year, day_of_month, day_of_week, day_name, week_of_year, is_weekend, 
 is_holiday, fiscal_year, fiscal_quarter, fiscal_period)
SELECT 
    DATE_FORMAT(d.date_val, '%Y%m%d') AS date_key,
    d.date_val AS calendar_date,
    YEAR(d.date_val) AS year,
    QUARTER(d.date_val) AS quarter,
    CONCAT('Q', QUARTER(d.date_val), ' ', YEAR(d.date_val)) AS quarter_name,
    MONTH(d.date_val) AS month,
    DATE_FORMAT(d.date_val, '%M') AS month_name,
    DATE_FORMAT(d.date_val, '%M %Y') AS month_year,
    DAY(d.date_val) AS day_of_month,
    DAYOFWEEK(d.date_val) AS day_of_week,
    DATE_FORMAT(d.date_val, '%W') AS day_name,
    WEEK(d.date_val) AS week_of_year,
    DAYOFWEEK(d.date_val) IN (1, 7) AS is_weekend,
    FALSE AS is_holiday,
    -- Fiscal year logic (assuming fiscal year = calendar year for simplicity)
    YEAR(d.date_val) AS fiscal_year,
    QUARTER(d.date_val) AS fiscal_quarter,
    CONCAT('FY', YEAR(d.date_val), '-Q', QUARTER(d.date_val)) AS fiscal_period
FROM (
    -- Generate date range from min to max encounter date
    SELECT DATE(encounter_date) AS date_val
    FROM encounters
    GROUP BY DATE(encounter_date)
    
    UNION
    
    -- Also include discharge dates
    SELECT DATE(discharge_date) AS date_val
    FROM encounters
    WHERE discharge_date IS NOT NULL
    GROUP BY DATE(discharge_date)
) d
ORDER BY d.date_val;

SELECT CONCAT('✓ dim_date loaded: ', COUNT(*), ' rows') AS status FROM dim_date;

-- ----------------------------------------------------------------------------
-- dim_specialty: Load all specialties
-- ----------------------------------------------------------------------------

INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code, specialty_category)
SELECT 
    specialty_id,
    specialty_name,
    specialty_code,
    -- Categorize specialties based on code patterns
    CASE 
        WHEN specialty_code IN ('CARD', 'IM', 'PULM', 'ENDO') THEN 'Medical'
        WHEN specialty_code IN ('GEN SURG', 'ORTHO', 'NEURO SURG') THEN 'Surgical'
        WHEN specialty_code IN ('RAD', 'PATH', 'LAB') THEN 'Diagnostic'
        WHEN specialty_code IN ('ER', 'ICU') THEN 'Emergency'
        ELSE 'Other'
    END AS specialty_category
FROM specialties
ORDER BY specialty_id;

SELECT CONCAT('✓ dim_specialty loaded: ', COUNT(*), ' rows') AS status FROM dim_specialty;

-- ----------------------------------------------------------------------------
-- dim_department: Load all departments
-- ----------------------------------------------------------------------------

INSERT INTO dim_department (department_id, department_name, floor, capacity, department_type, cost_center)
SELECT 
    department_id,
    department_name,
    floor,
    capacity,
    -- Infer department type from name and floor
    CASE 
        WHEN LOWER(department_name) LIKE '%emergency%' OR floor = 1 THEN 'Emergency'
        WHEN capacity > 25 THEN 'Inpatient'
        ELSE 'Outpatient'
    END AS department_type,
    CONCAT('CC-', LPAD(department_id, 4, '0')) AS cost_center
FROM departments
ORDER BY department_id;

SELECT CONCAT('✓ dim_department loaded: ', COUNT(*), ' rows') AS status FROM dim_department;

-- ----------------------------------------------------------------------------
-- dim_encounter_type: Load encounter types (small reference table)
-- ----------------------------------------------------------------------------

INSERT INTO dim_encounter_type (encounter_type_name, encounter_type_code, requires_admission, average_duration_hours)
VALUES
    ('Outpatient', 'OP', FALSE, 1.5),
    ('Inpatient', 'IP', TRUE, 96.0),
    ('ER', 'ER', FALSE, 6.5),
    ('Emergency', 'ER', FALSE, 6.5);  -- Some systems might use 'Emergency' instead of 'ER'

SELECT CONCAT('✓ dim_encounter_type loaded: ', COUNT(*), ' rows') AS status FROM dim_encounter_type;

-- ----------------------------------------------------------------------------
-- dim_patient: Load all patients with derived attributes
-- ----------------------------------------------------------------------------

INSERT INTO dim_patient 
(patient_id, mrn, first_name, last_name, full_name, date_of_birth, gender, 
 age_group, source_system, effective_date, is_current)
SELECT 
    p.patient_id,
    p.mrn,
    p.first_name,
    p.last_name,
    CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.last_name, '')) AS full_name,
    p.date_of_birth,
    p.gender,
    -- Calculate age group
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN '0-17'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 35 THEN '18-34'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 50 THEN '35-49'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 65 THEN '50-64'
        ELSE '65+'
    END AS age_group,
    'OLTP_MIGRATION' AS source_system,
    CURDATE() AS effective_date,
    TRUE AS is_current
FROM patients p
ORDER BY p.patient_id;

SELECT CONCAT('✓ dim_patient loaded: ', COUNT(*), ' rows') AS status FROM dim_patient;

-- ----------------------------------------------------------------------------
-- dim_provider: Load all providers with DENORMALIZED specialty
-- ----------------------------------------------------------------------------

INSERT INTO dim_provider 
(provider_id, first_name, last_name, full_name, credential, 
 specialty_id, specialty_name, specialty_code, hire_date, is_active)
SELECT 
    p.provider_id,
    p.first_name,
    p.last_name,
    CONCAT('Dr. ', p.first_name, ' ', p.last_name) AS full_name,
    p.credential,
    p.specialty_id,
    s.specialty_name,  -- DENORMALIZED
    s.specialty_code,  -- DENORMALIZED
    NULL AS hire_date,  -- Not in source data
    TRUE AS is_active
FROM providers p
JOIN specialties s ON p.specialty_id = s.specialty_id
ORDER BY p.provider_id;

SELECT CONCAT('✓ dim_provider loaded: ', COUNT(*), ' rows') AS status FROM dim_provider;

-- ----------------------------------------------------------------------------
-- dim_diagnosis: Load all diagnoses
-- ----------------------------------------------------------------------------

INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description, diagnosis_category, is_chronic)
SELECT 
    d.diagnosis_id,
    d.icd10_code,
    d.icd10_description,
    -- Categorize diagnoses based on ICD-10 code prefix
    CASE 
        WHEN d.icd10_code LIKE 'I%' THEN 'Cardiovascular'
        WHEN d.icd10_code LIKE 'E%' THEN 'Endocrine/Metabolic'
        WHEN d.icd10_code LIKE 'J%' THEN 'Respiratory'
        WHEN d.icd10_code LIKE 'K%' THEN 'Digestive'
        WHEN d.icd10_code LIKE 'M%' THEN 'Musculoskeletal'
        WHEN d.icd10_code LIKE 'N%' THEN 'Genitourinary'
        WHEN d.icd10_code LIKE 'C%' OR d.icd10_code LIKE 'D%' THEN 'Neoplasms'
        WHEN d.icd10_code LIKE 'S%' OR d.icd10_code LIKE 'T%' THEN 'Injury/Poisoning'
        ELSE 'Other'
    END AS diagnosis_category,
    -- Flag common chronic conditions
    d.icd10_code IN ('I10', 'E11.9', 'I50.9', 'J44.9', 'N18.9') AS is_chronic
FROM diagnoses d
ORDER BY d.diagnosis_id;

SELECT CONCAT('✓ dim_diagnosis loaded: ', COUNT(*), ' rows') AS status FROM dim_diagnosis;

-- ----------------------------------------------------------------------------
-- dim_procedure: Load all procedures
-- ----------------------------------------------------------------------------

INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description, procedure_category, average_duration_minutes)
SELECT 
    p.procedure_id,
    p.cpt_code,
    p.cpt_description,
    -- Categorize procedures based on CPT code ranges
    CASE 
        WHEN p.cpt_code BETWEEN '99200' AND '99499' THEN 'Evaluation & Management'
        WHEN p.cpt_code BETWEEN '70000' AND '79999' THEN 'Radiology'
        WHEN p.cpt_code BETWEEN '80000' AND '89999' THEN 'Laboratory'
        WHEN p.cpt_code BETWEEN '90000' AND '99999' THEN 'Medicine'
        WHEN p.cpt_code BETWEEN '00100' AND '69999' THEN 'Surgical'
        ELSE 'Other'
    END AS procedure_category,
    -- Estimate duration based on category
    CASE 
        WHEN p.cpt_code BETWEEN '99200' AND '99499' THEN 30
        WHEN p.cpt_code BETWEEN '70000' AND '79999' THEN 20
        WHEN p.cpt_code BETWEEN '80000' AND '89999' THEN 15
        ELSE 45
    END AS average_duration_minutes
FROM procedures p
ORDER BY p.procedure_id;

SELECT CONCAT('✓ dim_procedure loaded: ', COUNT(*), ' rows') AS status FROM dim_procedure;

-- ============================================================================
-- STEP 2: POPULATE FACT TABLE
-- ============================================================================

INSERT INTO fact_encounters 
(encounter_id, date_key, patient_key, provider_key, specialty_key, department_key, 
 encounter_type_key, encounter_date, discharge_date, diagnosis_count, procedure_count,
 total_claim_amount, total_allowed_amount, length_of_stay_days, etl_loaded_date)
SELECT 
    e.encounter_id,
    -- Date key lookup
    DATE_FORMAT(e.encounter_date, '%Y%m%d') AS date_key,
    -- Patient key lookup
    dp.patient_key,
    -- Provider key lookup
    dpr.provider_key,
    -- Specialty key lookup (from provider's specialty)
    ds.specialty_key,
    -- Department key lookup
    dd.department_key,
    -- Encounter type key lookup
    det.encounter_type_key,
    -- Encounter timestamps
    e.encounter_date,
    e.discharge_date,
    -- PRE-AGGREGATED: Count diagnoses
    COALESCE((SELECT COUNT(*) 
              FROM encounter_diagnoses ed 
              WHERE ed.encounter_id = e.encounter_id), 0) AS diagnosis_count,
    -- PRE-AGGREGATED: Count procedures
    COALESCE((SELECT COUNT(*) 
              FROM encounter_procedures ep 
              WHERE ep.encounter_id = e.encounter_id), 0) AS procedure_count,
    -- PRE-AGGREGATED: Sum billing amounts
    COALESCE((SELECT SUM(b.claim_amount) 
              FROM billing b 
              WHERE b.encounter_id = e.encounter_id), NULL) AS total_claim_amount,
    COALESCE((SELECT SUM(b.allowed_amount) 
              FROM billing b 
              WHERE b.encounter_id = e.encounter_id 
              AND b.claim_status = 'Paid'), NULL) AS total_allowed_amount,
    -- CALCULATED: Length of stay
    CASE 
        WHEN e.encounter_type = 'Inpatient' AND e.discharge_date IS NOT NULL 
        THEN DATEDIFF(e.discharge_date, e.encounter_date)
        ELSE NULL
    END AS length_of_stay_days,
    -- ETL metadata
    NOW() AS etl_loaded_date
FROM encounters e
-- Join to get dimension keys
JOIN dim_patient dp ON e.patient_id = dp.patient_id
JOIN dim_provider dpr ON e.provider_id = dpr.provider_id
JOIN dim_specialty ds ON dpr.specialty_id = ds.specialty_id
JOIN dim_department dd ON e.department_id = dd.department_id
JOIN dim_encounter_type det ON e.encounter_type = det.encounter_type_name
ORDER BY e.encounter_id;

SELECT CONCAT('✓ fact_encounters loaded: ', COUNT(*), ' rows') AS status FROM fact_encounters;

-- ============================================================================
-- STEP 3: POPULATE BRIDGE TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- bridge_encounter_diagnoses
-- ----------------------------------------------------------------------------

INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT 
    fe.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence
FROM encounter_diagnoses ed
JOIN fact_encounters fe ON ed.encounter_id = fe.encounter_id
JOIN dim_diagnosis dd ON ed.diagnosis_id = dd.diagnosis_id
ORDER BY fe.encounter_key, ed.diagnosis_sequence;

SELECT CONCAT('✓ bridge_encounter_diagnoses loaded: ', COUNT(*), ' rows') AS status 
FROM bridge_encounter_diagnoses;

-- ----------------------------------------------------------------------------
-- bridge_encounter_procedures
-- ----------------------------------------------------------------------------

INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date, procedure_sequence)
SELECT 
    fe.encounter_key,
    dp.procedure_key,
    ep.procedure_date,
    ROW_NUMBER() OVER (PARTITION BY fe.encounter_key ORDER BY ep.procedure_date) AS procedure_sequence
FROM encounter_procedures ep
JOIN fact_encounters fe ON ep.encounter_id = fe.encounter_id
JOIN dim_procedure dp ON ep.procedure_id = dp.procedure_id
ORDER BY fe.encounter_key, procedure_sequence;

SELECT CONCAT('✓ bridge_encounter_procedures loaded: ', COUNT(*), ' rows') AS status 
FROM bridge_encounter_procedures;

-- ============================================================================
-- STEP 4: VERIFICATION & DATA QUALITY CHECKS
-- ============================================================================

SELECT '
============================================================================
DATA MIGRATION COMPLETE - VERIFICATION SUMMARY
============================================================================
' AS '';

-- Row count comparison
SELECT 'ROW COUNT VERIFICATION' AS check_type, '' AS source_table, '' AS star_table, '' AS status
UNION ALL
SELECT '---', '---', '---', '---'
UNION ALL
SELECT 'Patients', 
       CAST(COUNT(*) AS CHAR) FROM patients
UNION ALL
SELECT '', 
       '', 
       CAST(COUNT(*) AS CHAR) FROM dim_patient
UNION ALL
SELECT '', '', '', 
       CASE WHEN (SELECT COUNT(*) FROM patients) = (SELECT COUNT(*) FROM dim_patient) 
       THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT '---', '---', '---', '---'
UNION ALL
SELECT 'Encounters',
       CAST(COUNT(*) AS CHAR) FROM encounters
UNION ALL
SELECT '',
       '',
       CAST(COUNT(*) AS CHAR) FROM fact_encounters
UNION ALL
SELECT '', '', '',
       CASE WHEN (SELECT COUNT(*) FROM encounters) = (SELECT COUNT(*) FROM fact_encounters)
       THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT '---', '---', '---', '---'
UNION ALL
SELECT 'Diagnoses Relationships',
       CAST(COUNT(*) AS CHAR) FROM encounter_diagnoses
UNION ALL
SELECT '',
       '',
       CAST(COUNT(*) AS CHAR) FROM bridge_encounter_diagnoses
UNION ALL
SELECT '', '', '',
       CASE WHEN (SELECT COUNT(*) FROM encounter_diagnoses) = (SELECT COUNT(*) FROM bridge_encounter_diagnoses)
       THEN '✓ MATCH' ELSE '✗ MISMATCH' END
UNION ALL
SELECT '---', '---', '---', '---'
UNION ALL
SELECT 'Procedures Relationships',
       CAST(COUNT(*) AS CHAR) FROM encounter_procedures
UNION ALL
SELECT '',
       '',
       CAST(COUNT(*) AS CHAR) FROM bridge_encounter_procedures
UNION ALL
SELECT '', '', '',
       CASE WHEN (SELECT COUNT(*) FROM encounter_procedures) = (SELECT COUNT(*) FROM bridge_encounter_procedures)
       THEN '✓ MATCH' ELSE '✗ MISMATCH' END;

-- Data quality checks
SELECT '
DATA QUALITY CHECKS
' AS '';

SELECT 'Check 1: Orphaned Keys' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE CONCAT('✗ FAIL - ', COUNT(*), ' orphans') END AS result
FROM fact_encounters f
LEFT JOIN dim_patient p ON f.patient_key = p.patient_key
WHERE p.patient_key IS NULL

UNION ALL

SELECT 'Check 2: Pre-aggregated Counts',
       CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE CONCAT('✗ FAIL - ', COUNT(*), ' mismatches') END
FROM fact_encounters f
WHERE f.diagnosis_count != (SELECT COUNT(*) FROM bridge_encounter_diagnoses b 
                             WHERE b.encounter_key = f.encounter_key)

UNION ALL

SELECT 'Check 3: Revenue Totals',
       CONCAT('✓ OLTP: $', FORMAT(SUM(allowed_amount), 2), ' | Star: $',
              FORMAT((SELECT SUM(total_allowed_amount) FROM fact_encounters), 2))
FROM billing
WHERE claim_status = 'Paid';

-- Summary statistics
SELECT '
STAR SCHEMA SUMMARY STATISTICS
' AS '';

SELECT 'Metric' AS metric, 'Count' AS value
UNION ALL SELECT '---', '---'
UNION ALL SELECT 'Total Encounters', CAST(COUNT(*) AS CHAR) FROM fact_encounters
UNION ALL SELECT 'Total Patients', CAST(COUNT(*) AS CHAR) FROM dim_patient
UNION ALL SELECT 'Total Providers', CAST(COUNT(*) AS CHAR) FROM dim_provider
UNION ALL SELECT 'Total Diagnoses', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_diagnoses
UNION ALL SELECT 'Total Procedures', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_procedures
UNION ALL SELECT 'Date Range', 
    CONCAT(MIN(calendar_date), ' to ', MAX(calendar_date)) FROM dim_date
UNION ALL SELECT 'Avg Diagnoses per Encounter', 
    CAST(ROUND(AVG(diagnosis_count), 2) AS CHAR) FROM fact_encounters
UNION ALL SELECT 'Avg Procedures per Encounter',
    CAST(ROUND(AVG(procedure_count), 2) AS CHAR) FROM fact_encounters
UNION ALL SELECT 'Total Revenue', 
    CONCAT('$', FORMAT(SUM(total_allowed_amount), 2)) FROM fact_encounters;

SELECT '
============================================================================
✓ MIGRATION COMPLETE - Star schema ready for analysis!
============================================================================
' AS '';

-- ============================================================================
-- END OF MIGRATION SCRIPT
-- ============================================================================