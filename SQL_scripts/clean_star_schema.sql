-- ============================================================================
-- CLEAN STAR SCHEMA - REMOVE ALL DATA
-- ============================================================================
-- Purpose: Remove mismatched data from star schema tables
-- Order matters due to foreign key constraints!
-- ============================================================================

SET FOREIGN_KEY_CHECKS = 0;

-- Remove bridge tables first (they reference fact and dimensions)
TRUNCATE TABLE bridge_encounter_procedures;
TRUNCATE TABLE bridge_encounter_diagnoses;

-- Remove fact table (references dimensions)
TRUNCATE TABLE fact_encounters;

-- Remove dimension tables (no dependencies)
TRUNCATE TABLE dim_date;
TRUNCATE TABLE dim_patient;
TRUNCATE TABLE dim_provider;
TRUNCATE TABLE dim_specialty;
TRUNCATE TABLE dim_department;
TRUNCATE TABLE dim_encounter_type;
TRUNCATE TABLE dim_diagnosis;
TRUNCATE TABLE dim_procedure;

SET FOREIGN_KEY_CHECKS = 1;

-- Verify all tables are empty
SELECT 'VERIFICATION - All tables should show 0 records' AS status;

SELECT 'Table' AS table_name, 'Record Count' AS count
UNION ALL SELECT '---', '---'
UNION ALL SELECT 'fact_encounters', CAST(COUNT(*) AS CHAR) FROM fact_encounters
UNION ALL SELECT 'bridge_encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_diagnoses
UNION ALL SELECT 'bridge_encounter_procedures', CAST(COUNT(*) AS CHAR) FROM bridge_encounter_procedures
UNION ALL SELECT 'dim_date', CAST(COUNT(*) AS CHAR) FROM dim_date
UNION ALL SELECT 'dim_patient', CAST(COUNT(*) AS CHAR) FROM dim_patient
UNION ALL SELECT 'dim_provider', CAST(COUNT(*) AS CHAR) FROM dim_provider
UNION ALL SELECT 'dim_specialty', CAST(COUNT(*) AS CHAR) FROM dim_specialty
UNION ALL SELECT 'dim_department', CAST(COUNT(*) AS CHAR) FROM dim_department
UNION ALL SELECT 'dim_encounter_type', CAST(COUNT(*) AS CHAR) FROM dim_encounter_type
UNION ALL SELECT 'dim_diagnosis', CAST(COUNT(*) AS CHAR) FROM dim_diagnosis
UNION ALL SELECT 'dim_procedure', CAST(COUNT(*) AS CHAR) FROM dim_procedure;

SELECT 'Star schema cleaned successfully!' AS result;