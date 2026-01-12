-- ============================================================================
-- CLEAN OLTP SCHEMA - REMOVE ALL DATA
-- ============================================================================
-- Purpose: Remove all data from normalized OLTP tables
-- Order matters due to foreign key constraints!
-- ============================================================================

SET FOREIGN_KEY_CHECKS = 0;

-- Remove transactional tables first (they reference everything)
TRUNCATE TABLE billing;
TRUNCATE TABLE encounter_procedures;
TRUNCATE TABLE encounter_diagnoses;
TRUNCATE TABLE encounters;

-- -- Remove reference/lookup tables
-- TRUNCATE TABLE procedures;
-- TRUNCATE TABLE diagnoses;
-- TRUNCATE TABLE providers;
-- TRUNCATE TABLE patients;
-- TRUNCATE TABLE departments;
-- TRUNCATE TABLE specialties;

SET FOREIGN_KEY_CHECKS = 1;

-- Verify all tables are empty
SELECT 'VERIFICATION - All tables should show 0 records' AS status;

SELECT 'Table' AS table_name, 'Record Count' AS count
UNION ALL SELECT '---', '---'
-- UNION ALL SELECT 'patients', CAST(COUNT(*) AS CHAR) FROM patients
-- UNION ALL SELECT 'specialties', CAST(COUNT(*) AS CHAR) FROM specialties
-- UNION ALL SELECT 'departments', CAST(COUNT(*) AS CHAR) FROM departments
-- UNION ALL SELECT 'providers', CAST(COUNT(*) AS CHAR) FROM providers
UNION ALL SELECT 'encounters', CAST(COUNT(*) AS CHAR) FROM encounters
--UNION ALL SELECT 'diagnoses', CAST(COUNT(*) AS CHAR) FROM diagnoses
UNION ALL SELECT 'encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM encounter_diagnoses
--UNION ALL SELECT 'procedures', CAST(COUNT(*) AS CHAR) FROM procedures
UNION ALL SELECT 'encounter_procedures', CAST(COUNT(*) AS CHAR) FROM encounter_procedures
UNION ALL SELECT 'billing', CAST(COUNT(*) AS CHAR) FROM billing;

SELECT 'OLTP schema cleaned successfully!' AS result;