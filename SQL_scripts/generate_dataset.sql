-- -- GENERATE REALISTIC OLTP DATA AT SCALE
-- -- ============================================================================
-- -- Target: 100K patients, 500 providers, 500K encounters
-- -- Time period: 2022-01-01 to 2024-12-31 (3 years)
-- -- Realistic distribution: 60% Outpatient, 30% Inpatient, 10% ER
-- -- ============================================================================

-- SET @start_time = NOW();

-- -- STEP 1: SPECIALTIES (15 specialties)
-- INSERT INTO specialties (specialty_id, specialty_name, specialty_code) VALUES
-- (1, 'Cardiology', 'CARD'),
-- (2, 'Internal Medicine', 'IM'),
-- (3, 'Emergency Medicine', 'EM'),
-- (4, 'Orthopedic Surgery', 'ORTHO'),
-- (5, 'Neurology', 'NEURO'),
-- (6, 'Pediatrics', 'PEDS'),
-- (7, 'Obstetrics & Gynecology', 'OBGYN'),
-- (8, 'Psychiatry', 'PSYCH'),
-- (9, 'Dermatology', 'DERM'),
-- (10, 'Oncology', 'ONC'),
-- (11, 'Pulmonology', 'PULM'),
-- (12, 'Gastroenterology', 'GI'),
-- (13, 'Endocrinology', 'ENDO'),
-- (14, 'Nephrology', 'NEPH'),
-- (15, 'Rheumatology', 'RHEUM');


-- -- STEP 2: DEPARTMENTS (15 departments)
-- INSERT INTO departments (department_id, department_name, floor, capacity) VALUES
-- (1, 'Cardiology Unit', 3, 25),
-- (2, 'Internal Medicine', 2, 40),
-- (3, 'Emergency Department', 1, 50),
-- (4, 'Orthopedic Surgery', 4, 30),
-- (5, 'Neurology Unit', 5, 20),
-- (6, 'Pediatrics', 2, 35),
-- (7, 'Women''s Health', 3, 28),
-- (8, 'Psychiatry Unit', 6, 22),
-- (9, 'Dermatology Clinic', 1, 15),
-- (10, 'Cancer Center', 4, 25),
-- (11, 'Pulmonology Unit', 5, 18),
-- (12, 'GI Unit', 3, 20),
-- (13, 'Endocrinology Clinic', 2, 12),
-- (14, 'Nephrology Unit', 4, 16),
-- (15, 'Rheumatology Clinic', 1, 10);


-- -- STEP 3: PROVIDERS (500 providers, distributed across specialties)
-- DROP PROCEDURE IF EXISTS generate_providers;
-- DELIMITER //
-- CREATE PROCEDURE generate_providers()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE fname VARCHAR(100);
--     DECLARE lname VARCHAR(100);
--     DECLARE spec_id INT;
--     DECLARE dept_id INT;
    
--     WHILE i <= 500 DO
--         -- Rotate through specialties and departments
--         SET spec_id = ((i - 1) % 15) + 1;
--         SET dept_id = spec_id;
        
--         -- Generate names based on provider number
--         SET fname = CONCAT('Provider', i);
--         SET lname = CASE 
--             WHEN i % 10 = 1 THEN 'Smith'
--             WHEN i % 10 = 2 THEN 'Johnson'
--             WHEN i % 10 = 3 THEN 'Williams'
--             WHEN i % 10 = 4 THEN 'Brown'
--             WHEN i % 10 = 5 THEN 'Jones'
--             WHEN i % 10 = 6 THEN 'Garcia'
--             WHEN i % 10 = 7 THEN 'Martinez'
--             WHEN i % 10 = 8 THEN 'Davis'
--             WHEN i % 10 = 9 THEN 'Rodriguez'
--             ELSE 'Wilson'
--         END;
        
--         INSERT INTO providers (provider_id, first_name, last_name, credential, specialty_id, department_id)
--         VALUES (i, fname, lname, 'MD', spec_id, dept_id);
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_providers();
-- DROP PROCEDURE generate_providers;

-- SELECT 'Providers generated' AS status, COUNT(*) AS count FROM providers;


-- -- STEP 4: DIAGNOSES (50 common ICD-10 codes)
-- INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description) VALUES
-- (1, 'I10', 'Essential hypertension'),
-- (2, 'E11.9', 'Type 2 diabetes without complications'),
-- (3, 'I50.9', 'Heart failure, unspecified'),
-- (4, 'J44.9', 'COPD, unspecified'),
-- (5, 'E78.5', 'Hyperlipidemia'),
-- (6, 'M79.3', 'Fibromyalgia'),
-- (7, 'J18.9', 'Pneumonia, unspecified'),
-- (8, 'N18.3', 'Chronic kidney disease, stage 3'),
-- (9, 'F41.9', 'Anxiety disorder, unspecified'),
-- (10, 'M17.9', 'Osteoarthritis of knee'),
-- (11, 'K21.9', 'GERD without esophagitis'),
-- (12, 'E66.9', 'Obesity, unspecified'),
-- (13, 'I25.10', 'Coronary artery disease'),
-- (14, 'G43.909', 'Migraine, unspecified'),
-- (15, 'J45.909', 'Asthma, unspecified'),
-- (16, 'N39.0', 'Urinary tract infection'),
-- (17, 'R07.9', 'Chest pain, unspecified'),
-- (18, 'M25.50', 'Joint pain, unspecified'),
-- (19, 'R51', 'Headache'),
-- (20, 'K59.00', 'Constipation'),
-- (21, 'F33.9', 'Major depressive disorder'),
-- (22, 'I48.91', 'Atrial fibrillation'),
-- (23, 'E11.65', 'Type 2 diabetes with hyperglycemia'),
-- (24, 'M81.0', 'Osteoporosis'),
-- (25, 'K80.20', 'Gallstone disease'),
-- (26, 'C50.919', 'Breast cancer'),
-- (27, 'C18.9', 'Colorectal cancer'),
-- (28, 'C61', 'Prostate cancer'),
-- (29, 'I63.9', 'Cerebral infarction'),
-- (30, 'S72.001A', 'Fracture of femur'),
-- (31, 'S82.001A', 'Fracture of tibia'),
-- (32, 'N40.0', 'Benign prostatic hyperplasia'),
-- (33, 'L40.9', 'Psoriasis'),
-- (34, 'D50.9', 'Iron deficiency anemia'),
-- (35, 'E03.9', 'Hypothyroidism'),
-- (36, 'E05.90', 'Hyperthyroidism'),
-- (37, 'K58.9', 'Irritable bowel syndrome'),
-- (38, 'M06.9', 'Rheumatoid arthritis'),
-- (39, 'G20', 'Parkinson''s disease'),
-- (40, 'G30.9', 'Alzheimer''s disease'),
-- (41, 'I21.9', 'Acute myocardial infarction'),
-- (42, 'I69.391', 'Dysphagia following stroke'),
-- (43, 'O80', 'Normal delivery'),
-- (44, 'P07.39', 'Preterm newborn'),
-- (45, 'J96.00', 'Acute respiratory failure'),
-- (46, 'K92.2', 'Gastrointestinal hemorrhage'),
-- (47, 'N17.9', 'Acute kidney failure'),
-- (48, 'E87.1', 'Hyponatremia'),
-- (49, 'R50.9', 'Fever, unspecified'),
-- (50, 'R10.9', 'Abdominal pain');



-- -- STEP 5: PROCEDURES (40 common CPT codes)
-- INSERT INTO procedures (procedure_id, cpt_code, cpt_description) VALUES
-- (1, '99213', 'Office visit, established patient, moderate'),
-- (2, '99214', 'Office visit, established patient, detailed'),
-- (3, '99215', 'Office visit, established patient, comprehensive'),
-- (4, '99223', 'Initial hospital care, high severity'),
-- (5, '99233', 'Subsequent hospital care, high severity'),
-- (6, '93000', 'Electrocardiogram'),
-- (7, '71020', 'Chest X-ray, 2 views'),
-- (8, '80053', 'Comprehensive metabolic panel'),
-- (9, '85025', 'Complete blood count with differential'),
-- (10, '36415', 'Venipuncture'),
-- (11, '93306', 'Echocardiography'),
-- (12, '70450', 'CT head without contrast'),
-- (13, '70553', 'MRI brain with and without contrast'),
-- (14, '72148', 'MRI lumbar spine without contrast'),
-- (15, '73562', 'X-ray knee, 3 views'),
-- (16, '76856', 'Ultrasound pelvis'),
-- (17, '88305', 'Tissue pathology'),
-- (18, '45378', 'Colonoscopy'),
-- (19, '43239', 'Upper endoscopy with biopsy'),
-- (20, '27447', 'Total knee arthroplasty'),
-- (21, '27130', 'Total hip arthroplasty'),
-- (22, '33533', 'CABG, arterial graft'),
-- (23, '47562', 'Laparoscopic cholecystectomy'),
-- (24, '38220', 'Bone marrow biopsy'),
-- (25, '96413', 'Chemotherapy administration IV'),
-- (26, '59400', 'Vaginal delivery'),
-- (27, '59510', 'Cesarean delivery'),
-- (28, '99281', 'ER visit, minor severity'),
-- (29, '99282', 'ER visit, low to moderate severity'),
-- (30, '99283', 'ER visit, moderate severity'),
-- (31, '99284', 'ER visit, high severity'),
-- (32, '99285', 'ER visit, critical severity'),
-- (33, '31500', 'Intubation, endotracheal'),
-- (34, '94640', 'Nebulizer therapy'),
-- (35, '90471', 'Immunization administration'),
-- (36, '90834', 'Psychotherapy, 45 minutes'),
-- (37, '96372', 'Therapeutic injection'),
-- (38, '12001', 'Simple wound repair'),
-- (39, '29881', 'Knee arthroscopy with meniscectomy'),
-- (40, '52000', 'Cystoscopy');


-- -- STEP 6: PATIENTS (100,000 patients)
-- DROP PROCEDURE IF EXISTS generate_patients;
-- DELIMITER //
-- CREATE PROCEDURE generate_patients()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE fname VARCHAR(100);
--     DECLARE lname VARCHAR(100);
--     DECLARE dob DATE;
--     DECLARE gen CHAR(1);
    
--     WHILE i <= 100000 DO
--         -- Generate realistic names
--         SET fname = CASE 
--             WHEN i % 20 = 0 THEN 'James'
--             WHEN i % 20 = 1 THEN 'Mary'
--             WHEN i % 20 = 2 THEN 'John'
--             WHEN i % 20 = 3 THEN 'Patricia'
--             WHEN i % 20 = 4 THEN 'Robert'
--             WHEN i % 20 = 5 THEN 'Jennifer'
--             WHEN i % 20 = 6 THEN 'Michael'
--             WHEN i % 20 = 7 THEN 'Linda'
--             WHEN i % 20 = 8 THEN 'William'
--             WHEN i % 20 = 9 THEN 'Barbara'
--             WHEN i % 20 = 10 THEN 'David'
--             WHEN i % 20 = 11 THEN 'Elizabeth'
--             WHEN i % 20 = 12 THEN 'Richard'
--             WHEN i % 20 = 13 THEN 'Susan'
--             WHEN i % 20 = 14 THEN 'Joseph'
--             WHEN i % 20 = 15 THEN 'Jessica'
--             WHEN i % 20 = 16 THEN 'Thomas'
--             WHEN i % 20 = 17 THEN 'Sarah'
--             WHEN i % 20 = 18 THEN 'Charles'
--             ELSE 'Karen'
--         END;
        
--         SET lname = CASE 
--             WHEN i % 15 = 0 THEN 'Smith'
--             WHEN i % 15 = 1 THEN 'Johnson'
--             WHEN i % 15 = 2 THEN 'Williams'
--             WHEN i % 15 = 3 THEN 'Brown'
--             WHEN i % 15 = 4 THEN 'Jones'
--             WHEN i % 15 = 5 THEN 'Garcia'
--             WHEN i % 15 = 6 THEN 'Miller'
--             WHEN i % 15 = 7 THEN 'Davis'
--             WHEN i % 15 = 8 THEN 'Rodriguez'
--             WHEN i % 15 = 9 THEN 'Martinez'
--             WHEN i % 15 = 10 THEN 'Hernandez'
--             WHEN i % 15 = 11 THEN 'Lopez'
--             WHEN i % 15 = 12 THEN 'Gonzalez'
--             WHEN i % 15 = 13 THEN 'Wilson'
--             ELSE 'Anderson'
--         END;
        
--         -- Age distribution: realistic mix (more adults/elderly)
--         SET dob = DATE_ADD('1930-01-01', INTERVAL FLOOR(RAND() * 30000) DAY);
--         SET gen = IF(i % 2 = 0, 'M', 'F');
        
--         INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
--         VALUES (i, fname, lname, dob, gen, CONCAT('MRN', LPAD(i, 8, '0')));
        
--         IF i % 10000 = 0 THEN
--             SELECT CONCAT('Patients: ', i, ' inserted') AS progress;
--         END IF;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_patients();
-- DROP PROCEDURE generate_patients;

-- SELECT 'Patients generated' AS status, COUNT(*) AS count FROM patients;


-- -- STEP 7: ENCOUNTERS (500,000 encounters)
-- -- Distribution: 60% Outpatient, 30% Inpatient, 10% ER
-- -- Time period: 2022-01-01 to 2024-12-31

-- DROP PROCEDURE IF EXISTS generate_encounters;
-- DELIMITER //
-- CREATE PROCEDURE generate_encounters()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE pat_id INT;
--     DECLARE prov_id INT;
--     DECLARE dept_id INT;
--     DECLARE enc_type VARCHAR(50);
--     DECLARE enc_date DATETIME;
--     DECLARE disc_date DATETIME;
--     DECLARE rand_val DECIMAL(3,2);
--     DECLARE duration_hours INT;
    
--     WHILE i <= 500000 DO
--         -- Random patient (weighted toward having multiple encounters)
--         SET pat_id = FLOOR(1 + (RAND() * 100000));
        
--         -- Random provider
--         SET prov_id = FLOOR(1 + (RAND() * 500));
        
--         -- Get department from provider
--         SELECT department_id INTO dept_id FROM providers WHERE provider_id = prov_id;
        
--         -- Encounter type distribution: 60% Outpatient, 30% Inpatient, 10% ER
--         SET rand_val = RAND();
--         IF rand_val < 0.60 THEN
--             SET enc_type = 'Outpatient';
--             SET duration_hours = FLOOR(1 + (RAND() * 3)); -- 1-3 hours
--         ELSEIF rand_val < 0.90 THEN
--             SET enc_type = 'Inpatient';
--             SET duration_hours = FLOOR(24 + (RAND() * 168)); -- 1-7 days
--         ELSE
--             SET enc_type = 'ER';
--             SET duration_hours = FLOOR(2 + (RAND() * 10)); -- 2-12 hours
--         END IF;
        
--         -- Random date between 2022-01-01 and 2024-12-31
--         SET enc_date = DATE_ADD('2022-01-01', INTERVAL FLOOR(RAND() * 1095) DAY);
--         SET enc_date = TIMESTAMP(enc_date, SEC_TO_TIME(FLOOR(RAND() * 86400)));
--         SET disc_date = DATE_ADD(enc_date, INTERVAL duration_hours HOUR);
        
--         INSERT INTO encounters (encounter_id, patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id)
--         VALUES (i, pat_id, prov_id, enc_type, enc_date, disc_date, dept_id);
        
--         IF i % 50000 = 0 THEN
--             SELECT CONCAT('Encounters: ', i, ' inserted') AS progress;
--         END IF;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_encounters();
-- DROP PROCEDURE generate_encounters;

-- SELECT 'Encounters generated' AS status, COUNT(*) AS count FROM encounters;



-- -- STEP 8: ENCOUNTER DIAGNOSES (1-3 diagnoses per encounter)
-- DROP PROCEDURE IF EXISTS generate_encounter_diagnoses;
-- DELIMITER //
-- CREATE PROCEDURE generate_encounter_diagnoses()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE enc_id INT;
--     DECLARE diag_id INT;
--     DECLARE num_diagnoses INT;
--     DECLARE seq INT;
--     DECLARE ed_id INT DEFAULT 1;
    
--     WHILE i <= 500000 DO
--         SET enc_id = i;
--         SET num_diagnoses = FLOOR(1 + (RAND() * 3)); -- 1-3 diagnoses
--         SET seq = 1;
        
--         WHILE seq <= num_diagnoses DO
--             SET diag_id = FLOOR(1 + (RAND() * 50));
            
--             INSERT INTO encounter_diagnoses (encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence)
--             VALUES (ed_id, enc_id, diag_id, seq);
            
--             SET ed_id = ed_id + 1;
--             SET seq = seq + 1;
--         END WHILE;
        
--         IF i % 50000 = 0 THEN
--             SELECT CONCAT('Encounter diagnoses: ', i, ' encounters processed') AS progress;
--         END IF;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_encounter_diagnoses();
-- DROP PROCEDURE generate_encounter_diagnoses;

-- SELECT 'Encounter diagnoses generated' AS status, COUNT(*) AS count FROM encounter_diagnoses;



-- -- STEP 9: ENCOUNTER PROCEDURES (1-5 procedures per encounter)
-- DROP PROCEDURE IF EXISTS generate_encounter_procedures;
-- DELIMITER //
-- CREATE PROCEDURE generate_encounter_procedures()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE enc_id INT;
--     DECLARE proc_id INT;
--     DECLARE num_procedures INT;
--     DECLARE proc_date DATE;
--     DECLARE enc_date DATE;
--     DECLARE ep_id INT DEFAULT 1;
--     DECLARE proc_count INT;
    
--     WHILE i <= 500000 DO
--         SET enc_id = i;
        
--         -- Get encounter date
--         SELECT DATE(encounter_date) INTO enc_date FROM encounters WHERE encounter_id = enc_id;
        
--         -- 1-5 procedures per encounter (outpatient fewer, inpatient more)
--         SET num_procedures = FLOOR(1 + (RAND() * 5));
--         SET proc_count = 1;
        
--         WHILE proc_count <= num_procedures DO
--             SET proc_id = FLOOR(1 + (RAND() * 40));
--             SET proc_date = enc_date;
            
--             INSERT INTO encounter_procedures (encounter_procedure_id, encounter_id, procedure_id, procedure_date)
--             VALUES (ep_id, enc_id, proc_id, proc_date);
            
--             SET ep_id = ep_id + 1;
--             SET proc_count = proc_count + 1;
--         END WHILE;
        
--         IF i % 50000 = 0 THEN
--             SELECT CONCAT('Encounter procedures: ', i, ' encounters processed') AS progress;
--         END IF;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_encounter_procedures();
-- DROP PROCEDURE generate_encounter_procedures;

-- SELECT 'Encounter procedures generated' AS status, COUNT(*) AS count FROM encounter_procedures;



-- -- STEP 10: BILLING (1 billing record per encounter)
-- DROP PROCEDURE IF EXISTS generate_billing;
-- DELIMITER //
-- CREATE PROCEDURE generate_billing()
-- BEGIN
--     DECLARE i INT DEFAULT 1;
--     DECLARE enc_id INT;
--     DECLARE enc_type VARCHAR(50);
--     DECLARE enc_date DATE;
--     DECLARE claim_amt DECIMAL(12,2);
--     DECLARE allowed_amt DECIMAL(12,2);
--     DECLARE claim_dt DATE;
--     DECLARE status VARCHAR(50);
    
--     WHILE i <= 500000 DO
--         SET enc_id = i;
        
--         SELECT encounter_type, DATE(encounter_date) 
--         INTO enc_type, enc_date 
--         FROM encounters WHERE encounter_id = enc_id;
        
--         -- Claim amounts based on encounter type
--         IF enc_type = 'Outpatient' THEN
--             SET claim_amt = 200 + (RAND() * 800); -- $200-$1000
--         ELSEIF enc_type = 'Inpatient' THEN
--             SET claim_amt = 5000 + (RAND() * 45000); -- $5000-$50000
--         ELSE -- ER
--             SET claim_amt = 1000 + (RAND() * 4000); -- $1000-$5000
--         END IF;
        
--         -- Allowed amount is 70-90% of claim
--         SET allowed_amt = claim_amt * (0.70 + (RAND() * 0.20));
        
--         -- Claim date is 1-30 days after encounter
--         SET claim_dt = DATE_ADD(enc_date, INTERVAL FLOOR(1 + (RAND() * 30)) DAY);
        
--         -- Status distribution: 85% Paid, 10% Pending, 5% Denied
--         SET status = CASE 
--             WHEN RAND() < 0.85 THEN 'Paid'
--             WHEN RAND() < 0.95 THEN 'Pending'
--             ELSE 'Denied'
--         END;
        
--         INSERT INTO billing (billing_id, encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
--         VALUES (i, enc_id, claim_amt, allowed_amt, claim_dt, status);
        
--         IF i % 50000 = 0 THEN
--             SELECT CONCAT('Billing: ', i, ' records inserted') AS progress;
--         END IF;
        
--         SET i = i + 1;
--     END WHILE;
-- END//
-- DELIMITER ;

-- CALL generate_billing();
-- DROP PROCEDURE generate_billing;

-- SELECT 'Billing records generated' AS status, COUNT(*) AS count FROM billing;



-- -- FINAL VERIFICATION
-- -- ============================================================================
-- SELECT '========================' AS separator;
-- SELECT 'DATA GENERATION COMPLETE' AS status;
-- SELECT '========================' AS separator;

-- SELECT 'Table' AS table_name, 'Record Count' AS count
-- UNION ALL SELECT '---', '---'
-- UNION ALL SELECT 'patients', CAST(COUNT(*) AS CHAR) FROM patients
-- UNION ALL SELECT 'specialties', CAST(COUNT(*) AS CHAR) FROM specialties
-- UNION ALL SELECT 'departments', CAST(COUNT(*) AS CHAR) FROM departments
-- UNION ALL SELECT 'providers', CAST(COUNT(*) AS CHAR) FROM providers
-- UNION ALL SELECT 'encounters', CAST(COUNT(*) AS CHAR) FROM encounters
-- UNION ALL SELECT 'diagnoses', CAST(COUNT(*) AS CHAR) FROM diagnoses
-- UNION ALL SELECT 'encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM encounter_diagnoses
-- UNION ALL SELECT 'procedures', CAST(COUNT(*) AS CHAR) FROM procedures
-- UNION ALL SELECT 'encounter_procedures', CAST(COUNT(*) AS CHAR) FROM encounter_procedures
-- UNION ALL SELECT 'billing', CAST(COUNT(*) AS CHAR) FROM billing;

-- SELECT 
--     'Encounter Type Distribution' AS metric,
--     encounter_type,
--     COUNT(*) AS count,
--     CONCAT(ROUND(COUNT(*) * 100.0 / 500000, 1), '%') AS percentage
-- FROM encounters
-- GROUP BY encounter_type;

-- SELECT 
--     CONCAT('Total execution time: ', TIMESTAMPDIFF(SECOND, @start_time, NOW()), ' seconds') AS summary;

-- SELECT 'OLTP database populated successfully!' AS result;





-- ============================================================================
-- GENERATE REALISTIC OLTP DATA AT SCALE
-- ============================================================================
-- Target: 100K patients, 500 providers, 500K encounters
-- Time period: 2022-01-01 to 2024-12-31 (3 years)
-- Realistic distribution: 60% Outpatient, 30% Inpatient, 10% ER
-- OPTIMIZED: Batch processing with commits to prevent timeouts
-- ============================================================================

SET @start_time = NOW();

-- ============================================================================
-- STEP 1: SPECIALTIES (15 specialties)
-- ============================================================================
INSERT INTO specialties (specialty_id, specialty_name, specialty_code) VALUES
(1, 'Cardiology', 'CARD'),
(2, 'Internal Medicine', 'IM'),
(3, 'Emergency Medicine', 'EM'),
(4, 'Orthopedic Surgery', 'ORTHO'),
(5, 'Neurology', 'NEURO'),
(6, 'Pediatrics', 'PEDS'),
(7, 'Obstetrics & Gynecology', 'OBGYN'),
(8, 'Psychiatry', 'PSYCH'),
(9, 'Dermatology', 'DERM'),
(10, 'Oncology', 'ONC'),
(11, 'Pulmonology', 'PULM'),
(12, 'Gastroenterology', 'GI'),
(13, 'Endocrinology', 'ENDO'),
(14, 'Nephrology', 'NEPH'),
(15, 'Rheumatology', 'RHEUM');

-- ============================================================================
-- STEP 2: DEPARTMENTS (15 departments)
-- ============================================================================
INSERT INTO departments (department_id, department_name, floor, capacity) VALUES
(1, 'Cardiology Unit', 3, 25),
(2, 'Internal Medicine', 2, 40),
(3, 'Emergency Department', 1, 50),
(4, 'Orthopedic Surgery', 4, 30),
(5, 'Neurology Unit', 5, 20),
(6, 'Pediatrics', 2, 35),
(7, 'Women''s Health', 3, 28),
(8, 'Psychiatry Unit', 6, 22),
(9, 'Dermatology Clinic', 1, 15),
(10, 'Cancer Center', 4, 25),
(11, 'Pulmonology Unit', 5, 18),
(12, 'GI Unit', 3, 20),
(13, 'Endocrinology Clinic', 2, 12),
(14, 'Nephrology Unit', 4, 16),
(15, 'Rheumatology Clinic', 1, 10);

-- ============================================================================
-- STEP 3: PROVIDERS (500 providers, distributed across specialties)
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_providers;
DELIMITER //
CREATE PROCEDURE generate_providers()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE fname VARCHAR(100);
    DECLARE lname VARCHAR(100);
    DECLARE spec_id INT;
    DECLARE dept_id INT;
    
    WHILE i <= 500 DO
        -- Rotate through specialties and departments
        SET spec_id = ((i - 1) % 15) + 1;
        SET dept_id = spec_id;
        
        -- Generate names based on provider number
        SET fname = CONCAT('Provider', i);
        SET lname = CASE 
            WHEN i % 10 = 1 THEN 'Smith'
            WHEN i % 10 = 2 THEN 'Johnson'
            WHEN i % 10 = 3 THEN 'Williams'
            WHEN i % 10 = 4 THEN 'Brown'
            WHEN i % 10 = 5 THEN 'Jones'
            WHEN i % 10 = 6 THEN 'Garcia'
            WHEN i % 10 = 7 THEN 'Martinez'
            WHEN i % 10 = 8 THEN 'Davis'
            WHEN i % 10 = 9 THEN 'Rodriguez'
            ELSE 'Wilson'
        END;
        
        INSERT INTO providers (provider_id, first_name, last_name, credential, specialty_id, department_id)
        VALUES (i, fname, lname, 'MD', spec_id, dept_id);
        
        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL generate_providers();
DROP PROCEDURE generate_providers;

SELECT 'Providers generated' AS status, COUNT(*) AS count FROM providers;

-- ============================================================================
-- STEP 4: DIAGNOSES (50 common ICD-10 codes)
-- ============================================================================
INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description) VALUES
(1, 'I10', 'Essential hypertension'),
(2, 'E11.9', 'Type 2 diabetes without complications'),
(3, 'I50.9', 'Heart failure, unspecified'),
(4, 'J44.9', 'COPD, unspecified'),
(5, 'E78.5', 'Hyperlipidemia'),
(6, 'M79.3', 'Fibromyalgia'),
(7, 'J18.9', 'Pneumonia, unspecified'),
(8, 'N18.3', 'Chronic kidney disease, stage 3'),
(9, 'F41.9', 'Anxiety disorder, unspecified'),
(10, 'M17.9', 'Osteoarthritis of knee'),
(11, 'K21.9', 'GERD without esophagitis'),
(12, 'E66.9', 'Obesity, unspecified'),
(13, 'I25.10', 'Coronary artery disease'),
(14, 'G43.909', 'Migraine, unspecified'),
(15, 'J45.909', 'Asthma, unspecified'),
(16, 'N39.0', 'Urinary tract infection'),
(17, 'R07.9', 'Chest pain, unspecified'),
(18, 'M25.50', 'Joint pain, unspecified'),
(19, 'R51', 'Headache'),
(20, 'K59.00', 'Constipation'),
(21, 'F33.9', 'Major depressive disorder'),
(22, 'I48.91', 'Atrial fibrillation'),
(23, 'E11.65', 'Type 2 diabetes with hyperglycemia'),
(24, 'M81.0', 'Osteoporosis'),
(25, 'K80.20', 'Gallstone disease'),
(26, 'C50.919', 'Breast cancer'),
(27, 'C18.9', 'Colorectal cancer'),
(28, 'C61', 'Prostate cancer'),
(29, 'I63.9', 'Cerebral infarction'),
(30, 'S72.001A', 'Fracture of femur'),
(31, 'S82.001A', 'Fracture of tibia'),
(32, 'N40.0', 'Benign prostatic hyperplasia'),
(33, 'L40.9', 'Psoriasis'),
(34, 'D50.9', 'Iron deficiency anemia'),
(35, 'E03.9', 'Hypothyroidism'),
(36, 'E05.90', 'Hyperthyroidism'),
(37, 'K58.9', 'Irritable bowel syndrome'),
(38, 'M06.9', 'Rheumatoid arthritis'),
(39, 'G20', 'Parkinson''s disease'),
(40, 'G30.9', 'Alzheimer''s disease'),
(41, 'I21.9', 'Acute myocardial infarction'),
(42, 'I69.391', 'Dysphagia following stroke'),
(43, 'O80', 'Normal delivery'),
(44, 'P07.39', 'Preterm newborn'),
(45, 'J96.00', 'Acute respiratory failure'),
(46, 'K92.2', 'Gastrointestinal hemorrhage'),
(47, 'N17.9', 'Acute kidney failure'),
(48, 'E87.1', 'Hyponatremia'),
(49, 'R50.9', 'Fever, unspecified'),
(50, 'R10.9', 'Abdominal pain');

-- ============================================================================
-- STEP 5: PROCEDURES (40 common CPT codes)
-- ============================================================================
INSERT INTO procedures (procedure_id, cpt_code, cpt_description) VALUES
(1, '99213', 'Office visit, established patient, moderate'),
(2, '99214', 'Office visit, established patient, detailed'),
(3, '99215', 'Office visit, established patient, comprehensive'),
(4, '99223', 'Initial hospital care, high severity'),
(5, '99233', 'Subsequent hospital care, high severity'),
(6, '93000', 'Electrocardiogram'),
(7, '71020', 'Chest X-ray, 2 views'),
(8, '80053', 'Comprehensive metabolic panel'),
(9, '85025', 'Complete blood count with differential'),
(10, '36415', 'Venipuncture'),
(11, '93306', 'Echocardiography'),
(12, '70450', 'CT head without contrast'),
(13, '70553', 'MRI brain with and without contrast'),
(14, '72148', 'MRI lumbar spine without contrast'),
(15, '73562', 'X-ray knee, 3 views'),
(16, '76856', 'Ultrasound pelvis'),
(17, '88305', 'Tissue pathology'),
(18, '45378', 'Colonoscopy'),
(19, '43239', 'Upper endoscopy with biopsy'),
(20, '27447', 'Total knee arthroplasty'),
(21, '27130', 'Total hip arthroplasty'),
(22, '33533', 'CABG, arterial graft'),
(23, '47562', 'Laparoscopic cholecystectomy'),
(24, '38220', 'Bone marrow biopsy'),
(25, '96413', 'Chemotherapy administration IV'),
(26, '59400', 'Vaginal delivery'),
(27, '59510', 'Cesarean delivery'),
(28, '99281', 'ER visit, minor severity'),
(29, '99282', 'ER visit, low to moderate severity'),
(30, '99283', 'ER visit, moderate severity'),
(31, '99284', 'ER visit, high severity'),
(32, '99285', 'ER visit, critical severity'),
(33, '31500', 'Intubation, endotracheal'),
(34, '94640', 'Nebulizer therapy'),
(35, '90471', 'Immunization administration'),
(36, '90834', 'Psychotherapy, 45 minutes'),
(37, '96372', 'Therapeutic injection'),
(38, '12001', 'Simple wound repair'),
(39, '29881', 'Knee arthroscopy with meniscectomy'),
(40, '52000', 'Cystoscopy');

-- ============================================================================
-- STEP 6: PATIENTS (100,000 patients) - BATCHED
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_patients_batch;
DELIMITER //
CREATE PROCEDURE generate_patients_batch(IN start_id INT, IN end_id INT)
BEGIN
    DECLARE i INT DEFAULT start_id;
    DECLARE fname VARCHAR(100);
    DECLARE lname VARCHAR(100);
    DECLARE dob DATE;
    DECLARE gen CHAR(1);
    
    START TRANSACTION;
    
    WHILE i <= end_id DO
        -- Generate realistic names
        SET fname = CASE 
            WHEN i % 20 = 0 THEN 'James'
            WHEN i % 20 = 1 THEN 'Mary'
            WHEN i % 20 = 2 THEN 'John'
            WHEN i % 20 = 3 THEN 'Patricia'
            WHEN i % 20 = 4 THEN 'Robert'
            WHEN i % 20 = 5 THEN 'Jennifer'
            WHEN i % 20 = 6 THEN 'Michael'
            WHEN i % 20 = 7 THEN 'Linda'
            WHEN i % 20 = 8 THEN 'William'
            WHEN i % 20 = 9 THEN 'Barbara'
            WHEN i % 20 = 10 THEN 'David'
            WHEN i % 20 = 11 THEN 'Elizabeth'
            WHEN i % 20 = 12 THEN 'Richard'
            WHEN i % 20 = 13 THEN 'Susan'
            WHEN i % 20 = 14 THEN 'Joseph'
            WHEN i % 20 = 15 THEN 'Jessica'
            WHEN i % 20 = 16 THEN 'Thomas'
            WHEN i % 20 = 17 THEN 'Sarah'
            WHEN i % 20 = 18 THEN 'Charles'
            ELSE 'Karen'
        END;
        
        SET lname = CASE 
            WHEN i % 15 = 0 THEN 'Smith'
            WHEN i % 15 = 1 THEN 'Johnson'
            WHEN i % 15 = 2 THEN 'Williams'
            WHEN i % 15 = 3 THEN 'Brown'
            WHEN i % 15 = 4 THEN 'Jones'
            WHEN i % 15 = 5 THEN 'Garcia'
            WHEN i % 15 = 6 THEN 'Miller'
            WHEN i % 15 = 7 THEN 'Davis'
            WHEN i % 15 = 8 THEN 'Rodriguez'
            WHEN i % 15 = 9 THEN 'Martinez'
            WHEN i % 15 = 10 THEN 'Hernandez'
            WHEN i % 15 = 11 THEN 'Lopez'
            WHEN i % 15 = 12 THEN 'Gonzalez'
            WHEN i % 15 = 13 THEN 'Wilson'
            ELSE 'Anderson'
        END;
        
        -- Age distribution: realistic mix (more adults/elderly)
        SET dob = DATE_ADD('1930-01-01', INTERVAL FLOOR(RAND() * 30000) DAY);
        SET gen = IF(i % 2 = 0, 'M', 'F');
        
        INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, mrn)
        VALUES (i, fname, lname, dob, gen, CONCAT('MRN', LPAD(i, 8, '0')));
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
    SELECT CONCAT('Patients batch complete: ', start_id, ' to ', end_id) AS progress;
END//
DELIMITER ;

-- Generate patients in batches of 10,000
CALL generate_patients_batch(1, 10000);
CALL generate_patients_batch(10001, 20000);
CALL generate_patients_batch(20001, 30000);
CALL generate_patients_batch(30001, 40000);
CALL generate_patients_batch(40001, 50000);
CALL generate_patients_batch(50001, 60000);
CALL generate_patients_batch(60001, 70000);
CALL generate_patients_batch(70001, 80000);
CALL generate_patients_batch(80001, 90000);
CALL generate_patients_batch(90001, 100000);

DROP PROCEDURE generate_patients_batch;
SELECT 'All patients generated' AS status, COUNT(*) AS count FROM patients;

-- ============================================================================
-- STEP 7: ENCOUNTERS (500,000 encounters) - BATCHED
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_encounters_batch;
DELIMITER //
CREATE PROCEDURE generate_encounters_batch(IN start_id INT, IN end_id INT)
BEGIN
    DECLARE i INT DEFAULT start_id;
    DECLARE pat_id INT;
    DECLARE prov_id INT;
    DECLARE dept_id INT;
    DECLARE enc_type VARCHAR(50);
    DECLARE enc_date DATETIME;
    DECLARE disc_date DATETIME;
    DECLARE rand_val DECIMAL(3,2);
    DECLARE duration_hours INT;
    
    START TRANSACTION;
    
    WHILE i <= end_id DO
        -- Random patient (weighted toward having multiple encounters)
        SET pat_id = FLOOR(1 + (RAND() * 100000));
        
        -- Random provider
        SET prov_id = FLOOR(1 + (RAND() * 500));
        
        -- Get department from provider
        SELECT department_id INTO dept_id FROM providers WHERE provider_id = prov_id;
        
        -- Encounter type distribution: 60% Outpatient, 30% Inpatient, 10% ER
        SET rand_val = RAND();
        IF rand_val < 0.60 THEN
            SET enc_type = 'Outpatient';
            SET duration_hours = FLOOR(1 + (RAND() * 3)); -- 1-3 hours
        ELSEIF rand_val < 0.90 THEN
            SET enc_type = 'Inpatient';
            SET duration_hours = FLOOR(24 + (RAND() * 168)); -- 1-7 days
        ELSE
            SET enc_type = 'ER';
            SET duration_hours = FLOOR(2 + (RAND() * 10)); -- 2-12 hours
        END IF;
        
        -- Random date between 2022-01-01 and 2024-12-31
        SET enc_date = DATE_ADD('2022-01-01', INTERVAL FLOOR(RAND() * 1095) DAY);
        SET enc_date = TIMESTAMP(enc_date, SEC_TO_TIME(FLOOR(RAND() * 86400)));
        SET disc_date = DATE_ADD(enc_date, INTERVAL duration_hours HOUR);
        
        INSERT INTO encounters (encounter_id, patient_id, provider_id, encounter_type, encounter_date, discharge_date, department_id)
        VALUES (i, pat_id, prov_id, enc_type, enc_date, disc_date, dept_id);
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
    SELECT CONCAT('Encounters batch complete: ', start_id, ' to ', end_id) AS progress;
END//
DELIMITER ;

-- Generate encounters in batches of 100,000
CALL generate_encounters_batch(1, 100000);
CALL generate_encounters_batch(100001, 200000);
CALL generate_encounters_batch(200001, 300000);
CALL generate_encounters_batch(300001, 400000);
CALL generate_encounters_batch(400001, 500000);

DROP PROCEDURE generate_encounters_batch;
SELECT 'All encounters generated' AS status, COUNT(*) AS count FROM encounters;

-- ============================================================================
-- STEP 8: ENCOUNTER DIAGNOSES (1-3 diagnoses per encounter) - BATCHED
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_encounter_diagnoses_batch;
DELIMITER //
CREATE PROCEDURE generate_encounter_diagnoses_batch(IN start_enc INT, IN end_enc INT)
BEGIN
    DECLARE i INT DEFAULT start_enc;
    DECLARE enc_id INT;
    DECLARE diag_id INT;
    DECLARE num_diagnoses INT;
    DECLARE seq INT;
    
    START TRANSACTION;
    
    WHILE i <= end_enc DO
        SET enc_id = i;
        SET num_diagnoses = FLOOR(1 + (RAND() * 3)); -- 1-3 diagnoses
        SET seq = 1;
        
        WHILE seq <= num_diagnoses DO
            SET diag_id = FLOOR(1 + (RAND() * 50));
            
            INSERT INTO encounter_diagnoses (encounter_id, diagnosis_id, diagnosis_sequence)
            VALUES (enc_id, diag_id, seq);
            
            SET seq = seq + 1;
        END WHILE;
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
    SELECT CONCAT('Encounter diagnoses batch complete: ', start_enc, ' to ', end_enc) AS progress;
END//
DELIMITER ;

-- Generate encounter diagnoses in batches of 100,000
CALL generate_encounter_diagnoses_batch(1, 100000);
CALL generate_encounter_diagnoses_batch(100001, 200000);
CALL generate_encounter_diagnoses_batch(200001, 300000);
CALL generate_encounter_diagnoses_batch(300001, 400000);
CALL generate_encounter_diagnoses_batch(400001, 500000);

DROP PROCEDURE generate_encounter_diagnoses_batch;
SELECT 'All encounter diagnoses generated' AS status, COUNT(*) AS count FROM encounter_diagnoses;

-- ============================================================================
-- STEP 9: ENCOUNTER PROCEDURES (1-5 procedures per encounter) - BATCHED
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_encounter_procedures_batch;
DELIMITER //
CREATE PROCEDURE generate_encounter_procedures_batch(IN start_enc INT, IN end_enc INT)
BEGIN
    DECLARE i INT DEFAULT start_enc;
    DECLARE enc_id INT;
    DECLARE proc_id INT;
    DECLARE num_procedures INT;
    DECLARE proc_date DATE;
    DECLARE enc_date DATE;
    DECLARE proc_count INT;
    
    START TRANSACTION;
    
    WHILE i <= end_enc DO
        SET enc_id = i;
        
        -- Get encounter date
        SELECT DATE(encounter_date) INTO enc_date FROM encounters WHERE encounter_id = enc_id;
        
        -- 1-5 procedures per encounter
        SET num_procedures = FLOOR(1 + (RAND() * 5));
        SET proc_count = 1;
        
        WHILE proc_count <= num_procedures DO
            SET proc_id = FLOOR(1 + (RAND() * 40));
            SET proc_date = enc_date;
            
            INSERT INTO encounter_procedures (encounter_id, procedure_id, procedure_date)
            VALUES (enc_id, proc_id, proc_date);
            
            SET proc_count = proc_count + 1;
        END WHILE;
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
    SELECT CONCAT('Encounter procedures batch complete: ', start_enc, ' to ', end_enc) AS progress;
END//
DELIMITER ;

-- Generate encounter procedures in batches of 100,000
CALL generate_encounter_procedures_batch(1, 100000);
CALL generate_encounter_procedures_batch(100001, 200000);
CALL generate_encounter_procedures_batch(200001, 300000);
CALL generate_encounter_procedures_batch(300001, 400000);
CALL generate_encounter_procedures_batch(400001, 500000);

DROP PROCEDURE generate_encounter_procedures_batch;
SELECT 'All encounter procedures generated' AS status, COUNT(*) AS count FROM encounter_procedures;

-- ============================================================================
-- STEP 10: BILLING (1 billing record per encounter) - BATCHED
-- ============================================================================
DROP PROCEDURE IF EXISTS generate_billing_batch;
DELIMITER //
CREATE PROCEDURE generate_billing_batch(IN start_enc INT, IN end_enc INT)
BEGIN
    DECLARE i INT DEFAULT start_enc;
    DECLARE enc_id INT;
    DECLARE enc_type VARCHAR(50);
    DECLARE enc_date DATE;
    DECLARE claim_amt DECIMAL(12,2);
    DECLARE allowed_amt DECIMAL(12,2);
    DECLARE claim_dt DATE;
    DECLARE status VARCHAR(50);
    
    START TRANSACTION;
    
    WHILE i <= end_enc DO
        SET enc_id = i;
        
        SELECT encounter_type, DATE(encounter_date) 
        INTO enc_type, enc_date 
        FROM encounters WHERE encounter_id = enc_id;
        
        -- Claim amounts based on encounter type
        IF enc_type = 'Outpatient' THEN
            SET claim_amt = 200 + (RAND() * 800); -- $200-$1000
        ELSEIF enc_type = 'Inpatient' THEN
            SET claim_amt = 5000 + (RAND() * 45000); -- $5000-$50000
        ELSE -- ER
            SET claim_amt = 1000 + (RAND() * 4000); -- $1000-$5000
        END IF;
        
        -- Allowed amount is 70-90% of claim
        SET allowed_amt = claim_amt * (0.70 + (RAND() * 0.20));
        
        -- Claim date is 1-30 days after encounter
        SET claim_dt = DATE_ADD(enc_date, INTERVAL FLOOR(1 + (RAND() * 30)) DAY);
        
        -- Status distribution: 85% Paid, 10% Pending, 5% Denied
        SET status = CASE 
            WHEN RAND() < 0.85 THEN 'Paid'
            WHEN RAND() < 0.95 THEN 'Pending'
            ELSE 'Denied'
        END;
        
        INSERT INTO billing (encounter_id, claim_amount, allowed_amount, claim_date, claim_status)
        VALUES (enc_id, claim_amt, allowed_amt, claim_dt, status);
        
        SET i = i + 1;
    END WHILE;
    
    COMMIT;
    SELECT CONCAT('Billing batch complete: ', start_enc, ' to ', end_enc) AS progress;
END//
DELIMITER ;

-- Generate billing in batches of 100,000
CALL generate_billing_batch(1, 100000);
CALL generate_billing_batch(100001, 200000);
CALL generate_billing_batch(200001, 300000);
CALL generate_billing_batch(300001, 400000);
CALL generate_billing_batch(400001, 500000);

DROP PROCEDURE generate_billing_batch;
SELECT 'All billing records generated' AS status, COUNT(*) AS count FROM billing;

-- ============================================================================
-- FINAL VERIFICATION
-- ============================================================================
SELECT '========================' AS separator;
SELECT 'DATA GENERATION COMPLETE' AS status;
SELECT '========================' AS separator;

SELECT 'Table' AS table_name, 'Record Count' AS count
UNION ALL SELECT '---', '---'
UNION ALL SELECT 'patients', CAST(COUNT(*) AS CHAR) FROM patients
UNION ALL SELECT 'specialties', CAST(COUNT(*) AS CHAR) FROM specialties
UNION ALL SELECT 'departments', CAST(COUNT(*) AS CHAR) FROM departments
UNION ALL SELECT 'providers', CAST(COUNT(*) AS CHAR) FROM providers
UNION ALL SELECT 'encounters', CAST(COUNT(*) AS CHAR) FROM encounters
UNION ALL SELECT 'diagnoses', CAST(COUNT(*) AS CHAR) FROM diagnoses
UNION ALL SELECT 'encounter_diagnoses', CAST(COUNT(*) AS CHAR) FROM encounter_diagnoses
UNION ALL SELECT 'procedures', CAST(COUNT(*) AS CHAR) FROM procedures
UNION ALL SELECT 'encounter_procedures', CAST(COUNT(*) AS CHAR) FROM encounter_procedures
UNION ALL SELECT 'billing', CAST(COUNT(*) AS CHAR) FROM billing;

SELECT 
    'Encounter Type Distribution' AS metric,
    encounter_type,
    COUNT(*) AS count,
    CONCAT(ROUND(COUNT(*) * 100.0 / 500000, 1), '%') AS percentage
FROM encounters
GROUP BY encounter_type;

SELECT 
    CONCAT('Total execution time: ', TIMESTAMPDIFF(SECOND, @start_time, NOW()), ' seconds') AS summary;

SELECT 'OLTP database populated successfully!' AS result;