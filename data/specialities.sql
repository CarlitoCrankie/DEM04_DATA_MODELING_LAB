-- Delete existing billing records (safe cleanup)
START TRANSACTION;
DELETE FROM billing WHERE billing_id BETWEEN 14001 AND 24000;
COMMIT;

-- Insert new billing records
INSERT INTO billing (
    billing_id, encounter_id, claim_amount,
    allowed_amount, claim_date, claim_status
)
SELECT
    14000 + s.gs AS billing_id,
    7000 + s.gs AS encounter_id,
    s.claim_amount,
    s.claim_amount * 0.8 AS allowed_amount,
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 1095) DAY) AS claim_date,
    ELT(1 + FLOOR(RAND() * 3), 'Pending','Paid','Denied') AS claim_status
FROM (
    SELECT
        (units.u + tens.t*10 + hundreds.h*100 + thousands.th*1000) + 1 AS gs,
        ROUND(100 + RAND() * 4900, 2) AS claim_amount
    FROM (SELECT 0 u UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) units
    CROSS JOIN (SELECT 0 t UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN (SELECT 0 h UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN (SELECT 0 th UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) thousands
    ORDER BY gs
    LIMIT 10000
) s;


-- Delete existing encounter_diagnoses records (safe cleanup)
START TRANSACTION;
DELETE FROM encounter_diagnoses WHERE encounter_diagnosis_id BETWEEN 8001 AND 18000;
COMMIT;

-- Insert new encounter_diagnoses records
INSERT INTO encounter_diagnoses (
    encounter_diagnosis_id, encounter_id, diagnosis_id, diagnosis_sequence
)
SELECT
    8000 + s.gs AS encounter_diagnosis_id,
    (SELECT encounter_id FROM encounters ORDER BY RAND() LIMIT 1) AS encounter_id,
    (SELECT diagnosis_id FROM diagnoses ORDER BY RAND() LIMIT 1) AS diagnosis_id,
    1 + FLOOR(RAND() * 4) AS diagnosis_sequence
FROM (
    SELECT (units.u + tens.t*10 + hundreds.h*100 + thousands.th*1000) + 1 AS gs
    FROM (SELECT 0 u UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) units
    CROSS JOIN (SELECT 0 t UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN (SELECT 0 h UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN (SELECT 0 th UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) thousands
    ORDER BY gs
    LIMIT 10000
) s;


-- Delete existing encounter_procedures records (safe cleanup)
START TRANSACTION;
DELETE FROM encounter_procedures WHERE encounter_procedure_id BETWEEN 9001 AND 19000;
COMMIT;

-- Insert new encounter_procedures records
INSERT INTO encounter_procedures (
    encounter_procedure_id, encounter_id, procedure_id, procedure_date
)
SELECT
    9000 + s.gs AS encounter_procedure_id,
    (SELECT encounter_id FROM encounters ORDER BY RAND() LIMIT 1) AS encounter_id,
    (SELECT procedure_id FROM procedures ORDER BY RAND() LIMIT 1) AS procedure_id,
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 1095) DAY) AS procedure_date
FROM (
    SELECT (units.u + tens.t*10 + hundreds.h*100 + thousands.th*1000) + 1 AS gs
    FROM (SELECT 0 u UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) units
    CROSS JOIN (SELECT 0 t UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN (SELECT 0 h UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN (SELECT 0 th UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) thousands
    ORDER BY gs
    LIMIT 10000
) s;




INSERT INTO diagnoses (diagnosis_id, icd10_code, icd10_description) VALUES
-- Cardiology
(3001, 'I10',    'Essential (Primary) Hypertension'),
(3002, 'I50.9',  'Heart Failure, Unspecified'),
(3003, 'I25.10', 'Coronary Artery Disease'),
(3004, 'I48.91', 'Atrial Fibrillation'),
(3005, 'R07.9',  'Chest Pain, Cardiac Origin'),
-- Internal Medicine
(3010, 'E11.9',  'Type 2 Diabetes Mellitus Without Complications'),
(3011, 'E78.5',  'Hyperlipidemia, Unspecified'),
(3012, 'K21.9',  'Gastro-esophageal Reflux Disease'),
(3013, 'N18.9',  'Chronic Kidney Disease, Unspecified'),
(3014, 'J06.9',  'Upper Respiratory Infection'),
-- Neurology
(3020, 'G43.909', 'Migraine, Unspecified'),
(3021, 'I63.9',   'Cerebral Infarction (Stroke)'),
(3022, 'G40.909', 'Epilepsy, Unspecified'),
(3023, 'R51.9',   'Headache'),
(3024, 'F32.9',   'Major Depressive Disorder'),
-- Emergency Unit
(3030, 'S06.0X0A', 'Concussion Without Loss of Consciousness'),
(3031, 'R07.9',    'Acute Chest Pain'),
(3032, 'J18.9',    'Pneumonia, Unspecified'),
(3033, 'T14.90',   'Injury, Unspecified'),
(3034, 'R10.9',    'Abdominal Pain, Unspecified');


INSERT INTO procedures (procedure_id, cpt_code, cpt_description) VALUES
-- Cardiology
(4001, '93000', 'Electrocardiogram (EKG)'),
(4002, '93306', 'Echocardiogram'),
(4003, '92928', 'Coronary Angioplasty with Stent'),
(4004, '93015', 'Cardiac Stress Test'),
(4005, '93224', 'Holter Monitor Recording'),
-- Internal Medicine
(4010, '80053', 'Comprehensive Metabolic Panel'),
(4011, '85025', 'Complete Blood Count'),
(4012, '83036', 'Hemoglobin A1C'),
(4013, '36415', 'Venipuncture'),
(4014, '81002', 'Urinalysis'),
-- Neurology
(4020, '95816', 'Electroencephalogram (EEG)'),
(4021, '70551', 'MRI Brain Without Contrast'),
(4022, '70450', 'CT Head Without Contrast'),
(4023, '96116', 'Neurobehavioral Status Exam'),
(4024, '95957', 'Digital EEG Analysis'),
-- Emergency Unit
(4030, '71045', 'Chest X-ray, Single View'),
(4031, '72125', 'CT Cervical Spine'),
(4032, '96372', 'Therapeutic Injection'),
(4033, '12001', 'Simple Wound Repair'),
(4034, '99285', 'Emergency Department Visit, High Severity');
