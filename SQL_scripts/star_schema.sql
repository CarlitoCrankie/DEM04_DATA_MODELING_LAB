-- HEALTH ANALYTICS STAR SCHEMA
-- Dimension modelling optimized for querying analytical data
-- Grain: One row per encounter in the fact table
-- Pattern: Star Schema with bridge tables for many-to-many relationships

-- Dimension Tables
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_patient;
DROP TABLE IF EXISTS dim_specialty;
DROP TABLE IF EXISTS dim_provider;
DROP TABLE IF EXISTS dim_department;
DROP TABLE IF EXISTS dim_encounter_type;
DROP TABLE IF EXISTS dim_diagnosis;
DROP TABLE IF EXISTS dim_procedure;
DROP TABLE IF EXISTS fact_encounters;
DROP TABLE IF EXISTS bridge_encounter_diagnoses;
DROP TABLE IF EXISTS bridge_encounter_procedures;


-- dim_date : Pre-computed date attributes for time-based analysis
/*
    Purpose: Eliminate date functions in queries; enable fast time filtering
    Load: One-time load for date range (e.g., 2020-2030)
*/
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,                    -- Format: YYYYMMDD (e.g., 20240510)
    calendar_date DATE NOT NULL UNIQUE,          -- Actual date value    
    -- Calendar attributes
    year INT NOT NULL,                           -- 2024
    quarter INT NOT NULL,                        -- 1-4
    quarter_name VARCHAR(10) NOT NULL,           -- 'Q2 2024'
    month INT NOT NULL,                          -- 1-12
    month_name VARCHAR(20) NOT NULL,             -- 'May'
    month_year VARCHAR(20) NOT NULL,             -- 'May 2024'
    day_of_month INT NOT NULL,                   -- 1-31
    day_of_week INT NOT NULL,                    -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,               -- 'Friday'
    week_of_year INT NOT NULL,                   -- 1-53

    -- Business attributes
    is_weekend BOOLEAN NOT NULL,                 -- TRUE for Sat/Sun
    is_holiday BOOLEAN DEFAULT FALSE,            -- TRUE for recognized holidays
    
    -- Fiscal calendar (if different from calendar year)
    fiscal_year INT NOT NULL,                    -- Fiscal year
    fiscal_quarter INT NOT NULL,                 -- Fiscal quarter
    fiscal_period VARCHAR(20),                   -- 'FY2024-Q2'
    
    INDEX idx_calendar_date (calendar_date),
    INDEX idx_year_month (year, month),
    INDEX idx_fiscal_year (fiscal_year)
);

-- dim_patient: Patient demographics and attributes
/*
    Purpose: Describe WHO the patient is
    SCD Type: Type 1 (overwrite) for most attributes
*/
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    patient_id INT NOT NULL,                     -- Natural key from source
    mrn VARCHAR(20) NOT NULL,                    -- Medical Record Number
    
    -- Demographics
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                      -- Concatenated for display
    date_of_birth DATE NOT NULL,
    gender CHAR(1),                              -- 'M', 'F', 'O', 'U'
    
    -- Derived attributes for analysis
    age_group VARCHAR(20),                       -- '0-17', '18-34', '35-49', '50-64', '65+'
    
    -- Audit columns
    source_system VARCHAR(50),                   -- Which system this patient came from
    effective_date DATE,                         -- When this record became effective
    is_current BOOLEAN DEFAULT TRUE,             -- For SCD Type 2 if needed
    
    UNIQUE INDEX idx_patient_id (patient_id),
    UNIQUE INDEX idx_mrn (mrn),
    INDEX idx_age_group (age_group)
);

-- dim_specialty: Medical specialties
/*
    Purpose: Describe medical specialties independently
    Note: Also denormalized into dim_provider for query performance
*/
CREATE TABLE dim_specialty (
    specialty_key INT PRIMARY KEY AUTO_INCREMENT, -- Surrogate key
    specialty_id INT NOT NULL,                    -- Natural key from source
    specialty_name VARCHAR(100) NOT NULL,         -- 'Cardiology'
    specialty_code VARCHAR(10) NOT NULL,          -- 'CARD'
    
    UNIQUE INDEX idx_specialty_id (specialty_id),
    INDEX idx_specialty_name (specialty_name)
);

-- dim_provider
/*
    Purpose: Describe WHO provided the care
    Denormalization: Includes specialty attributes for query performance
*/
CREATE TABLE dim_provider (
    provider_key INT PRIMARY KEY AUTO_INCREMENT,  -- Surrogate key
    provider_id INT NOT NULL,                     -- Natural key from source
    
    -- Provider details
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),                       -- 'Dr. James Chen'
    credential VARCHAR(20),                       -- 'MD', 'DO', 'NP', 'PA'
    
    -- DENORMALIZED: Specialty information (from specialties table)
    specialty_id INT NOT NULL,                    -- Keep FK for integrity
    specialty_name VARCHAR(100) NOT NULL,         -- DENORMALIZED
    specialty_code VARCHAR(10) NOT NULL,          -- DENORMALIZED
    
    UNIQUE INDEX idx_provider_id (provider_id),
    INDEX idx_specialty_name (specialty_name),    -- Common filter
    INDEX idx_full_name (full_name)
);

-- dim_department: Hospital departments and locations
/*
    Purpose: Describes WHERE the care was provided
*/
CREATE TABLE dim_department(
    department_key INT PRIMARY KEY AUTO_INCREMENT, -- Surrogate key
    department_id INT NOT NULL,                    -- Natural key from source
    department_name VARCHAR(100) NOT NULL,         -- 'Cardiology Unit'
    
    -- Location details
    floor INT,                                     -- Physical floor number
    capacity INT,                                  -- Number of beds/rooms
    
    UNIQUE INDEX idx_department_id (department_id),
    INDEX idx_department_name (department_name)
);

-- dim_encounter_type: Types of patient encounters
/*
    Purpose: Describe WHAT type of encounter occurred
    Note: Small dimension (3-10 values) but kept separate for consistency
*/
CREATE TABLE dim_encounter_type (
    encounter_type_key INT PRIMARY KEY AUTO_INCREMENT, -- Surrogate key
    encounter_type_name VARCHAR(50) NOT NULL,          -- 'Inpatient', 'Outpatient', 'ER'
    encounter_type_code VARCHAR(10) NOT NULL,          -- 'IP', 'OP', 'ER'
    
    -- Optional attributes for analysis
    requires_admission BOOLEAN DEFAULT FALSE,          -- TRUE for inpatient
    average_duration_hours DECIMAL(5,2),               -- For forecasting
    
    UNIQUE INDEX idx_encounter_type_name (encounter_type_name)
);

-- dim diagnosis: ICD-10 diagnosis codes and decriptions
/*
    Purpose: Describe diagnoses (referenced by bridge table)
*/
CREATE TABLE dim_diagnosis (
    diagnosis_key INT PRIMARY KEY AUTO_INCREMENT, -- Surrogate key
    diagnosis_id INT NOT NULL,                    -- Natural key from source
    icd10_code VARCHAR(10) NOT NULL,              -- 'I10'
    icd10_description VARCHAR(200) NOT NULL,      -- 'Essential Hypertension'
    
    UNIQUE INDEX idx_diagnosis_id (diagnosis_id),
    INDEX idx_icd10_code (icd10_code)
);

-- dim_procedure: CPT procedures and descriptions
/*
    Purpose: Describe procedures (referenced by bridge table)
*/
CREATE TABLE dim_procedure (
    procedure_key INT PRIMARY KEY AUTO_INCREMENT, -- Surrogate key
    procedure_id INT NOT NULL,                    -- Natural key from source
    cpt_code VARCHAR(10) NOT NULL,                -- '99213'
    cpt_description VARCHAR(200) NOT NULL,        -- 'Office Visit - Level 3'
    
    -- Optional attributes
    procedure_category VARCHAR(50),               -- 'Evaluation', 'Diagnostic', 'Therapeutic'
    average_duration_minutes INT,                 -- For scheduling
    
    UNIQUE INDEX idx_procedure_id (procedure_id),
    INDEX idx_cpt_code (cpt_code)
);

-- FACT TABLE
/*
    fact_encounters: Core fact table - one row per patient encounter
    Grain: One row represents one patient encounter
    Purpose: Central table for all encounter-based analytics
*/
CREATE TABLE fact_encounters (
    encounter_key INT PRIMARY KEY AUTO_INCREMENT,     -- Surrogate key
    encounter_id INT NOT NULL,                        -- Natural key from source
    
    -- Foreign keys to dimensions (the "star" joins)
    date_key INT NOT NULL,                            -- When encounter occurred
    patient_key INT NOT NULL,                         -- Who was the patient
    provider_key INT NOT NULL,                        -- Who was the provider
    specialty_key INT NOT NULL,                       -- What specialty
    department_key INT NOT NULL,                      -- Where encounter occurred
    encounter_type_key INT NOT NULL,                  -- What type of encounter
    
    -- Degenerate dimensions (attributes that don't warrant own dimension)
    encounter_date DATETIME NOT NULL,                 -- Exact timestamp
    discharge_date DATETIME,                          -- NULL for outpatient
    
    -- PRE-AGGREGATED METRICS (calculated during ETL)
    diagnosis_count INT DEFAULT 0,                    -- Number of diagnoses
    procedure_count INT DEFAULT 0,                    -- Number of procedures
    total_claim_amount DECIMAL(12,2),                 -- Sum of all claims
    total_allowed_amount DECIMAL(12,2),               -- Actual revenue
    length_of_stay_days INT,                          -- NULL for outpatient
    
    -- Audit columns
    etl_loaded_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    
    -- Indexes for common query patterns
    UNIQUE INDEX idx_encounter_id (encounter_id),
    INDEX idx_date_key (date_key),                    -- Time-based queries
    INDEX idx_patient_key (patient_key),              -- Patient history queries
    INDEX idx_provider_key (provider_key),            -- Provider performance
    INDEX idx_specialty_key (specialty_key),          -- Specialty analysis
    INDEX idx_encounter_type_key (encounter_type_key), -- Type-based filtering
    INDEX idx_encounter_date (encounter_date),         -- Date range queries
    INDEX idx_patient_date (patient_key, date_key)     -- Patient timeline
);


-- BRIDGE TABLES FOR MANY-TO-MANY RELATIONSHIPS

-- bridge_encounter_diagnoses: Links encounters to diagnoses
CREATE TABLE bridge_encounter_diagnoses (
    encounter_key INT NOT NULL,
    diagnosis_key INT NOT NULL,
    diagnosis_sequence INT NOT NULL,
    
    PRIMARY KEY (encounter_key, diagnosis_key, diagnosis_sequence),
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    
    INDEX idx_diagnosis_key (diagnosis_key),
    INDEX idx_encounter_key (encounter_key)
);

-- bridge_encounter_procedures: Links encounters to procedures
CREATE TABLE bridge_encounter_procedures (
    encounter_key INT NOT NULL,
    procedure_key INT NOT NULL,
    procedure_date DATE NOT NULL,
    procedure_sequence INT,
    
    PRIMARY KEY (encounter_key, procedure_key, procedure_sequence),
    
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    
    INDEX idx_procedure_key (procedure_key),
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_date (procedure_date)
);
