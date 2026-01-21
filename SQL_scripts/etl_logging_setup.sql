-- ETL LOGGING INFRASTRUCTURE SETUP
-- Purpose: Create logging tables and procedures for ETL operations
-- Run ONCE before first ETL execution

-- SECTION 1: DROP EXISTING OBJECTS (Clean Setup)

DROP PROCEDURE IF EXISTS start_etl_batch;
DROP PROCEDURE IF EXISTS log_etl_event;
DROP PROCEDURE IF EXISTS log_etl_error;
DROP PROCEDURE IF EXISTS complete_etl_batch;
DROP PROCEDURE IF EXISTS generate_batch_id;
DROP PROCEDURE IF EXISTS get_batch_summary;
DROP PROCEDURE IF EXISTS resolve_errors;

DROP TABLE IF EXISTS etl_error_records;
DROP TABLE IF EXISTS etl_log;
DROP TABLE IF EXISTS etl_batch_control;

-- SECTION 2: LOGGING TABLES

-- Main ETL execution log - tracks every step
CREATE TABLE etl_log (
    log_id INT PRIMARY KEY AUTO_INCREMENT,
    batch_id VARCHAR(50) NOT NULL,
    log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    log_level VARCHAR(10) NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    message TEXT,
    rows_affected INT DEFAULT 0,
    rows_expected INT DEFAULT NULL,
    execution_time_seconds DECIMAL(10,2),
    
    INDEX idx_batch_id (batch_id),
    INDEX idx_log_timestamp (log_timestamp),
    INDEX idx_log_level (log_level),
    INDEX idx_step_name (step_name)
);

-- Detailed error tracking for failed/skipped records
CREATE TABLE etl_error_records (
    error_id INT PRIMARY KEY AUTO_INCREMENT,
    batch_id VARCHAR(50) NOT NULL,
    error_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source_table VARCHAR(100) NOT NULL,
    source_id INT,
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT,
    field_name VARCHAR(100),
    field_value VARCHAR(500),
    resolved BOOLEAN DEFAULT FALSE,
    resolved_timestamp DATETIME,
    resolved_by VARCHAR(100),
    
    INDEX idx_batch_id (batch_id),
    INDEX idx_error_type (error_type),
    INDEX idx_resolved (resolved),
    INDEX idx_source_table (source_table)
);

-- Batch control table for tracking ETL runs
CREATE TABLE etl_batch_control (
    batch_id VARCHAR(50) PRIMARY KEY,
    batch_type VARCHAR(50) NOT NULL,
    start_timestamp DATETIME NOT NULL,
    end_timestamp DATETIME,
    status VARCHAR(20) DEFAULT 'RUNNING',
    total_rows_processed INT DEFAULT 0,
    total_errors INT DEFAULT 0,
    last_successful_encounter_id INT,
    notes TEXT,
    
    INDEX idx_status (status),
    INDEX idx_start_timestamp (start_timestamp)
);

SELECT 'Logging tables created' AS status;

-- SECTION 3: LOGGING PROCEDURES

-- Procedure: Start a new ETL batch
DELIMITER //
CREATE PROCEDURE start_etl_batch(
    IN p_batch_id VARCHAR(50),
    IN p_batch_type VARCHAR(50)
)
BEGIN
    INSERT INTO etl_batch_control (batch_id, batch_type, start_timestamp, status)
    VALUES (p_batch_id, p_batch_type, NOW(), 'RUNNING');
    
    INSERT INTO etl_log (batch_id, log_level, step_name, message)
    VALUES (p_batch_id, 'INFO', 'ETL_START', CONCAT('Starting ', p_batch_type, ' ETL batch'));
END//
DELIMITER ;

-- Procedure: Log an ETL event
DELIMITER //
CREATE PROCEDURE log_etl_event(
    IN p_batch_id VARCHAR(50),
    IN p_log_level VARCHAR(10),
    IN p_step_name VARCHAR(100),
    IN p_message TEXT,
    IN p_rows_affected INT,
    IN p_execution_seconds DECIMAL(10,2)
)
BEGIN
    INSERT INTO etl_log (
        batch_id, 
        log_level, 
        step_name, 
        message, 
        rows_affected, 
        execution_time_seconds
    )
    VALUES (
        p_batch_id, 
        p_log_level, 
        p_step_name, 
        p_message, 
        p_rows_affected, 
        p_execution_seconds
    );
    
    IF p_rows_affected IS NOT NULL AND p_rows_affected > 0 THEN
        UPDATE etl_batch_control 
        SET total_rows_processed = total_rows_processed + p_rows_affected
        WHERE batch_id = p_batch_id;
    END IF;
END//
DELIMITER ;

-- Procedure: Log an error record
DELIMITER //
CREATE PROCEDURE log_etl_error(
    IN p_batch_id VARCHAR(50),
    IN p_source_table VARCHAR(100),
    IN p_source_id INT,
    IN p_error_type VARCHAR(100),
    IN p_error_message TEXT,
    IN p_field_name VARCHAR(100),
    IN p_field_value VARCHAR(500)
)
BEGIN
    INSERT INTO etl_error_records (
        batch_id,
        source_table,
        source_id,
        error_type,
        error_message,
        field_name,
        field_value
    )
    VALUES (
        p_batch_id,
        p_source_table,
        p_source_id,
        p_error_type,
        p_error_message,
        p_field_name,
        p_field_value
    );
    
    UPDATE etl_batch_control 
    SET total_errors = total_errors + 1
    WHERE batch_id = p_batch_id;
END//
DELIMITER ;

-- Procedure: Complete an ETL batch
DELIMITER //
CREATE PROCEDURE complete_etl_batch(
    IN p_batch_id VARCHAR(50),
    IN p_status VARCHAR(20),
    IN p_notes TEXT
)
BEGIN
    UPDATE etl_batch_control 
    SET 
        end_timestamp = NOW(),
        status = p_status,
        notes = p_notes
    WHERE batch_id = p_batch_id;
    
    INSERT INTO etl_log (batch_id, log_level, step_name, message)
    VALUES (
        p_batch_id, 
        CASE WHEN p_status = 'COMPLETED' THEN 'INFO' ELSE 'ERROR' END,
        'ETL_END', 
        CONCAT('ETL batch ', p_status, ': ', COALESCE(p_notes, ''))
    );
END//
DELIMITER ;

-- Procedure: Get batch summary
DELIMITER //
CREATE PROCEDURE get_batch_summary(IN p_batch_id VARCHAR(50))
BEGIN
    SELECT 
        batch_id,
        batch_type,
        status,
        start_timestamp,
        end_timestamp,
        TIMESTAMPDIFF(MINUTE, start_timestamp, COALESCE(end_timestamp, NOW())) AS duration_minutes,
        total_rows_processed,
        total_errors
    FROM etl_batch_control
    WHERE batch_id = p_batch_id;
    
    SELECT log_level, COUNT(*) AS count
    FROM etl_log
    WHERE batch_id = p_batch_id
    GROUP BY log_level;
    
    SELECT error_type, COUNT(*) AS count
    FROM etl_error_records
    WHERE batch_id = p_batch_id
    GROUP BY error_type
    ORDER BY count DESC;
END//
DELIMITER ;

-- Procedure: Mark errors as resolved
DELIMITER //
CREATE PROCEDURE resolve_errors(
    IN p_batch_id VARCHAR(50),
    IN p_error_type VARCHAR(100),
    IN p_resolved_by VARCHAR(100)
)
BEGIN
    UPDATE etl_error_records
    SET 
        resolved = TRUE,
        resolved_timestamp = NOW(),
        resolved_by = p_resolved_by
    WHERE batch_id = p_batch_id
      AND error_type = p_error_type
      AND resolved = FALSE;
    
    SELECT ROW_COUNT() AS errors_resolved;
END//
DELIMITER ;

SELECT 'Logging procedures created' AS status;

-- SECTION 4: VERIFICATION
SELECT 'ETL LOGGING SETUP COMPLETE' AS status;

SELECT 'Tables Created:' AS info;
SELECT table_name 
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_name LIKE 'etl_%';

SELECT 'Procedures Created:' AS info;
SELECT routine_name 
FROM information_schema.routines
WHERE routine_schema = DATABASE()
  AND routine_type = 'PROCEDURE'
  AND (routine_name LIKE '%etl%' OR routine_name LIKE '%batch%');
