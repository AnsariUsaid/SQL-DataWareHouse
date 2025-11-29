/*
================================================================================
SILVER LAYER - DATA QUALITY CHECKS
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Data quality validation checks for Silver layer tables.
             Focus on CRM customer information table quality assessment.

QUALITY CHECKS PERFORMED:
- Duplicate primary key detection
- NULL value validation
- Data format consistency checks
- Record count validation

================================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- CRM CUSTOMER INFO - DATA QUALITY CHECKS
-- =============================================================================

PRINT '================================================================================';
PRINT 'DATA QUALITY ASSESSMENT: Bronze.crm_cust_info';
PRINT '================================================================================';

-- Check 1: Duplicate Primary Keys Detection
PRINT 'CHECK 1: Duplicate Primary Keys (cst_id)';
PRINT '----------------------------------------';

SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM Bronze.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1;

-- Check 2: NULL Primary Keys Detection
PRINT 'CHECK 2: NULL Primary Keys';
PRINT '--------------------------';

SELECT 
    cst_id,
    COUNT(*) AS null_key_count
FROM Bronze.crm_cust_info 
GROUP BY cst_id 
HAVING cst_id IS NULL;

-- Check 3: Combined Duplicate and NULL Check
PRINT 'CHECK 3: All Data Quality Issues (Duplicates + NULLs)';
PRINT '----------------------------------------------------';

SELECT 
    cst_id,
    COUNT(*) AS record_count,
    CASE 
        WHEN cst_id IS NULL THEN 'NULL_PRIMARY_KEY'
        WHEN COUNT(*) > 1 THEN 'DUPLICATE_PRIMARY_KEY'
        ELSE 'VALID'
    END AS issue_type
FROM Bronze.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check 4: Data Completeness Analysis
PRINT 'CHECK 4: NULL Values Analysis by Column';
PRINT '--------------------------------------';

SELECT 
    'NULL_ANALYSIS' AS check_type,
    SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS null_cst_id,
    SUM(CASE WHEN cst_key IS NULL THEN 1 ELSE 0 END) AS null_cst_key,
    SUM(CASE WHEN cst_firstname IS NULL OR TRIM(cst_firstname) = '' THEN 1 ELSE 0 END) AS null_empty_firstname,
    SUM(CASE WHEN cst_lastname IS NULL OR TRIM(cst_lastname) = '' THEN 1 ELSE 0 END) AS null_empty_lastname,
    SUM(CASE WHEN cst_marital_status IS NULL THEN 1 ELSE 0 END) AS null_marital_status,
    SUM(CASE WHEN cst_gndr IS NULL THEN 1 ELSE 0 END) AS null_gender,
    SUM(CASE WHEN cst_create_date IS NULL THEN 1 ELSE 0 END) AS null_create_date,
    COUNT(*) AS total_records
FROM Bronze.crm_cust_info;

-- Check 5: Data Format Validation
PRINT 'CHECK 5: Data Format Issues';
PRINT '---------------------------';

-- Gender values validation
SELECT 
    'GENDER_VALUES' AS check_type,
    cst_gndr,
    COUNT(*) AS record_count
FROM Bronze.crm_cust_info
WHERE cst_gndr IS NOT NULL
GROUP BY cst_gndr
ORDER BY record_count DESC;

-- Marital status values validation
SELECT 
    'MARITAL_STATUS_VALUES' AS check_type,
    cst_marital_status,
    COUNT(*) AS record_count
FROM Bronze.crm_cust_info
WHERE cst_marital_status IS NOT NULL
GROUP BY cst_marital_status
ORDER BY record_count DESC;

-- Check 6: String Data Quality Issues
PRINT 'CHECK 6: String Data Quality Issues';
PRINT '----------------------------------';

SELECT 
    'STRING_QUALITY_ISSUES' AS check_type,
    SUM(CASE WHEN cst_firstname != TRIM(cst_firstname) THEN 1 ELSE 0 END) AS firstname_whitespace_issues,
    SUM(CASE WHEN cst_lastname != TRIM(cst_lastname) THEN 1 ELSE 0 END) AS lastname_whitespace_issues,
    SUM(CASE WHEN LEN(TRIM(cst_firstname)) = 0 AND cst_firstname IS NOT NULL THEN 1 ELSE 0 END) AS empty_firstname,
    SUM(CASE WHEN LEN(TRIM(cst_lastname)) = 0 AND cst_lastname IS NOT NULL THEN 1 ELSE 0 END) AS empty_lastname
FROM Bronze.crm_cust_info;

-- Check 7: Summary Statistics
PRINT 'CHECK 7: Overall Data Quality Summary';
PRINT '------------------------------------';

WITH QualityMetrics AS (
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT cst_id) AS unique_customers,
        COUNT(*) - COUNT(DISTINCT cst_id) AS duplicate_records,
        SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS null_primary_keys,
        SUM(CASE WHEN cst_firstname IS NULL OR TRIM(cst_firstname) = '' THEN 1 ELSE 0 END) AS incomplete_names
    FROM Bronze.crm_cust_info
)
SELECT 
    'QUALITY_SUMMARY' AS check_type,
    total_records,
    unique_customers,
    duplicate_records,
    null_primary_keys,
    incomplete_names,
    ROUND((CAST(unique_customers AS FLOAT) / total_records * 100), 2) AS data_uniqueness_pct,
    ROUND((CAST(total_records - null_primary_keys - duplicate_records - incomplete_names AS FLOAT) / total_records * 100), 2) AS data_quality_score_pct
FROM QualityMetrics;

PRINT '================================================================================';
PRINT 'DATA QUALITY ASSESSMENT COMPLETED';
PRINT '================================================================================';