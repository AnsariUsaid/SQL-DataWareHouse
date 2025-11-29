/*
================================================================================
BRONZE LAYER - TABLE CREATION SCRIPT
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Creates Bronze layer tables for raw data ingestion from CSV sources.
             This script creates tables for both CRM and ERP source systems.

BRONZE LAYER CHARACTERISTICS:
- Raw data with no transformations
- Maintains original data types and structure
- Batch processing with Full Load, Truncate & Insert pattern
- Serves as landing zone for source data

================================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- CRM SYSTEM TABLES (Source: source_crm folder)
-- =============================================================================

-- Create CRM Customer Information table (already exists - keeping for reference)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'crm_cust_info')
BEGIN
    CREATE TABLE Bronze.crm_cust_info(
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_marital_status NVARCHAR(50), -- Fixed typo from original CSV
        cst_gndr NVARCHAR(50),
        cst_create_date DATE
    );
END
GO

-- Create CRM Product Information table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'crm_prd_info')
BEGIN
    CREATE TABLE Bronze.crm_prd_info(
        prd_id INT,
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(255),
        prd_cost DECIMAL(10,2),
        prd_line NVARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE
    );
END
GO

-- Create CRM Sales Details table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'crm_sales_details')
BEGIN
    CREATE TABLE Bronze.crm_sales_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt INT, -- Stored as YYYYMMDD format in source
        sls_ship_dt INT,  -- Stored as YYYYMMDD format in source
        sls_due_dt INT,   -- Stored as YYYYMMDD format in source
        sls_sales DECIMAL(10,2),
        sls_quantity INT,
        sls_price DECIMAL(10,2)
    );
END
GO

-- =============================================================================
-- ERP SYSTEM TABLES (Source: source_erp folder)
-- =============================================================================

-- Create ERP Customer Demographics table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'erp_cust_demographics')
BEGIN
    CREATE TABLE Bronze.erp_cust_demographics(
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(20)
    );
END
GO

-- Create ERP Customer Location table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'erp_cust_location')
BEGIN
    CREATE TABLE Bronze.erp_cust_location(
        cid NVARCHAR(50),
        cntry NVARCHAR(100)
    );
END
GO

-- Create ERP Product Categories table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Bronze') AND name = 'erp_product_categories')
BEGIN
    CREATE TABLE Bronze.erp_product_categories(
        id NVARCHAR(50),
        cat NVARCHAR(100),
        subcat NVARCHAR(100),
        maintenance NVARCHAR(10)
    );
END
GO

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- List all Bronze layer tables
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date AS CreatedDate
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'Bronze'
ORDER BY t.name;

-- Get row counts for all Bronze tables (will be 0 until data is loaded)
SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    SUM(p.[rows]) AS RecordCount
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.schema_id = SCHEMA_ID('Bronze')
AND p.index_id IN (0, 1)
GROUP BY t.schema_id, t.name
ORDER BY t.name;