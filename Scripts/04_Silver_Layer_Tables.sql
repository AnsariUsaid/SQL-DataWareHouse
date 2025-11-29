/*
================================================================================
SILVER LAYER - TABLE CREATION SCRIPT
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Creates Silver layer tables for cleansed and standardized data.
             This script creates tables for both CRM and ERP source systems.

SILVER LAYER CHARACTERISTICS:
- Cleansed and standardized data from Bronze layer
- Data quality improvements and business rule enforcement
- Batch processing with Full Load, Truncate & Insert pattern
- Added audit columns for data lineage tracking
- Data type conversions and formatting applied

================================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- CRM SYSTEM TABLES (Silver Layer - Cleansed Data)
-- =============================================================================

-- Create CRM Customer Information table (cleansed and standardized)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'crm_cust_info')
BEGIN
    CREATE TABLE Silver.crm_cust_info(
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_marital_status NVARCHAR(50), -- Standardized marital status values
        cst_gndr NVARCHAR(50),
        cst_create_date DATE,
        dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
    );
END
GO

-- Create CRM Product Information table (cleansed and standardized)
-- Drop existing table if it exists to ensure proper structure
IF EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'crm_prd_info')
    DROP TABLE Silver.crm_prd_info;

CREATE TABLE Silver.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    cat_id NVARCHAR(50), -- Category ID extracted from original prd_key
    prd_key_clean NVARCHAR(50), -- Cleaned product key after dash
    prd_nm NVARCHAR(255),
    prd_cost DECIMAL(10,2),
    prd_line NVARCHAR(50),
    prd_line_desc NVARCHAR(50), -- Descriptive product line names
    prd_start_dt DATE,
    prd_end_dt DATE, -- End date from original data or calculated via LEAD for duplicates
    dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
);
GO

-- Create CRM Sales Details table (cleansed and standardized)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'crm_sales_details')
BEGIN
    CREATE TABLE Silver.crm_sales_details(
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt DATE, -- Converted from YYYYMMDD INT to proper DATE
        sls_ship_dt DATE,  -- Converted from YYYYMMDD INT to proper DATE
        sls_due_dt DATE,   -- Converted from YYYYMMDD INT to proper DATE
        sls_sales DECIMAL(10,2),
        sls_quantity INT,
        sls_price DECIMAL(10,2),
        dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
    );
END
GO

-- =============================================================================
-- ERP SYSTEM TABLES (Silver Layer - Cleansed Data)
-- =============================================================================

-- Create ERP Customer Demographics table (cleansed and standardized)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'erp_cust_demographics')
BEGIN
    CREATE TABLE Silver.erp_cust_demographics(
        cid NVARCHAR(50),
        bdate DATE,
        gen NVARCHAR(20), -- Standardized gender values
        dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
    );
END
GO

-- Create ERP Customer Location table (cleansed and standardized)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'erp_cust_location')
BEGIN
    CREATE TABLE Silver.erp_cust_location(
        cid NVARCHAR(50),
        cntry NVARCHAR(100), -- Standardized country names
        dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
    );
END
GO

-- Create ERP Product Categories table (cleansed and standardized)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE schema_id = SCHEMA_ID('Silver') AND name = 'erp_product_categories')
BEGIN
    CREATE TABLE Silver.erp_product_categories(
        id NVARCHAR(50),
        cat NVARCHAR(100),
        subcat NVARCHAR(100),
        maintenance NVARCHAR(10), -- Standardized Yes/No values
        dwh_date_loaded DATETIME2 DEFAULT GETDATE() -- Audit column for data lineage
    );
END
GO

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- List all Silver layer tables
SELECT 
    t.name AS TableName,
    s.name AS SchemaName,
    t.create_date AS CreatedDate
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'Silver'
ORDER BY t.name;

-- Get row counts for all Silver tables (will be 0 until data is loaded)
SELECT 
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    SUM(p.[rows]) AS RecordCount
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE t.schema_id = SCHEMA_ID('Silver')
AND p.index_id IN (0, 1)
GROUP BY t.schema_id, t.name
ORDER BY t.name;