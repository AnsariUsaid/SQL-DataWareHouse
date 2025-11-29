/*
================================================================================
BRONZE LAYER - DATA LOADING SCRIPT (DOCKER CONTAINER PATHS)
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Loads raw CSV data into Bronze layer tables.
             Uses Docker container internal paths for SQL Server access.

PREREQUISITES:
- CSV files copied to Docker container using:
  docker cp <local_file> sqlserver:/var/opt/mssql/data/

================================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- CLEAR EXISTING DATA (TRUNCATE TABLES)
-- =============================================================================

TRUNCATE TABLE Bronze.crm_cust_info;
TRUNCATE TABLE Bronze.crm_prd_info;
TRUNCATE TABLE Bronze.crm_sales_details;
TRUNCATE TABLE Bronze.erp_cust_demographics;
TRUNCATE TABLE Bronze.erp_cust_location;
TRUNCATE TABLE Bronze.erp_product_categories;

-- =============================================================================
-- BULK INSERT DATA FROM CSV FILES
-- =============================================================================

-- Load CRM Customer Information
BULK INSERT Bronze.crm_cust_info
FROM '/var/opt/mssql/data/cust_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load CRM Product Information  
BULK INSERT Bronze.crm_prd_info
FROM '/var/opt/mssql/data/prd_info.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load CRM Sales Details
BULK INSERT Bronze.crm_sales_details
FROM '/var/opt/mssql/data/sales_details.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load ERP Customer Demographics
BULK INSERT Bronze.erp_cust_demographics
FROM '/var/opt/mssql/data/CUST_AZ12.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load ERP Customer Location
BULK INSERT Bronze.erp_cust_location
FROM '/var/opt/mssql/data/LOC_A101.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load ERP Product Categories
BULK INSERT Bronze.erp_product_categories
FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- =============================================================================
-- DATA VERIFICATION QUERIES
-- =============================================================================

-- Verify data loaded successfully
SELECT 'CRM Customer Info' AS TableName, COUNT(*) AS RecordCount FROM Bronze.crm_cust_info
UNION ALL
SELECT 'CRM Product Info', COUNT(*) FROM Bronze.crm_prd_info
UNION ALL  
SELECT 'CRM Sales Details', COUNT(*) FROM Bronze.crm_sales_details
UNION ALL
SELECT 'ERP Customer Demographics', COUNT(*) FROM Bronze.erp_cust_demographics
UNION ALL
SELECT 'ERP Customer Location', COUNT(*) FROM Bronze.erp_cust_location
UNION ALL
SELECT 'ERP Product Categories', COUNT(*) FROM Bronze.erp_product_categories;

-- Sample data verification
SELECT TOP 5 * FROM Bronze.crm_cust_info;
SELECT TOP 5 * FROM Bronze.crm_prd_info;
SELECT TOP 5 * FROM Bronze.crm_sales_details;