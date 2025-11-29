/*
================================================================================
SILVER LAYER - DATA TRANSFORMATION AND LOADING
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Transforms and loads data from Bronze to Silver layer.
             Applies data cleansing, standardization, and deduplication.

TRANSFORMATIONS APPLIED:
- Remove duplicate records (keep latest by create date)
- Standardize gender and marital status values
- Clean string data (trim whitespace)
- Handle NULL values appropriately
- Add audit columns for data lineage
- Clean line breaks and carriage returns from text fields

DATA CLEANING NOTES:
- Some source data columns contain unwanted characters like line feeds (CHAR(10)) 
  and carriage returns (CHAR(13)) that need to be removed for proper data quality
- Use pattern: REPLACE(REPLACE(TRIM(column), CHAR(10), ''), CHAR(13), '') 
- Example: WHEN REPLACE(REPLACE(TRIM(cntry), CHAR(10), ''), CHAR(13), '') = 'DE'
- This ensures clean comparison and standardization of text values

================================================================================
*/

USE DataWarehouse;
GO

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - CRM CUSTOMER INFO
-- =============================================================================

PRINT '================================================================================';
PRINT 'BRONZE TO SILVER TRANSFORMATION: CRM Customer Information';
PRINT '================================================================================';

-- Clear existing Silver data
TRUNCATE TABLE Silver.crm_cust_info;

-- Transform and insert data from Bronze to Silver
INSERT INTO Silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    dwh_date_loaded
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN cst_marital_status = 'S' THEN 'Single'
        WHEN cst_marital_status = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_marital_status,
    CASE 
        WHEN cst_gndr = 'F' THEN 'Female'
        WHEN cst_gndr = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS cst_gndr,
    cst_create_date,
    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_flag
    FROM Bronze.crm_cust_info
    WHERE cst_id IS NOT NULL -- Exclude NULL primary keys
) ranked_data
WHERE row_flag = 1; -- Keep only the latest record for each customer

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - CRM PRODUCT INFO
-- =============================================================================

PRINT 'Transforming CRM Product Information...';

-- Clear existing Silver data
TRUNCATE TABLE Silver.crm_prd_info;

-- Transform and insert product data
INSERT INTO Silver.crm_prd_info (
    prd_id,
    prd_key,
    cat_id,
    prd_key_clean,
    prd_nm,
    prd_cost,
    prd_line,
    prd_line_desc,
    prd_start_dt,
    prd_end_dt,
    dwh_date_loaded
)
SELECT
    prd_id,
    TRIM(prd_key) AS prd_key,

    -- cat_id
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

    -- cleaned prd_key after dash
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key_clean,

    TRIM(prd_nm) AS prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    TRIM(prd_line) AS prd_line,

    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'Other'
    END AS prd_line_desc,

    prd_start_dt,

    -- final end date logic
    ISNULL(
        DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)),
        prd_end_dt   -- << use end date from Bronze for last record
    ) AS prd_end_dt,

    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,   -- important
        ROW_NUMBER() OVER(PARTITION BY prd_id ORDER BY prd_start_dt DESC) AS row_flag
    FROM Bronze.crm_prd_info
    WHERE prd_id IS NOT NULL
) ranked_data
WHERE row_flag = 1;

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - CRM SALES DETAILS
-- =============================================================================

PRINT 'Transforming CRM Sales Details...';

-- Clear existing Silver data
TRUNCATE TABLE Silver.crm_sales_details;

-- Transform and insert sales data with date conversions
INSERT INTO Silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price,
    dwh_date_loaded
)
SELECT 
    TRIM(sls_ord_num) AS sls_ord_num,
    TRIM(sls_prd_key) AS sls_prd_key,
    sls_cust_id,
    -- Convert YYYYMMDD integer to proper DATE format
    CASE 
        WHEN sls_order_dt IS NOT NULL AND LEN(CAST(sls_order_dt AS VARCHAR)) = 8 
        THEN TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        ELSE NULL
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt IS NOT NULL AND LEN(CAST(sls_ship_dt AS VARCHAR)) = 8 
        THEN TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        ELSE NULL
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt IS NOT NULL AND LEN(CAST(sls_due_dt AS VARCHAR)) = 8 
        THEN TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        ELSE NULL
    END AS sls_due_dt,
    --Rules for -ve , NULL or mismatched sales quantity/price 
    --(these transformations are totally based on the current data quality)
    --1.if sales is -ve,0,NULL then derive it from quantity * price
    --2.if price is 0 or null then derive it from sales/quantity
    --3.if price is -ve make it +ve
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 THEN 
            CASE 
                WHEN sls_quantity IS NOT NULL AND sls_price IS NOT NULL THEN sls_quantity * sls_price
                ELSE 0
            END
        ELSE sls_sales
    END AS sls_sales,
    CASE 
        WHEN sls_quantity IS NULL OR sls_quantity < 0 THEN 0
        ELSE sls_quantity
    END AS sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price = 0 THEN 
            CASE 
                WHEN sls_quantity IS NOT NULL AND sls_quantity <> 0 THEN 
                    CASE 
                        WHEN sls_sales IS NOT NULL THEN sls_sales / sls_quantity
                        ELSE 0
                    END
                ELSE 0
            END
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END AS sls_price,
    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        ROW_NUMBER() OVER(PARTITION BY sls_ord_num, sls_prd_key ORDER BY sls_order_dt DESC) AS row_flag
    FROM Bronze.crm_sales_details
    WHERE sls_ord_num IS NOT NULL AND sls_prd_key IS NOT NULL -- Exclude NULL keys
) ranked_data
WHERE row_flag = 1;

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - ERP CUSTOMER DEMOGRAPHICS
-- =============================================================================

PRINT 'Transforming ERP Customer Demographics...';

-- Clear existing Silver data
TRUNCATE TABLE Silver.erp_cust_demographics;

-- Transform and insert demographics data
INSERT INTO Silver.erp_cust_demographics (
    cid,
    bdate,
    gen,
    dwh_date_loaded
)
SELECT 
    CASE WHEN cid LIKE '%NAS' THEN SUBSTRING(cid,4,LEN(cid))
        ELSE cid 
    END AS cid,
    CASE WHEN bdate>GETDATE() THEN NULL 
        ELSE bdate 
    END AS bdate,
    CASE 
        WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(10), ''), CHAR(13), '')) IN ('MALE', 'M') THEN 'Male'
        WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(10), ''), CHAR(13), '')) IN ('FEMALE', 'F') THEN 'Female'
    ELSE 'Unknown'
    END AS gen,
    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        cid,
        bdate,
        gen,
        ROW_NUMBER() OVER(PARTITION BY cid ORDER BY bdate DESC) AS row_flag
    FROM Bronze.erp_cust_demographics
    WHERE cid IS NOT NULL -- Exclude NULL primary keys
) ranked_data
WHERE row_flag = 1;

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - ERP CUSTOMER LOCATION
-- =============================================================================

PRINT 'Transforming ERP Customer Location...';

-- Clear existing Silver data
TRUNCATE TABLE Silver.erp_cust_location;

-- Transform and insert location data
INSERT INTO Silver.erp_cust_location (
    cid,
    cntry,
    dwh_date_loaded
)
SELECT 
    TRIM(cid) AS cid,
    TRIM(cntry) AS cntry,
    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        cid,
        cntry,
        ROW_NUMBER() OVER(PARTITION BY cid ORDER BY cid) AS row_flag
    FROM Bronze.erp_cust_location
    WHERE cid IS NOT NULL AND cntry IS NOT NULL -- Exclude NULL values
) ranked_data
WHERE row_flag = 1;

-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION - ERP PRODUCT CATEGORIES
-- =============================================================================

PRINT 'Transforming ERP Product Categories...';

-- Clear existing Silver data
TRUNCATE TABLE Silver.erp_product_categories;

-- Transform and insert category data
INSERT INTO Silver.erp_product_categories (
    id,
    cat,
    subcat,
    maintenance,
    dwh_date_loaded
)
SELECT 
    TRIM(id) AS id,
    TRIM(cat) AS cat,
    TRIM(subcat) AS subcat,
    CASE 
        WHEN UPPER(TRIM(maintenance)) IN ('YES', 'Y', '1', 'TRUE') THEN 'Yes'
        WHEN UPPER(TRIM(maintenance)) IN ('NO', 'N', '0', 'FALSE') THEN 'No'
        ELSE 'Unknown'
    END AS maintenance,
    GETDATE() AS dwh_date_loaded
FROM (
    SELECT 
        id,
        cat,
        subcat,
        maintenance,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS row_flag
    FROM Bronze.erp_product_categories
    WHERE id IS NOT NULL -- Exclude NULL primary keys
) ranked_data
WHERE row_flag = 1;

-- =============================================================================
-- TRANSFORMATION VERIFICATION
-- =============================================================================

PRINT 'TRANSFORMATION VERIFICATION';
PRINT '---------------------------';

-- Verify record counts
SELECT 'CRM Customer Info' AS table_name, COUNT(*) AS silver_count FROM Silver.crm_cust_info
UNION ALL
SELECT 'CRM Product Info', COUNT(*) FROM Silver.crm_prd_info
UNION ALL
SELECT 'CRM Sales Details', COUNT(*) FROM Silver.crm_sales_details
UNION ALL
SELECT 'ERP Customer Demographics', COUNT(*) FROM Silver.erp_cust_demographics
UNION ALL
SELECT 'ERP Customer Location', COUNT(*) FROM Silver.erp_cust_location
UNION ALL
SELECT 'ERP Product Categories', COUNT(*) FROM Silver.erp_product_categories;

-- Verify data quality improvements
SELECT 
    'DATA_QUALITY_VERIFICATION' AS check_type,
    COUNT(DISTINCT cst_id) AS unique_customers,
    COUNT(*) AS total_records,
    SUM(CASE WHEN cst_marital_status IN ('Single', 'Married', 'Unknown') THEN 1 ELSE 0 END) AS standardized_marital_status,
    SUM(CASE WHEN cst_gndr IN ('Male', 'Female', 'Unknown') THEN 1 ELSE 0 END) AS standardized_gender
FROM Silver.crm_cust_info;

PRINT '================================================================================';
PRINT 'BRONZE TO SILVER TRANSFORMATION COMPLETED SUCCESSFULLY';
PRINT '================================================================================';