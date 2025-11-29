/*
================================================================================
SQL DATA WAREHOUSE PROJECT - DATABASE INITIALIZATION SCRIPT
================================================================================

Project: Modern Data Warehouse Architecture Implementation
Author: Usaid
Created: 2024
Description: Initializes foundational database structure for a modern data 
             warehouse following the medallion architecture pattern.

ARCHITECTURE OVERVIEW:
- Bronze Layer: Raw data ingestion zone (no transformations)
- Silver Layer: Cleansed and standardized data zone  
- Gold Layer: Analytics-ready data with business logic applied

WARNINGS AND PREREQUISITES:
- This script will create a new database called 'DataWarehouse'
- Ensure you have appropriate permissions to create databases
- Review and modify database name if conflicts exist
- Backup any existing databases with similar names before execution

================================================================================
*/

-- Switch to master database for database creation
USE master;
GO

-- Create the Data Warehouse database (keeping original name as specified)
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
END
GO

-- Switch to the newly created database
USE DataWarehouse;
GO

-- Create Bronze Schema (Raw Data Layer)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Bronze')
BEGIN
    EXEC('CREATE SCHEMA Bronze');
END
GO

-- Create Silver Schema (Cleansed & Standardized Data Layer)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver')
BEGIN
    EXEC('CREATE SCHEMA Silver');
END
GO

-- Create Gold Schema (Analytics & Business Logic Layer)  
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold')
BEGIN
    EXEC('CREATE SCHEMA Gold');
END
GO