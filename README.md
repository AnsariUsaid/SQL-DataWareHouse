# Modern Data Warehouse - SQL Server Implementation

A comprehensive data warehouse project implementing the medallion architecture pattern with Bronze, Silver, and Gold layers using SQL Server.

## Architecture Overview

This project implements a modern enterprise data warehouse architecture with three distinct layers:

### 1. Source Systems Layer
- **ERP System**: CSV file-based data source
- **CRM System**: CSV file-based data source

### 2. Data Warehouse Layer (SQL Server)

#### A. Bronze Layer (Raw Zone)
- **Object Type**: Tables
- **Load**: Batch Processing, Full Load, Truncate & Insert
- **Transformations**: None (Raw Data, No Transformations)
- **Data Model**: None (As-Is)

#### B. Silver Layer (Cleansed & Standardized Zone)
- **Object Type**: Tables
- **Load**: Batch Processing, Full Load, Truncate & Insert
- **Transformations**:
  - Data Cleansing
  - Data Standardization
  - Data Normalization
  - Derived Columns
  - Data Enrichment
- **Data Model**: None (As-Is)

#### C. Gold Layer (Analytics Layer)
- **Object Type**: Views
- **Load**: No Load (Logical Layer Only)
- **Transformations**:
  - Data Integration
  - Aggregations
  - Business Logic
- **Data Model**:
  - Star Schema
  - Flat Tables
  - Aggregated Tables

### 3. Consumption Layer
- **Ad-Hoc Analysis**: Interactive data exploration
- **BI & Dashboards**: Business intelligence and reporting
- **Machine Learning Models**: Advanced analytics and ML workloads

## Data Flow

```
CSV Sources → Bronze Layer → Silver Layer → Gold Layer → Consumption Layer
```

The data flows left to right through the architecture:
1. Source systems (ERP/CRM CSV files) feed into Bronze
2. Bronze data is cleansed and standardized in Silver
3. Silver data is transformed with business logic in Gold
4. Gold layer serves BI, ML, and Ad-hoc analysis needs

## Project Structure

```
SQL-DataWareHouse-Project/
├── Scripts/           # SQL scripts for database objects
├── Datasets/          # Source CSV files and sample data
├── Docs/              # Documentation and architecture diagrams
├── Tests/             # Test scripts and validation queries
└── README.md          # This file
```

## Getting Started

### Prerequisites
- SQL Server 2019 or later
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Appropriate database creation permissions

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/AnsariUsaid/SQL-DataWareHouse.git
   cd SQL-DataWareHouse
   ```

2. **Initialize the database**
   - Open `Scripts/01_Database_Initialization.sql` in SSMS
   - Review and execute the script to create the database and schemas

3. **Load source data**
   - Place your CSV files in the `Datasets/` directory
   - Execute data loading scripts (to be developed)

4. **Create Bronze layer tables**
   - Execute Bronze layer table creation scripts
   - Load raw data from CSV sources

5. **Build Silver layer transformations**
   - Execute Silver layer table creation scripts
   - Implement data cleansing and standardization logic

6. **Develop Gold layer views**
   - Create analytical views with business logic
   - Implement star schema and aggregated tables

## Design Principles

- **Medallion Architecture**: Progressive data refinement through Bronze → Silver → Gold
- **Scalability**: Designed to handle enterprise-scale data volumes
- **Maintainability**: Clear separation of concerns across layers
- **Performance**: Optimized for analytical workloads
- **Data Quality**: Built-in data validation and cleansing processes

## Layer Responsibilities

### Bronze Layer
- Raw data ingestion with no transformations
- Maintains data lineage and audit trail
- Supports data recovery and reprocessing

### Silver Layer
- Data quality improvements and standardization
- Business rule enforcement
- Data type conversions and formatting

### Gold Layer
- Business-ready datasets for analytics
- Optimized for query performance
- Implements dimensional modeling concepts

## Contributing

1. Follow the established naming conventions
2. Document all schema changes
3. Test scripts before committing
4. Update this README for architectural changes

## License

This project is for educational and demonstration purposes.

---

**Note**: This is an active development project. The architecture and implementation details may evolve as the project progresses.