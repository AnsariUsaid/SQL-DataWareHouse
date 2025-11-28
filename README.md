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
├── Scripts/
│   ├── 01_Database_Initialization.sql      # Creates database and schemas
│   ├── 02_Bronze_Layer_Tables.sql          # Creates all Bronze layer tables
│   └── 03_Bronze_Layer_Data_Load.sql       # Loads CSV data into Bronze tables
├── Datasets/
│   ├── source_crm/                         # CRM system CSV files
│   │   ├── cust_info.csv                   # Customer information
│   │   ├── prd_info.csv                    # Product information
│   │   └── sales_details.csv               # Sales transaction details
│   └── source_erp/                         # ERP system CSV files
│       ├── CUST_AZ12.csv                   # Customer demographics
│       ├── LOC_A101.csv                    # Customer location data
│       └── PX_CAT_G1V2.csv                 # Product categories
├── Docs/              # Documentation and architecture diagrams
├── Tests/             # Test scripts and validation queries
└── README.md          # This file
```

## Getting Started

### Prerequisites
- SQL Server 2019 or later (Docker container recommended)
- SQL Server Management Studio (SSMS) or Azure Data Studio
- Docker (if using containerized SQL Server)

### Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/AnsariUsaid/SQL-DataWareHouse.git
   cd SQL-DataWareHouse
   ```

2. **Setup SQL Server (Docker recommended)**
   ```bash
   docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourPassword123" \
      -p 1433:1433 --name sqlserver \
      -d mcr.microsoft.com/mssql/server:2022-latest
   ```

3. **Initialize the database**
   - Execute `Scripts/01_Database_Initialization.sql`
   - Creates database 'DataWareHous' and Bronze, Silver, Gold schemas

4. **Create Bronze layer tables**
   - Execute `Scripts/02_Bronze_Layer_Tables.sql`
   - Creates 6 tables for CRM and ERP source systems

5. **Load data into Bronze layer**
   - Copy CSV files to Docker container:
     ```bash
     docker cp Datasets/source_crm/cust_info.csv sqlserver:/var/opt/mssql/data/
     docker cp Datasets/source_crm/prd_info.csv sqlserver:/var/opt/mssql/data/
     docker cp Datasets/source_crm/sales_details.csv sqlserver:/var/opt/mssql/data/
     docker cp Datasets/source_erp/CUST_AZ12.csv sqlserver:/var/opt/mssql/data/
     docker cp Datasets/source_erp/LOC_A101.csv sqlserver:/var/opt/mssql/data/
     docker cp Datasets/source_erp/PX_CAT_G1V2.csv sqlserver:/var/opt/mssql/data/
     ```
   - Execute `Scripts/03_Bronze_Layer_Data_Load.sql`

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