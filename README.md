# Microsoft SQL Server to PostgreSQL Data Piple with DLT

This project implements a data pipeline using the Data Load Tool (DLT) to
transfer data from a Microsoft SQL Server (MSSQL) database to a PostgreSQL
database. The pipeline includes features for incremental loading and setting
replica identity for efficient data synchronization.

## Features

- Extracts data from specified tables in an MSSQL database
- Loads data into a PostgreSQL database
- Supports incremental loading based on an `updated_at` column
- Sets Replica Identity for loaded tables in PostgreSQL
- Configurable through environment variables

## Why This Script is Different

This script goes beyond a simple data transfer by implementing several key
features:

1. Incremental Loading: It uses DLT's incremental loading capability, which
   allows for efficient updates by only transferring new or modified data based
   on the `updated_at` column.
2. Replica Identity Setting: Unlike standard ETL processes, this script sets the
   Replica Identity for each table in PostgreSQL after loading the data. This is
   crucial for several reasons:

   - It enables more efficient data synchronization in the future.
   - It allows for full UPDATE and DELETE operations when using logical
     replication.
   - It improves the performance of UPSERT operations in subsequent data loads.

3. Customizable Table Selection: The script allows you to easily specify which
   tables to extract and load, making it adaptable to various data transfer
   scenarios.
4. Error Handling and Logging: Robust error handling and logging mechanisms are
   built into the script, ensuring smooth operation and easy troubleshooting.

## Custom Replica Identity Setting

It's important to note that DLT (Data Load Tool) does not provide out-of-the-box
functionality for setting Replica Identity as a post-hook after table creation.
This is why our script includes a custom `set_replica_identity` function.

This custom implementation allows us to:

1. Check if the necessary index for Replica Identity exists
2. Create the index if it doesn't exist
3. Set the Replica Identity for each table after the initial data load

By adding this custom functionality, we enhance DLT's capabilities and ensure
that our PostgreSQL tables are properly configured for efficient data
synchronization and potential future replication needs.

## Why Set Replica Identity

Setting Replica Identity is a critical step when creating tables in PostgreSQL,
especially in the context of data replication and synchronization:

1. Data Integrity: It ensures that each row can be uniquely identified, which is
   essential for accurate updates and deletes.
2. Replication Support: It enables full support for logical replication,
   allowing UPDATE and DELETE operations to be properly replicated.
3. Efficient Updates: When using merge operations or UPSERT statements, having a
   proper Replica Identity allows PostgreSQL to efficiently locate and update
   existing rows.
4. Future-Proofing: Even if you're not using replication now, setting Replica
   Identity prepares your database for potential future use of replication or
   more complex data synchronization scenarios.

By including these features, this script provides a more robust and future-proof
solution for data transfer and synchronization between MSSQL and PostgreSQL
databases.

## Prerequisites

- Python 3.7+
- Access to source MSSQL database
- Access to destination PostgreSQL database
- Required Python packages: dlt[sql_database], pymysql, psycopg2
- Install package with requirements.txt

## Configuration

Set the following environment variables for the destination PostgreSQL database:

- `DESTINATION_POSTGRES_DATABASE`: Database name (default: "datalake_house")
- `DESTINATION_POSTGRES_USER`: Username (default: "myusername")
- `DESTINATION_POSTGRES_PASSWORD`: Password (default: "changeme")
- `DESTINATION_POSTGRES_HOST`: Host address (default: "myapp")
- `DESTINATION_POSTGRES_PORT`: Port number (default: "50432")

## Usage

1. Ensure all required environment variables are set.
2. Run the script:

```bash
python sql_database_pipeline.py
```

The script will:

1. Connect to the source MSSQL database Extract data from specified tables
2. Load data into the destination PostgreSQL database
3. Set replica identity for the loaded tables

### Customization

- Modify the `load_tables()` function to specify which tables to extract and
  load.
- Adjust the primary keys and incremental loading configuration for each table
  as needed.

### Error Handling

The script includes error handling for database connections and operations.

Check the console output for any error messages or warnings during execution.

#### License: MIT
