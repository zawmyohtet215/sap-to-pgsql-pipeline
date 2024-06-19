# sap-to-pgsql-pipeline

### Project Description: Data Pipeline from SAP to PostgreSQL

#### Overview
This project aims to establish a data pipeline that extracts data from an SAP database, processes it, and loads it into a PostgreSQL database. The pipeline ensures that data is transformed appropriately and stored in a structured manner for analysis and reporting purposes.

#### Technologies Used
- **Python**: Core language for scripting the pipeline.
- **Pandas**: Data manipulation and analysis.
- **SQLAlchemy**: Database connection and ORM for SAP.
- **psycopg2**: PostgreSQL adapter for Python.
- **SAP HANA**: Source database.
- **PostgreSQL**: Target database.

#### Project Structure
The project is divided into the following main sections:

1. **Database Connection Functions**
   - **SAP Connection**: Establishes a connection to the SAP database.
   - **PostgreSQL Connection**: Establishes a connection to the PostgreSQL database.

2. **Extract Data from SAP**
   - **Extract Function**: Executes SQL queries to extract data from SAP and saves the results as CSV files.
   - **Staging File Paths**: Defines file paths for storing extracted CSV files.
   - **SQL Queries**: Defines SQL queries for extracting data from SAP.
   - **Run Extraction**: Executes the extraction queries and stores the results in specified paths.

3. **Load Data into PostgreSQL**
   - **Insert Dimension Tables**: Inserts data from CSV files into PostgreSQL dimension tables.
   - **Insert Fact Table**: Inserts data into PostgreSQL fact table with mechanisms to handle duplicates.
   - **Composite Key Creation**: Creates a composite key column to detect and handle duplicate rows in fact data.
   - **Load Data**: Executes the full load process for both dimension and fact tables.

#### Detailed Steps

1. **Database Connection Functions**
   - `connect_to_sap()`: Reads SAP connection parameters from a credentials file, establishes a connection using SQLAlchemy, and handles any connection errors.
   - `connect_to_pgsql()`: Reads PostgreSQL connection parameters from a credentials file and establishes a connection using psycopg2, handling any errors during the process.

2. **Extract Data from SAP**
   - **Function `extract_from_sap()`**:
     - Takes connection details, SQL query file, output folder, and output filename as parameters.
     - Reads the SQL query, executes it against the SAP database, and saves the results to a CSV file.
     - Handles specific data transformation issues, such as filling missing values and renaming columns.

   - **Staging File Paths**: Defines paths where extracted CSV files are stored, segmented by dimension and fact data.

   - **SQL Queries**: Lists SQL query filenames used for extracting various tables from SAP, including both dimension (e.g., Agent, Branch) and fact tables (e.g., AR Current Month, AR Last Month).

   - **Run Extraction**: Establishes the SAP connection and sequentially runs the extraction functions for all defined queries, storing results in the specified paths.

3. **Load Data into PostgreSQL**
   - **Insert Dimension Tables**:
     - `insert_dim_into_pgsql()`: Deletes old data from PostgreSQL dimension tables and inserts new data from corresponding CSV files.
     - Uses `COPY` command for efficient data loading and updates a timestamp for tracking.

   - **Insert Fact Table**:
     - `insert_fact_into_pgsql()`: Creates a temporary table to handle new fact data, avoiding duplicate entries.
     - Uses a composite key column to detect and manage duplicate rows before inserting data into the main fact table.

   - **Composite Key Creation**:
     - `combine_columns()`: Function to combine multiple columns into a single key for duplicate detection.
     - `add_key()`: Adds the composite key column to the CSV files before loading them into PostgreSQL.

   - **Load Data**: Executes the data loading process for dimension tables first, followed by the fact table. The process includes conditional checks based on the current date to manage monthly data appropriately.

#### Execution Flow
1. Establish connections to both SAP and PostgreSQL databases.
2. Extract data from SAP using predefined SQL queries and save them as CSV files.
3. Load extracted CSV files into PostgreSQL, ensuring data integrity and handling duplicates.
4. Update dimension tables and fact table in PostgreSQL as per the schedule and data freshness requirements.

#### Conclusion
This data pipeline automates the process of extracting, transforming, and loading (ETL) data from SAP to PostgreSQL. It ensures data accuracy, handles potential data issues, and supports efficient data integration for downstream analysis and reporting.
