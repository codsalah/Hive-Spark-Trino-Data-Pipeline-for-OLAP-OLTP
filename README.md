# TransferMarket Data Modeling Project

## Project Overview
This project involves modeling the TransferMarket dataset into a **Star Schema** and implementing the model on **HDFS** using **CSV**, **Avro**, and **Parquet** formats. The project also includes a comparison between these formats in terms of **size**, **write speed**, and **read speed**. Additionally, multiple compression levels and algorithms are tested for Avro and Parquet formats.

---

## Resources
- **Dataset**: [TransferMarket Dataset on Kaggle](https://www.kaggle.com/dataset-link)

---

## Data Schema 
### ERD Traditional schema
![image](https://github.com/user-attachments/assets/f7e2764f-9bfd-439b-bb3f-a578fd56f33b)

## Spark Data Exploration

### Description
The `spark data exploration` folder contains code for exploring and analyzing data using **Apache Spark**. The code is written in **Python** and utilizes the **PySpark** library to perform data transformations, filtering, and analytical queries.

   - Contains functions to analyze data related to **clubs**, **players**, and **transfers**.
   - Performs operations such as:
     - Filtering clubs by name.
     - Analyzing specific club metrics compared to others in the same domestic competition.
     - Exploring player data, including market value, squad size, and contract expiration.
     - Comparing two players in the same position.
     - Analyzing transfer data between clubs.

---

## Staging Database

### Description
The `staging database` folder contains scripts and files related to extracting data from source files (CSV) and loading it into a **staging database**. This is typically the first step in an ETL (Extract, Transform, Load) pipeline, where raw data is ingested into a database for further processing.

Key Files
1. **`Extract_from_Source_to_Staging.py`**:
   - A Python script that reads CSV files and loads the data into a MySQL database.
   - Uses **Pandas** for reading CSV files in chunks and **SQLAlchemy** for database connectivity.
   - Handles large datasets efficiently by processing them in chunks.

2. **DDL Files**:
   - Contains SQL scripts (`DDL`) for creating the database schema (tables, indexes, etc.) in the staging database.
   - These scripts define the structure of the tables where the raw data will be loaded.

---

## Spark Data Modeling

### Description
The `SparkDataModeling` folder contains scripts for transforming raw data into a **star schema** using **Apache Spark**. The star schema is a common data modeling technique used in data warehousing, consisting of **fact tables** and **dimension tables**. This folder includes scripts for creating dimension tables (`ClubDim`, `CompetitionDim`, `PlayerDim`, `TimeDim`) and a fact table (`TransferFact`).

1. **Dimension Tables**:
   - **`ClubDim.py`**: Transforms raw club data into the `ClubDim` dimension table.
   - **`CompetitionDim.py`**: Transforms raw competition data into the `CompetitionDim` dimension table.
   - **`PlayerDim.py`**: Transforms raw player data into the `PlayerDim` dimension table.
   - **`TimeDim.py`**: Extracts time-related data (e.g., transfer dates) and creates the `TimeDim` dimension table.

2. **Fact Table**:
   - **`TransferFact.py`**: Aggregates and transforms transfer data into the `TransferFact` fact table, linking it to the dimension tables.
  
     ![Image](https://github.com/user-attachments/assets/21b1959f-880a-41e8-8b72-3632f11a2498)


---

## CSVtoAvro

### Description
The `CSVtoAvro` folder contains a Python script that converts CSV files into **Avro** format. 

### Key File
1. **`csv_to_avro.py`**:
   - A Python script that reads CSV files, converts them into Avro format, and saves the output as `.avro` files.
   - Defines custom Avro schemas for each CSV file to ensure data integrity and compatibility.


---

## HiveDWH Exploration

### Description
The `HiveDWH Exploration` folder contains scripts for loading data into **HDFS** (Hadoop Distributed File System) and **Hive** .  

1. **`load_data_to_hdfs.sh`**:
   - A Bash script that uploads dimension and fact tables (in CSV, Parquet and Avro formats) from the local file system to HDFS.
   - Creates necessary directories in HDFS if they don't already exist.
   - Verifies the upload by checking the contents of the HDFS directories.

2. **`LoadDataToHive.hql`**:
   - An HQL script that creates Hive tables and loads data from HDFS into these tables.
   - Defines the schema for dimension and fact tables in Hive.
   - Supports loading data from **multiple formats (CSV, Parquet, Avro)**.

---












