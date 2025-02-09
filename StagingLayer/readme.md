# Staging Layer - MySQL

This folder contains scripts and configurations for the **Staging Layer** of the Data Warehouse using MySQL.

## Overview
- Loads raw data from CSV files into a MySQL database.
- Uses Python (Pandas, SQLAlchemy) for data loading.
- SQL scripts to create staging tables.

## Files
- **`load_data.py`**: Python script to load CSV data into MySQL.
- **`schema.sql`**: SQL file to create staging tables.
- **`.env`**: Stores database credentials (not included in the repository for security).

## How to Use
1. Set up a MySQL database.
2. Update `.env` with your database credentials.
3. Run `schema.sql` to create tables.
4. Run `load_data.py` to load data.

## Dependencies
- Python 3
- Pandas
- SQLAlchemy
- MySQL Connector

## Notes
- Ensure MySQL is running before executing scripts.
- Large CSV files are processed in chunks to optimize memory usage.

---
**Author**: Salma 

