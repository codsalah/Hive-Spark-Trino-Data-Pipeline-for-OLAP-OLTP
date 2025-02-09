# CSV to Avro Conversion

## Overview
This project converts CSV files into Avro format using **Pandas** and **FastAvro**.

## Files
- **clubs_dim.csv** → **clubs_dim.avro**
- **competitions_dim.csv** → **competitions_dim.avro**
- **players_dim.csv** → **players_dim.avro**
- **time_dim.csv** → **time_dim.avro**
- **TransferFact.csv** → **transfers_fact.avro**

## Dependencies
Ensure you have the required Python libraries installed:
```bash
pip install pandas fastavro numpy
```

## How It Works
1. Reads CSV files using **Pandas**.
2. Replaces `NaN` values with `None` to maintain Avro compatibility.
3. Converts each CSV row into a dictionary.
4. Writes data into Avro format using **FastAvro**.

## Running the Script
Execute the Python script to convert all CSV files:
```bash
python convert_csv_to_avro.py
```

## Notes
- The Avro schema for each file is predefined in the script.
- The script handles missing values to ensure data integrity.
- `country_name` is replaced with an empty string (`''`) when missing.

## Output
Avro files will be generated in the same directory as the script.

---
**Author**: Salma
