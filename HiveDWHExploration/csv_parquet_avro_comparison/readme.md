# CSV, Parquet, and Avro Comparison Queries

## Queries

### 1. Top 10 Clubs by Transfer Spending  

#### CSV Query:
```sql
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_csv
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;
```
![CSV Spending](https://github.com/user-attachments/assets/ff2bac2c-a95d-4132-82a1-ba2f111dc687)

#### Avro Query:
```sql
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_avro
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;
```
![Avro Spending](https://github.com/user-attachments/assets/8e3ad522-2a98-41cf-bdd6-70ec5397d138)

#### Parquet Query:
```sql
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_parquet
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;
```
![Parquet Spending](https://github.com/user-attachments/assets/f7f09b5e-25c9-4090-a108-052243c32bad)

---

### 2. Top 10 Clubs by Transfer Count  

#### CSV Query:
```sql
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_csv t
JOIN club_dim_csv c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10; 
```
![CSV Transfers](https://github.com/user-attachments/assets/f865d5d4-cac2-437d-9cfc-6b7f12ff0797)

#### Parquet Query:
```sql
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_parquet t
JOIN club_dim_parquet c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10; 
```
![Parquet Transfers](https://github.com/user-attachments/assets/f90cca51-343e-4d1e-a18b-b24ccf85c002)

#### Avro Query:
```sql
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_avro t
JOIN club_dim_avro c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10; 
```
![Avro Transfers](https://github.com/user-attachments/assets/6a87307a-ae19-47f4-9e7d-ab4d64a2e335)

---

## Comparison & Insights

### **Performance & Storage Differences**
- **CSV**: Easy to read and use but inefficient for large datasets due to lack of indexing and compression.
- **Parquet**: Columnar format, optimized for analytical queries, faster read times, and lower storage usage.
- **Avro**: Row-based format, better for data serialization and interoperability but less efficient for large-scale analytical queries.

### **Key Takeaways**
- **Parquet** is the best choice for analytical queries due to its efficient storage and read speeds.
- **Avro** is useful for data exchange and schema evolution.
- **CSV** remains a simple, widely compatible format but is not optimal for large datasets in analytical workflows.


