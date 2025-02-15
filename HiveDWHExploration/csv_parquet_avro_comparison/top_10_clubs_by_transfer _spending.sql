-- ========================================
-- Top 10 Club Spending on Transfers - Comparison Across File Formats
-- ========================================

-- ===== CSV File Query =====
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_csv
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;

-- ===== Parquet File Query =====
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_parquet
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;

-- ===== Avro File Query =====
SELECT to_club_id, SUM(transfer_fee) AS total_spending
FROM transfer_fact_avro
GROUP BY to_club_id
ORDER BY total_spending DESC
LIMIT 10;
