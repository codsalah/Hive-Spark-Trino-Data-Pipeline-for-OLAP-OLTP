-- ========================================
-- Top 10 Clubs by Transfer Count - Comparison Across File Formats
-- ========================================

-- ===== CSV File Query =====
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_csv t
JOIN club_dim_csv c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10;

-- ===== Parquet File Query =====
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_parquet t
JOIN club_dim_parquet c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10;

-- ===== Avro File Query =====
SELECT 
    c.name,   
    COUNT(t.transfer_id) AS total_transfers
FROM transfer_fact_avro t
JOIN club_dim_avro c ON t.to_club_id = c.club_id   
GROUP BY c.name 
ORDER BY total_transfers DESC 
LIMIT 10;
