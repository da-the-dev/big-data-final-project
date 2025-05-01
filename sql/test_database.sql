SELECT COUNT(*) FROM traffic;

SELECT * FROM traffic LIMIT 10;

SELECT state, COUNT(*) as congestion_count 
FROM traffic 
GROUP BY state 
ORDER BY congestion_count DESC LIMIT 10;