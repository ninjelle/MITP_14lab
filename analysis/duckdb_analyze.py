import duckdb
import time

start = time.time()

conn = duckdb.connect()

result = conn.execute("""
    SELECT 
        rating,
        COUNT(*) as count,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price
    FROM '../data/products.parquet'
    GROUP BY rating
    ORDER BY rating
""").fetchdf()

end = time.time()

print(result)
print(f"\nВремя выполнения: {end - start:.4f} секунд")