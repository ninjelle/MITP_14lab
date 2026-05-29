import polars as pl
import json
import os

json_path = "../data/products.json"

if not os.path.exists(json_path):
    print(f"Файл {json_path} не найден.")
    exit(1)

data = []
with open(json_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            data.append(json.loads(line))

df = pl.DataFrame(data)

df = df.unique()

df = df.with_columns(
    pl.col("price").str.replace_all("£", "").str.replace_all(",", "").cast(pl.Float64)
)

rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
df = df.with_columns(
    pl.col("rating").replace_strict(rating_map)
)

parquet_path = "../data/products.parquet"
df.write_parquet(parquet_path)

print(f"Данные сохранены в {parquet_path}")
print(f"Размер DataFrame: {df.height} строк, {df.width} столбцов")

df_verify = pl.read_parquet(parquet_path)
print(f"\nПроверка загрузки Parquet:")
print(df_verify.head(3))