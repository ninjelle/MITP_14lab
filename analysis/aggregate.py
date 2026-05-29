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

print("Агрегация по рейтингу:")
result = df.group_by("rating").agg([
    pl.col("price").mean().alias("средняя_цена"),
    pl.col("price").min().alias("мин_цена"),
    pl.col("price").max().alias("макс_цена"),
    pl.len().alias("количество")
]).sort("rating")

print(result)