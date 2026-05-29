import polars as pl
import matplotlib.pyplot as plt
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

plt.rcParams['font.size'] = 12

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

rating_counts = df.group_by("rating").len().sort("rating")
axes[0].bar(rating_counts["rating"], rating_counts["len"], color='skyblue', edgecolor='black')
axes[0].set_xlabel("Рейтинг")
axes[0].set_ylabel("Количество книг")
axes[0].set_title("Распределение книг по рейтингу")
axes[0].set_xticks([1, 2, 3, 4, 5])

price_by_rating = df.group_by("rating").agg(pl.col("price").mean()).sort("rating")
axes[1].bar(price_by_rating["rating"], price_by_rating["price"], color='salmon', edgecolor='black')
axes[1].set_xlabel("Рейтинг")
axes[1].set_ylabel("Средняя цена (£)")
axes[1].set_title("Средняя цена книги по рейтингу")
axes[1].set_xticks([1, 2, 3, 4, 5])

plt.tight_layout()
plt.savefig("../data/charts.png", dpi=150)
plt.show()

print("Графики сохранены в ../data/charts.png")