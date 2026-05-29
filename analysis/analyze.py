import polars as pl
import json
import sys
import os

json_path = "../data/products.json"

if not os.path.exists(json_path):
    print(f"Файл {json_path} не найден. Сначала запустите Go-сборщик.")
    sys.exit(1)

print("Загрузка данных из JSON...")

data = []
with open(json_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            data.append(json.loads(line))

df = pl.DataFrame(data)

print("\nПервые 5 строк:")
print(df.head(5))

print("\nИнформация о данных:")
print(f"Количество строк: {df.height}")
print(f"Количество столбцов: {df.width}")
print(f"Типы столбцов: {df.schema}")

print("\nПропуски в данных:")
print(df.null_count())