# 2. Файл bronze_to_silver.py.
# - Зчитує таблицю bronze
# - Очистка тексту для всіх текстових колонок
# - Дедублікація рядків
# - Запис таблиці в папку silver/{table}, де {table} — ім’я таблиці.

import re
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import col, udf, when, lit

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text) if text is not None else "")


def process_numeric_range(value):
    if value is None:
        return None
    value = str(value).strip()

    # Регулярний вираз для пошуку діапазону (X-Y) або просто числа (X)
    range_match = re.fullmatch(r'(\d+(\.\d+)?)-(\d+(\.\d+)?)', value)
    number_match = re.fullmatch(r'(\d+(\.\d+)?)', value)

    if range_match:
        # Якщо це діапазон, беремо середнє
        try:
            low = float(range_match.group(1))
            high = float(range_match.group(3))
            return (low + high) / 2.0
        except ValueError:
            return None
    elif number_match:
        # Якщо це просто число
        try:
            return float(value)
        except ValueError:
            return None
    else:
        # Нечислові або неформатовані значення
        return None

clean_text_udf = udf(clean_text, StringType())
process_numeric_udf = udf(process_numeric_range, DoubleType())

spark = SparkSession.builder.appName("Bronze_to_Silver").getOrCreate()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    input_path = f"{os.getcwd()}/out_tables/bronze/{table}"
    df = spark.read.parquet(input_path)

    # 1. Загальна чистка текстових колонок
    text_columns = [field.name for field in df.schema.fields if
                    str(field.dataType) == "StringType" and field.name not in ["height", "weight"]]
    for col_name in text_columns:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))

    # 2. СПЕЦІАЛЬНА ОБРОБКА для athlete_bio
    if table == "athlete_bio":
        # Перетворюємо height/weight за допомогою UDF, яка обробляє діапазони
        df = df.withColumn("height", process_numeric_udf(F.col("height")))
        df = df.withColumn("weight", process_numeric_udf(F.col("weight")))

        # Додаткова чистка: видаляємо рядки, де height або weight є NULL (тобто не були числами/діапазонами)
        df = df.filter(F.col("height").isNotNull() & F.col("weight").isNotNull())

    # 3. Дедублікація та запис
    df = df.dropDuplicates()
    output_path = f"{os.getcwd()}/out_tables/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    df = spark.read.parquet(output_path)
    print(f"--- Processed Silver table: {table} ---")
    df.show(5, truncate=False)  # Змінив на show(5) для кращого контролю

spark.stop()