from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

# 动态构造路径，确保在 Airflow 中不出错
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(BASE_DIR, "../data/raw/netflix/netflix_titles.csv")
output_path = os.path.join(BASE_DIR, "../data/formatted/netflix/titles.parquet")

# 启动 Spark
spark = SparkSession.builder \
    .appName("Format Netflix Data") \
    .getOrCreate()

# 读取 CSV
df = spark.read.csv(input_path, header=True)

# 清洗字段
df_clean = df.select(
    "show_id", "title", "type", "release_year", "date_added", "rating", "duration", "listed_in"
).dropna(subset=["title", "release_year"])

# 转换字段类型
df_clean = df_clean.withColumn("release_year", col("release_year").cast("int")) \
                   .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

# 保存为 Parquet
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Netflix data formatted and saved to Parquet.")

spark.stop()
