from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Format Netflix Data") \
    .getOrCreate()

# 输入输出路径
input_path = "data/raw/netflix/netflix_titles.csv"
output_path = "data/formatted/netflix/titles.parquet"

# 读取 CSV
df = spark.read.csv(input_path, header=True)

# 清洗字段：选出有用字段，去除空值
df_clean = df.select(
    "show_id", "title", "type", "release_year", "date_added", "rating", "duration", "listed_in"
).dropna(subset=["title", "release_year"])

# 格式化日期字段（加分项）
df_clean = df_clean.withColumn("release_year", col("release_year").cast("int")) \
                   .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))

# 保存为 Parquet
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Netflix data formatted and saved to Parquet.")

spark.stop()
