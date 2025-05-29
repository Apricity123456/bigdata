from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# 使用脚本当前目录构造绝对路径，确保兼容 Airflow 的 tmp 工作目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_basics = os.path.join(BASE_DIR, "../data/raw/imdb/title.basics.tsv")
input_ratings = os.path.join(BASE_DIR, "../data/raw/imdb/title.ratings.tsv")
output_basics = os.path.join(BASE_DIR, "../data/formatted/imdb/basics.parquet")
output_ratings = os.path.join(BASE_DIR, "../data/formatted/imdb/ratings.parquet")

# 启动 Spark
spark = SparkSession.builder \
    .appName("Format IMDB Data") \
    .getOrCreate()

# 读取 IMDb 数据
df_basics = spark.read.csv(input_basics, sep="\t", header=True, nullValue="\\N")
df_ratings = spark.read.csv(input_ratings, sep="\t", header=True, nullValue="\\N")

# 选择有用字段并清洗
df_basics_clean = df_basics.select(
    "tconst", "titleType", "primaryTitle", "startYear", "genres"
).dropna()
df_basics_clean = df_basics_clean.withColumn("startYear", col("startYear").cast("int"))

df_ratings_clean = df_ratings.select(
    "tconst", "averageRating", "numVotes"
).dropna()
df_ratings_clean = df_ratings_clean.withColumn("averageRating", col("averageRating").cast("float")) \
                                   .withColumn("numVotes", col("numVotes").cast("int"))

# 保存为 Parquet
df_basics_clean.write.mode("overwrite").parquet(output_basics)
df_ratings_clean.write.mode("overwrite").parquet(output_ratings)

print(" IMDB data formatted and saved to Parquet.")

spark.stop()
