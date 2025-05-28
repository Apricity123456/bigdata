from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Format IMDB Data") \
    .getOrCreate()

# 输入路径
input_basics = "data/raw/imdb/title.basics.tsv"
input_ratings = "data/raw/imdb/title.ratings.tsv"

# 输出路径
output_basics = "data/formatted/imdb/basics.parquet"
output_ratings = "data/formatted/imdb/ratings.parquet"

# 读取 TSV 文件（用制表符）
df_basics = spark.read.csv(input_basics, sep="\t", header=True, nullValue="\\N")
df_ratings = spark.read.csv(input_ratings, sep="\t", header=True, nullValue="\\N")

# 选择有用字段
df_basics_clean = df_basics.select(
    "tconst", "titleType", "primaryTitle", "startYear", "genres"
).dropna()

df_ratings_clean = df_ratings.select(
    "tconst", "averageRating", "numVotes"
).dropna()

# 类型转换
df_basics_clean = df_basics_clean.withColumn("startYear", col("startYear").cast("int"))
df_ratings_clean = df_ratings_clean.withColumn("averageRating", col("averageRating").cast("float")) \
                                   .withColumn("numVotes", col("numVotes").cast("int"))

# 写入 Parquet
df_basics_clean.write.mode("overwrite").parquet(output_basics)
df_ratings_clean.write.mode("overwrite").parquet(output_ratings)

print("✅ IMDB data formatted and saved to Parquet.")

spark.stop()
