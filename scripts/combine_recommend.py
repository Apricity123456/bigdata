from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log, lower, trim
import os

# 构造路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
imdb_basics_path = os.path.join(BASE_DIR, "../data/formatted/imdb/basics.parquet")
imdb_ratings_path = os.path.join(BASE_DIR, "../data/formatted/imdb/ratings.parquet")
netflix_path = os.path.join(BASE_DIR, "../data/formatted/netflix/titles.parquet")
output_path = os.path.join(BASE_DIR, "../data/combined/recommended_movies.parquet")

# 启动 Spark
spark = SparkSession.builder \
    .appName("Combine IMDB + Netflix for Recommendation") \
    .getOrCreate()

# 读取数据
df_basics = spark.read.parquet(imdb_basics_path)
df_ratings = spark.read.parquet(imdb_ratings_path)
df_netflix = spark.read.parquet(netflix_path)

# Join basics 和 ratings
df_imdb = df_basics.join(df_ratings, on="tconst", how="inner")

# 对标题做小写+去空格处理
df_netflix_clean = df_netflix.withColumn("title_clean", trim(lower(col("title"))))
df_imdb_clean = df_imdb.withColumn("primaryTitle_clean", trim(lower(col("primaryTitle"))))

# Join 两边数据
df_joined = df_imdb_clean.join(
    df_netflix_clean,
    df_imdb_clean.primaryTitle_clean == df_netflix_clean.title_clean,
    how="inner"
)

# 计算推荐分数
df_result = df_joined.withColumn(
    "recommend_score",
    col("averageRating") + log(col("numVotes") + 1)
)

# 选择字段 + 排序
df_final = df_result.select(
    "title", "averageRating", "numVotes", "recommend_score",
    "release_year", "rating", "duration", "genres", "listed_in"
).orderBy(col("recommend_score").desc())

# 写入 Parquet
df_final.write.mode("overwrite").parquet(output_path)

print("✅ Recommendation completed and saved to:", output_path)
spark.stop()
