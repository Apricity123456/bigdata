from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log

spark = SparkSession.builder \
    .appName("Combine IMDB + Netflix for Recommendation") \
    .getOrCreate()

# 路径
imdb_basics_path = "data/formatted/imdb/basics.parquet"
imdb_ratings_path = "data/formatted/imdb/ratings.parquet"
netflix_path = "data/formatted/netflix/titles.parquet"
output_path = "data/combined/recommended_movies.parquet"

# 读取数据
df_basics = spark.read.parquet(imdb_basics_path)
df_ratings = spark.read.parquet(imdb_ratings_path)
df_netflix = spark.read.parquet(netflix_path)

# Join IMDB basics 和 ratings
df_imdb = df_basics.join(df_ratings, on="tconst", how="inner")

# 使用主标题与 Netflix 电影标题进行模糊匹配（简化：lower + trim 后 inner join）
df_netflix_clean = df_netflix.withColumn("title_clean", col("title").alias("title")) \
    .withColumn("title_clean", col("title_clean").cast("string"))

df_imdb_clean = df_imdb.withColumn("primaryTitle_clean", col("primaryTitle").cast("string"))

df_joined = df_imdb_clean.join(
    df_netflix_clean,
    df_imdb_clean.primaryTitle_clean == df_netflix_clean.title_clean,
    how="inner"
)

# 推荐分数：IMDB 评分 + log(投票数)
df_result = df_joined.withColumn(
    "recommend_score",
    col("averageRating") + log(col("numVotes") + 1)
)

# 选择字段
df_final = df_result.select(
    "title", "averageRating", "numVotes", "recommend_score",
    "release_year", "rating", "duration", "genres", "listed_in"
).orderBy(col("recommend_score").desc())

# 保存结果
df_final.write.mode("overwrite").parquet(output_path)

print("✅ Recommendation completed and saved to:", output_path)
spark.stop()
