from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
import os
import random
# 构造路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(BASE_DIR, "../data/formatted/netflix/titles.parquet")
output_path = os.path.join(BASE_DIR, "../data/als/recommendations.parquet")

# 初始化 Spark
spark = SparkSession.builder \
    .appName("ALS Recommendation Model") \
    .getOrCreate()

# 读取 Netflix 数据
df = spark.read.parquet(input_path)

# 添加模拟 user_id 与评分
df = df.withColumn("user_id", monotonically_increasing_id())
df = df.withColumn("rating", lit(1.0))

# 编码 show_id 为 item_id
indexer = StringIndexer(inputCol="show_id", outputCol="item_id")
df_indexed = indexer.fit(df).transform(df)

# 训练 ALS 模型
als = ALS(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    coldStartStrategy="drop",
    nonnegative=True
)
model = als.fit(df_indexed)

# 推荐 Top 10
# recommendations = model.recommendForAllUsers(10)
recommendations = model.recommendForAllUsers(random.randint(5, 15))

recommendations.write.mode("overwrite").parquet(output_path)
print(f" ALS 推荐完成并已保存到 {output_path}")

spark.stop()
