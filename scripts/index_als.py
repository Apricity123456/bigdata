from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql.functions import explode
import os

# 构造路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(BASE_DIR, "../data/als/recommendations.parquet")
index_name = "als_recommendation"

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Index Flattened ALS Recommendations") \
    .getOrCreate()

# 读取推荐结果
df = spark.read.parquet(input_path)

# 展开结构
df_flat = df.selectExpr("user_id", "explode(recommendations) as rec") \
            .selectExpr("user_id", "rec.item_id as item_id", "rec.rating as rating")

df_flat = df_flat.limit(200)

# 转为 Pandas
pdf = df_flat.toPandas()
pdf = pdf.where(pdf.notnull(), None)

# 连接 Elasticsearch
es = Elasticsearch("http://localhost:9200")
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# 准备数据
actions = [
    {
        "_index": index_name,
        "_source": {
            "user_id": int(row["user_id"]),
            "item_id": int(row["item_id"]),
            "rating": float(row["rating"])
        }
    }
    for _, row in pdf.iterrows()
]

bulk(es, actions)
print(f" Indexed {len(actions)} ALS recommendations to {index_name}")
spark.stop()
