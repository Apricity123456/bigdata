from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Index Recommendations to Elasticsearch") \
    .getOrCreate()

# 读取推荐结果
df = spark.read.parquet("data/combined/recommended_movies.parquet")
df = df.limit(200)  # 限制数量防止爆掉 Elasticsearch

# 转为 Pandas，便于批量上传
pdf = df.toPandas()

# 初始化 Elasticsearch 客户端
es = Elasticsearch("http://localhost:9200")

# 定义索引名称
index_name = "movies_recommendation"

# 删除旧索引（如果已存在）
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# 准备数据
actions = [
    {
        "_index": index_name,
        "_source": row.dropna().to_dict()
    }
    for _, row in pdf.iterrows()
]

# 批量写入
bulk(es, actions)

print(f"✅ Indexed {len(actions)} documents to {index_name}")
spark.stop()
