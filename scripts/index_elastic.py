from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import os

# 构造路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(BASE_DIR, "../data/combined/recommended_movies.parquet")
index_name = "movies_recommendation"

# 初始化 Spark
spark = SparkSession.builder \
    .appName("Index Recommendations to Elasticsearch") \
    .getOrCreate()

# 读取推荐结果
try:
    df = spark.read.parquet(input_path)
    df = df.limit(200)
    pdf = df.toPandas()
except Exception as e:
    print(" Failed to read parquet or convert to Pandas:", e)
    spark.stop()
    exit(1)

# 初始化 Elasticsearch
try:
    es = Elasticsearch("http://localhost:9200")

    # 删除旧索引
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
except Exception as e:
    print(" Elasticsearch connection/index error:", e)
    spark.stop()
    exit(1)

# 转换成 bulk actions
try:
    actions = [
        {
            "_index": index_name,
            "_source": row.dropna().to_dict()
        }
        for _, row in pdf.iterrows()
    ]
    bulk(es, actions)
    print(f" Indexed {len(actions)} documents to {index_name}")
except Exception as e:
    print(" Bulk indexing failed:", e)

spark.stop()
