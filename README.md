#  Big Data Recommendation Project - IMDb & Netflix (ENGLISH VERSION)

##  Project Overview

This project implements a complete end-to-end data pipeline using Big Data technologies. It covers everything from data ingestion, cleaning, formatting, transformation (via Spark), machine learning recommendation, and indexing results into Elasticsearch for dashboard visualization using Kibana.

The domain is focused on movies, with cross-source analysis of IMDb and Netflix data to generate valuable movie recommendations.

---

##  Data Pipeline Structure

### 1. Data Ingestion

* **IMDb**: Download `title.basics.tsv.gz` and `title.ratings.tsv.gz` from IMDb official dataset site.
* **Netflix**: Download `netflix_titles.csv` from Kaggle using API.
* Scripts: [`ingest_imdb_api.py`](./scripts/ingest_imdb_api.py), [`ingest_netflix_api.py`](./scripts/ingest_netflix_api.py)

### 2. Data Formatting

* Convert CSV/TSV to Parquet for efficient processing
* Standardize date fields, type conversion, missing values
* Scripts: [`format_imdb.py`](./scripts/format_imdb.py), [`format_netflix.py`](./scripts/format_netflix.py)

### 3. Data Combination

* Join IMDb (titles + ratings) with Netflix titles via lowercase title match
* Compute recommendation score: `recommend_score = averageRating + log(numVotes + 1)`
* Script: [`combine_recommend.py`](./scripts/combine_recommend.py)

### 4. ALS-Based Recommendation

* Use `pyspark.ml.recommendation.ALS` for collaborative filtering
* Simulate user-item interactions with generated IDs and constant rating
* Script: [`als_recommend.py`](./scripts/als_recommend.py)

### 5. Elasticsearch Indexing + Kibana Dashboard

* Index both heuristic and ALS-based results to Elasticsearch
* Visualize key metrics on Kibana dashboard
* Scripts: [`index_elastic.py`](./scripts/index_elastic.py), [`index_als.py`](./scripts/index_als.py)

### 6. Full Workflow with Airflow DAG

* Use 3 DAGs to manage separate or dual recommendation pipelines
* DAGs: [`airflow_dag.py`](./dags/airflow_dag.py), [`als_pipeline_dag.py`](./dags/als_pipeline_dag.py), [`dual_recommend_dag.py`](./dags/dual_recommend_dag.py)

---

##  Project Structure

```bash
bigdata/
├── data/
│   ├── raw/                # Original source files
│   ├── formatted/          # Cleaned parquet files
│   ├── combined/           # Final recommendation output
│   └── als/                # ALS model results
├── scripts/                # Python scripts
├── dags/                   # Airflow DAG files
├── dashboards/             # Optional Kibana visual configs
├── requirements.txt
└── README.md
```

---

##  Environment Setup

### Python Dependencies (requirements.txt)

```txt
python-dotenv>=0.21.0
requests>=2.28.1
pandas>=1.5.3
numpy>=1.23.5
pyspark>=3.3.2
elasticsearch>=7.17.10
apache-airflow==2.7.3
apache-airflow-providers-apache-spark==4.0.1
apache-airflow-providers-elasticsearch==4.2.1
apache-airflow-providers-cncf-kubernetes>=6.0.0
kaggle>=1.5.16
jupyterlab>=3.6.3
ipykernel>=6.19.4
```

### System Dependencies

* Python >= 3.8
* Java 11 (required by Spark)
* Apache Spark 3.3+
* Elasticsearch 7.x (localhost:9200)
* Apache Airflow 2.x
* (Optional) Kibana

---

##  Project Highlights

* Cross-source data integration (IMDb + Netflix)
* Clean data lake structure (raw / formatted / combined)
* Machine learning with ALS collaborative filtering
* Airflow orchestration
* Elasticsearch + Kibana dashboard integration

---

##  How to Run

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start Elasticsearch
elasticsearch &  # Or via Docker

# Launch Airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow standalone

# Manually trigger DAG
airflow dags trigger dual_recommend_pipeline
```

---

##  Useful Links

* [IMDb Datasets](https://datasets.imdbws.com/)
* [Kaggle Netflix Dataset](https://www.kaggle.com/shivamb/netflix-shows)
* [Elasticsearch Python Client](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
* [Apache Spark](https://spark.apache.org/docs/latest/)
* [Apache Airflow](https://airflow.apache.org/docs/)

---

##  Submission Checklist

* `README.md`: Full documentation in Chinese and English
* `requirements.txt`: Full dependency list
* `video.mp4`: Demonstration (<10 min)
* `project.zip`: Source code archive



#  Big Data Recommendation Project - IMDb & Netflix (CHINESE VERSION)

##  项目概要

该项目实现了一个基于大数据技术的完整线性数据流程，从数据采集，数据清洗与格式化，到 Spark 统计/推荐算法处理，最终通过 Elasticsearch 实现可视化指标分析。

该项目以影视为主题，选取 IMDB 和 Netflix 两类影视数据源，展示如何通过跨源分析和机器学习算法实现实际价值的推荐系统。

---

##  数据流程模型

### 1. 数据采集 (Ingestion)

* **IMDb**: 通过官方提供的 `.tsv.gz` API 下载 basics 和 ratings
* **Netflix**: 通过 Kaggle API 下载 `netflix_titles.csv`
* 脚本：[`ingest_imdb_api.py`](./scripts/ingest_imdb_api.py), [`ingest_netflix_api.py`](./scripts/ingest_netflix_api.py)

### 2. 数据格式化 (Formatting)

* 将原始 CSV/TSV 数据格式化为 Parquet
* 标准化日期、数值类型、处理空值
* 脚本：[`format_imdb.py`](./scripts/format_imdb.py), [`format_netflix.py`](./scripts/format_netflix.py)

### 3. 数据组合 (Combination)

* 利用 Spark 将 IMDb 标题 + 评分与 Netflix 的视频标题 join
* 计算素数：`recommend_score = averageRating + log(numVotes + 1)`
* 脚本：[`combine_recommend.py`](./scripts/combine_recommend.py)

### 4. ALS 算法推荐

* 使用 `pyspark.ml.recommendation.ALS` 基于 Netflix title 模拟用户行为
* 随机生成 user\_id 和评分，进行返回 top N 推荐
* 脚本：[`als_recommend.py`](./scripts/als_recommend.py)

### 5. Elasticsearch 索引 + Kibana 可视化

* 将两种推荐结果分别索引到 Elasticsearch
* Kibana 中构建推荐统计值相关的 Dashboard
* 脚本：[`index_elastic.py`](./scripts/index_elastic.py), [`index_als.py`](./scripts/index_als.py)

### 6. 全流程引擎: Airflow DAG

* 通过 3 个 DAG 分别控制 Heuristic / ALS / 双流程
* 文件：[`airflow_dag.py`](./dags/airflow_dag.py), [`als_pipeline_dag.py`](./dags/als_pipeline_dag.py), [`dual_recommend_dag.py`](./dags/dual_recommend_dag.py)

---

##  文件结构

```bash
bigdata/
├── data/
│   ├── raw/                # 原始数据源 (IMDb / Netflix)
│   ├── formatted/          # 清洗后格式化 Parquet 文件
│   ├── combined/           # 组合推荐结果
│   └── als/                # ALS 推荐结果
├── scripts/                # Python 脚本
├── dags/                   # Airflow DAG
├── dashboards/             # Kibana 配置 (JSON 或截图)
├── requirements.txt
└── README.md
```

---

##  环境配置

### Python 依赖 (requirements.txt):

```txt
python-dotenv>=0.21.0
requests>=2.28.1
pandas>=1.5.3
numpy>=1.23.5
pyspark>=3.3.2
elasticsearch>=7.17.10
apache-airflow==2.7.3
apache-airflow-providers-apache-spark==4.0.1
apache-airflow-providers-elasticsearch==4.2.1
apache-airflow-providers-cncf-kubernetes>=6.0.0
kaggle>=1.5.16
jupyterlab>=3.6.3
ipykernel>=6.19.4
```

### 系统依赖:

* Python >= 3.8
* Java 11 (用于 Spark 运行)
* Apache Spark 3.3+
* Elasticsearch 7.x (default: `localhost:9200`)
* Airflow 2.x (LocalExecutor)
* Kibana (optional)

---

##  项目特色点

* 跨源数据组合: IMDb 分数 + Netflix 内容 join 生成实际价值
* 环境分层明确: raw/formatted/combined 等数据清清楚楚
* ALS 算法推荐模型: Spark MLlib 实现用户行为应用
* Airflow DAG 实现全链连数据运行
* Elasticsearch + Kibana 结果可视化

---

##  如何运行

```bash
# 配置虚拟环境 (Python >= 3.8)
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 启动 Elasticsearch
elasticsearch &  # 或 Docker 启动

# 运行 Airflow DAG
export AIRFLOW_HOME=~/airflow
airflow db init
airflow standalone

# 手动执行 DAG
airflow dags trigger dual_recommend_pipeline
```

---

##  链接参考

* [IMDb Datasets](https://datasets.imdbws.com/)
* [Kaggle Netflix Dataset](https://www.kaggle.com/shivamb/netflix-shows)
* [Elasticsearch Python Client](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)
* [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
* [Apache Airflow Documentation](https://airflow.apache.org/docs/)

---

##  项目提交内容

* `README.md`: 详细项目说明
* `requirements.txt`: 完整环境依赖
* `video.mp4`: 运行视频解说 (<10min)
* `project.zip`: 代码打包文件
