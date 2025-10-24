[Event Sources]
  POS systems, web clicks, mobile apps, IoT sensors
       │
       ▼
[Kafka Cluster (topics: transactions, events)]
       │
       ▼
[Spark Structured Streaming]  <-- running on Databricks or Spark cluster
  • read from Kafka topics
  • transformations, enrichments
  • write to Delta Lake (raw & curated)
  • write to ML scoring endpoint / serving
       │
       ▼
[Delta Lake (Data Lakehouse)]
  • raw_transactions (append)
  • curated_transactions (cleaned/aggregated/partitioned)
  • feature_store (for ML)
       │
       ▼
[Databricks Notebooks / MLflow]
  • feature engineering, training, hyperparam tuning
  • register model in MLflow Model Registry
       │
       ▼
[Model Serving / REST API]
  • MLflow Serve / Databricks Model Serving or REST microservice
       │
       ▼
[Power BI]
  • Connects to Databricks SQL endpoint or Delta Lake
  • Real-time dashboard + alerts

## Databricks workspace configuration

This repository contains example streaming code that is intended to run on Databricks. To configure and verify a Databricks workspace using the Python SDK, follow these steps:

1. Create a Databricks personal access token (PAT) from the Databricks UI: User Settings -> Access Tokens.
2. Set the following environment variables on your machine or CI runner:

   - `DATABRICKS_HOST` e.g. `https://<your-workspace>.azuredatabricks.net` or `https://<region>.cloud.databricks.com`
   - `DATABRICKS_TOKEN` your PAT

3. Install dependencies (preferably in a virtualenv):

```powershell
python -m venv .venv; .venv\Scripts\Activate; pip install -r requirements.txt
```

4. Run the verification script which lists clusters in the workspace:

```powershell
python databricks_config.py
```

Notes:
- The script will not create resources unless you explicitly change the code to do so. It is intentionally read-only and safe.
- If you prefer the Databricks CLI, see the Databricks docs: https://docs.databricks.com/dev-tools/cli/index.html

./bin/kafka-server-start.sh ./config/server.properties 
---------------------
