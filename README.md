<div align="center">
  <br>
  <h1>E-commerce Data Platform</h1>
</div>
<br>

This is an data engineering project created for learning. It demonstrates a modern data pipeline architecture using PySpark, Apache Airflow, and GKE.

### 🚀 Project Overview
This project simulates a typical data pipeline workflow:
1. **Extract & Load**: Raw data is stored in Google Cloud Storage (GCS) in the `raw` zone.
2. **Transform**: Data is lightly cleaned and transformed using **PySpark**, then stored back in GCS in the `transform` zone.
3. **Data Warehouse**: An **external table** is created in **BigQuery**, referencing data in GCS.
4. **Orchestration**: The entire pipeline is scheduled and orchestrated using **Apache Airflow**.
5. **Data Quality**: Data validations are handled with **Soda**.
6. **Visualization**: Insights are visualized from BigQuery using **Apache Superset**.
7. **Infrastructure**: All components are deployed on **Google Kubernetes Engine (GKE)** using **Helm**.