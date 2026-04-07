# databricks-lakeflow-banking-dlt-pyspark
Automated Banking ETL Pipeline using Databricks Lakeflow (DLT) and Python. Features incremental ingestion via Auto Loader, data quality enforcement (Expectations), and SCD Type 1 &amp; 2 for historical banking records.

# Automated Lakeflow Declarative Pipeline using DLT & Python (PySpark)

## 📌 Overview

I have created this project from scratch and published it on my YouTube Channel : www.youtube.com/@DataToCrunch .

<img width="1647" height="923" alt="image" src="https://github.com/user-attachments/assets/041d9bdb-912b-4973-a48c-a5f4645d5f9e" />

This project demonstrates a production-ready, automated data engineering pipeline built on the **Databricks Lakehouse** platform. It utilizes **Lakeflow Declarative Pipelines (Delta Live Tables)** to process banking data through a structured Medallion Architecture.

The pipeline automates ingestion, rigorous data cleaning via Expectations, and complex historical tracking using **SCD Type 1 and Type 2** logic.

---

## 🏗️ Architecture

Unity Catalog Volumes → Auto Loader → DLT Bronze → DLT Silver → DLT Gold → delta tables(streaming tables, materialized views, views) → Databricks SQL Dashboards → Databricks Jobs/ Workflows

<img width="1564" height="872" alt="image" src="https://github.com/user-attachments/assets/060ac933-07c9-471f-b318-98283518b943" />

---

## ⚙️ Tech Stack

- **Databricks Lakeflow Declarative Pipeline & Workflows**
- **Delta Live Tables (DLT)**
- **Python (PySpark)**
- **Auto Loader** (`cloudFiles`)
- **Unity Catalog** (Volumes,Delta Tables & Governance)
- **Delta Lake**

---

## 🔄 Pipeline Flow

### 🔹 Data Source
- **Dataset:** Banking Customers and Account Transactions.
- **Ingestion:** **Auto Loader** used to incrementally ingest CSV files from Unity Catalog Volumes with schema enforcement.

### 🔹 Bronze Layer (Cleaning)
- **Data Standardization:** Normalizes strings to uppercase/lowercase and maps gender/status codes to descriptive labels.
- **Quality Gates:** Implements `@dlt.expect_or_fail` for critical IDs and `@dlt.expect_or_drop` for email regex and phone validation.
- **Constraint Filtering:** Ensures only valid transaction channels and income ranges enter the pipeline.

### 🔹 Silver Layer (Transformations)
- **Feature Engineering:** Calculates `customer_age` and `tenure_days`.
- **SCD Type 1:** Uses `dlt.apply_changes` for Customers to maintain the latest profile state.
- **SCD Type 2:** Uses `dlt.create_auto_cdc_flow` for Transactions to track full historical versioning.
- **Categorization:** Adds `channel_type` (Physical/Digital) and `txn_direction` (In/Out) flags.

### 🔹 Gold Layer (Analytics)
- **Materialized Views:** Joins customers and transactions into a unified wide table.
- **Business Aggregations:** Aggregates totals for credits, debits, average transaction amounts, and customer tenure.

<img width="2879" height="1463" alt="image" src="https://github.com/user-attachments/assets/d4fdda6f-6391-42f8-bf46-118de83526e8" />

### 🔹 Visualization
- **Databricks SQL Dashboards** powered by the Gold Layer for real-time banking insights.
  
<img width="2879" height="1464" alt="image" src="https://github.com/user-attachments/assets/8dd98c7e-d997-4943-9c08-59613332ac4b" />


### 🔹 Databricks Jobs & Workflows
- **Orchestration:** The entire DLT pipeline is wrapped in a **Databricks Workflow Job**.
- **Automation:** Configured for scheduled execution, ensuring that as new `.csv` files land in the Volume, the job triggers the end-to-end flow.
- **Monitoring & Notifications:**     - Automated email notifications are configured to trigger upon job completion (Success/Failure).
    - Status updates include Job ID, Run ID, and total duration for easy operational tracking.

<img width="2879" height="1453" alt="image" src="https://github.com/user-attachments/assets/94a04cba-f953-4b81-9455-c4cdae3e58dd" />

<img width="2879" height="1458" alt="image" src="https://github.com/user-attachments/assets/b3a456e7-b753-4a5f-bc17-66a3274e3fe6" />

<img width="2341" height="1101" alt="image" src="https://github.com/user-attachments/assets/6bd52b57-e86c-47df-962a-678ed924828c" />


---

## 🚀 Key Features

- **Declarative ETL:** Logic defined via PySpark decorators, leaving orchestration to the DLT engine.
- **Data Quality Expectations:** Built-in monitoring to "Stop", "Warn", or "Drop" invalid data.
- **Automated CDC:** Handles Slowly Changing Dimensions (SCD) without manual `MERGE` statements.
- **Unified Orchestration:** Fully automated via **Databricks Workflows**.

---

## ▶️ How to Run
- Storage: Upload the datasets to your Unity Catalog Volume path.
- Configuration: Create a new Delta Live Tables pipeline in Databricks.
- Source: Point the pipeline to the Python files in the dlt_pipeline/ folder.
- Execution: Run the pipeline to build the Bronze, Silver, and Gold tables.
- Orchestration: Schedule the pipeline using Databricks Workflows for automation.

---

## 🎥 YouTube Walkthrough

https://youtu.be/8_Tr2vbTyw4?si=JHNsRBqTP9V6xZqC

---

## 📊 Output

- Automated Workflow Job: A scheduled, monitored job orchestrating the full ETL lifecycle.
- Data Quality Dashboards: Integrated DLT event logs showing pass/fail rates for data expectations.
- Automated Data Lineage: Full end-to-end visibility in Unity Catalog from Volume to Gold table.

---

## 📌 Dataset
Self-Created Banking Dataset: The .csv datasets used for this project were custom-built to demonstrate production-level ETL patterns.
