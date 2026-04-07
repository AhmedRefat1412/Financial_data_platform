#  Financial Data Platform (End-to-End Data Engineering Project)

An end-to-end Data Engineering project designed to process, validate, and analyze financial transaction data for fraud detection using a modern data stack on AWS.

---

##  Project Overview

This project demonstrates how to build a scalable and production-ready data platform that transforms raw financial data into reliable, analytics-ready insights.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to ensure data quality, consistency, and performance.

---

## 🏗️ Architecture

![Architecture](https://github.com/AhmedRefat1412/Financial_data_platform/blob/main/docs/Financial_data_paltform.drawio.png)

---

## 📊 Dashboard

![Dashboard](https://github.com/AhmedRefat1412/Financial_data_platform/blob/main/docs/Dashboard.png)

---

## ⭐ Data Warehouse (Star Schema)

![Star Schema](https://github.com/AhmedRefat1412/Financial_data_platform/blob/main/docs/Data_Warehouse_Digram.png)

---



##  Data Pipeline

1. **Data Ingestion (Bronze Layer)**
   - Raw data loaded into Amazon S3
   - Data stored without modification

2. **Data Quality Validation**
   - Great Expectations used as a data quality gate
   - Ensures data accuracy before processing

3. **Data Processing (Silver Layer)**
   - AWS Glue (PySpark) for cleaning and transformation
   - Data standardized and structured

4. **Data Warehousing (Gold Layer)**
   - Data loaded into Amazon Redshift
   - Star Schema designed for analytical queries

5. **Data Modeling**
   - dbt used for transformations and business logic
   - Creation of business-ready datasets

6. **Orchestration**
   - Apache Airflow schedules and manages workflows

7. **Visualization**
   - Power BI dashboard for fraud detection insights

---

##  Tech Stack

- AWS S3 (Data Lake)
- AWS Glue (PySpark)
- Amazon Redshift (Data Warehouse)
- Apache Airflow
- Docker
- dbt
- Power BI
- Great Expectations

---

##  Data Scale

- Processed **2M+ records**
- Built with scalability and performance in mind



