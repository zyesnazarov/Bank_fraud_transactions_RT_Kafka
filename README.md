# Real-Time Fraud Detection with Kafka & Databricks

## Project Overview
This project showcases a **real-time fraud detection pipeline**, developed during my time at **Deloitte's Forensic Team**. Our objective was to design a **streaming data pipeline** capable of identifying fraudulent transactions in near real-time using **Apache Kafka, Databricks, and Azure Data Lake**.

### **Business Context**
Financial fraud is a persistent challenge in the banking sector. The goal of this project was to:
- **Detect fraudulent transactions** in real time.
- **Leverage big data processing** to analyze streaming transactions.
- **Generate fraud risk scores** and trigger alerts for suspicious activities.

## **Architecture**
The fraud detection pipeline follows this architecture:

1. **Transaction Simulation**: A **Kafka producer** simulates live bank transactions, generating transaction data streams (due to NDA, i could not provide exact dataset that was provided to us, therefore I tried to simulate the incomeing data from Banking OLTP system via random Python package)
2. **Streaming Ingestion**: Apache Kafka brokers the real-time transactions into a **Kafka topic**.
3. **Data Processing with Databricks & Spark**:
   - Spark processes the incoming data in micro-batches.
   - Fraud detection logic applies rules-based and statistical anomaly detection techniques.
4. **Storage & Monitoring**:
   - Transactions are stored in **Azure Data Lake** for further analysis.
   - Fraud alerts are generated based on risk scores.

## **Technology Stack**
- **Apache Kafka** - Message streaming for real-time transactions.
- **Databricks (Apache Spark)** - Real-time fraud detection & analytics.
- **Azure Data Lake** - Scalable storage for transaction records.
- **Python (pyspark, Kafka libraries)** - Implementation of data ingestion and processing.

## **Project Breakdown**
### **1. Kafka Producer for Transaction Simulation**
- Generates **synthetic bank transactions** in real time.
- Publishes transactions to a **Kafka topic**.

Code: [`kafka-producer.py`](./kafka-producer.py)

### **2. Fraud Detection with Databricks & Spark**
- Reads the Kafka stream in **real-time**.
- Applies **fraud detection rules** such as:
  - Large transactions within a short timeframe.
  - Transactions from unusual locations.
  - Multiple high-value transactions from the same account.
- Writes flagged transactions to **Azure Data Lake**.

Code: [`fraud-transactions-databricks.py`](./fraud-transactions-databricks.py)

## **How to Run the Project**
### **1. Start Kafka & Producer**
1. Set up **Kafka** and create a topic for transactions.
2. Run the Kafka producer to start generating transaction data:
   ```bash
   python kafka-producer.py
   ```

### **2. Process Transactions in Databricks**
1. Load the **Databricks notebook** and run the streaming job.
2. Monitor the transaction flow and detect fraudulent activity.

## **Results & Impact**
- Successfully **processed thousands of transactions in real-time**.
- Identified **high-risk fraudulent transactions** using a combination of **rules-based detection & statistical models**.
- Improved **response time for fraud alerts**, reducing financial losses for banks.

## **Future Enhancements**
- Implement **ML-based fraud detection models** (e.g., Isolation Forest, Autoencoders).
- Enhance Kafka consumers for **faster processing & alerting mechanisms**.
- Integrate with **real-time dashboards** (e.g., Power BI, Grafana) for visualization.
