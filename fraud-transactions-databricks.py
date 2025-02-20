# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, when, count, round, hour
import pyspark.sql.functions as F

# COMMAND ----------

spark = SparkSession.builder \
    .appName("FraudDetectionPipeline") \
    .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0") \
    .getOrCreate()
spark.conf.set("fs.azure.account.key.storagezyesnazarov.dfs.core.windows.net", "TY4OPOBPKi7VK4VCqf+IefBLLvjCgBDYqpzyoQPSxW+w7xzE2Bha6Cll0skDYJbM9M9dH62gd+Ji+AStfY+QOA==")


# COMMAND ----------

#reading from kafka producer from EC2 instance on AWS.
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "3.95.137.41:9092") \
    .option("subscribe", "bank-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# COMMAND ----------


#Stream 
df_transformed = df.selectExpr("CAST(value AS STRING) as json_data") \
    .selectExpr("from_json(json_data, 'transaction_id STRING, account_no INT, transaction_timestamp STRING, amount DOUBLE, balance_amt DOUBLE, bank_id INT') as data") \
    .select("data.*")

# COMMAND ----------

df_transformed = df_transformed.withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))

# COMMAND ----------

df_fraud = df_transformed.withColumn("fraud_large_withdrawal", when(col("amount") > 10000,1).otherwise(0)).withColumn("fraud_low_balance", when((col("amount") < 0) & (col("balance_amt") < 100), 1).otherwise(0))

# COMMAND ----------

# ratio of transaction amount to balance 
df_fraud = df_fraud.withColumn("amount_balance_ratio", round((col("amount") / col("balance_amt")), 3))

# COMMAND ----------

#night withdrawals between 00:00 and 05:00
df_fraud = df_fraud.withColumn("night_withdrawal", when(
    (col("amount") < 0) & (hour(col("transaction_timestamp")).between(0,5)), 1).otherwise(0)
                   )

# COMMAND ----------

#making an aggregated fraud indicator encompaassing all the frauds triggers before
df_fraud = df_fraud.withColumn("fraud_indicator", when(
    (col("fraud_large_withdrawal") == 1) | 
    (col("fraud_low_balance") == 1) | 
    (col("amount_balance_ratio") <= -0.2) |
    (col("night_withdrawal") == 1), 1).otherwise(0)
                   )

# COMMAND ----------

df_fraud.writeStream \
    .format("delta") \
    .option("path", "abfss://fraud-transactions-project@storagezyesnazarov.dfs.core.windows.net/delta/transactions/") \
    .option("checkpointLocation", "abfss://fraud-transactions-project@storagezyesnazarov.dfs.core.windows.net/checkpoints/") \
    .outputMode("append") \
    .start()

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE transactions
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM delta.`abfss://fraud-transactions-project@storagezyesnazarov.dfs.core.windows.net/delta/transactions`

# COMMAND ----------

