# Databricks notebook source
# Spark configuration value

spark.conf.set(
    "fs.azure.account.key.microdata23.dfs.core.windows.net",
    "kMyO4k30H9duja12tOKGNuxFMttKW0111kGrpqJaGHwG/0UyIGGighmGCmOUvcWRzV6ARBD5624U+ASt2rdN7g==")

# COMMAND ----------

# Displaying Data Frame in Tabular Format

display(dbutils.fs.ls("abfss://bronze@microdata23.dfs.core.windows.net"))

# COMMAND ----------

# Reading data from bronze container

accounts_df = spark.read.csv("abfss://bronze@microdata23.dfs.core.windows.net/accounts.csv", header=True, inferSchema="true");
accounts_df.show()
customers_df = spark.read.csv("abfss://bronze@microdata23.dfs.core.windows.net/customers.csv", header=True, inferSchema="true");
customers_df.show()
loan_payments_df = spark.read.csv("abfss://bronze@microdata23.dfs.core.windows.net/loan_payments.csv", header=True, inferSchema="true");
loan_payments_df.show()
loans_df = spark.read.csv("abfss://bronze@microdata23.dfs.core.windows.net/loans.csv", header=True, inferSchema="true");
loans_df.show()
transactions_df = spark.read.csv("abfss://bronze@microdata23.dfs.core.windows.net/transactions.csv", header=True, inferSchema="true");
transactions_df.show()

# COMMAND ----------

# Cleaning the data - removing rows with missing values

accounts_df = accounts_df.filter(accounts_df.account_id.isNotNull())
customers_df = customers_df.filter(customers_df.customer_id.isNotNull())
loan_payments_df = loan_payments_df.filter(loan_payments_df.payment_id.isNotNull())
loans_df = loans_df.filter(loans_df.loan_id.isNotNull())
transactions_df = transactions_df.filter(transactions_df.transaction_id.isNotNull())

# COMMAND ----------

from pyspark.sql import functions as DE

# Apply data type conversions
accounts_df = accounts_df.withColumn("balance", DE.col("balance").cast("double"))
accounts_df = accounts_df.withColumn("account_id", DE.col("account_id").cast("int"))

customers_df = customers_df.withColumn("customer_id", DE.col("customer_id").cast("int"))
customers_df = customers_df.withColumn("zip", DE.col("zip").cast("int"))

loans_df = loans_df.withColumn("loan_id", DE.col("loan_id").cast("int"))
loans_df = loans_df.withColumn("customer_id", DE.col("customer_id").cast("int"))
loans_df = loans_df.withColumn("loan_amount", DE.col("loan_amount").cast("double"))
    
# Renaming columns using withColumnRenamed()
accounts_df = accounts_df.withColumnRenamed("account_type", "savings_checking")
customers_df = customers_df.withColumnRenamed("zip", "pincode")
loan_payments_df = loan_payments_df.withColumnRenamed("payment_amount", "pay_amount")
loans_df = loans_df.withColumnRenamed("interest_rate", "interest")
transactions_df = transactions_df.withColumnRenamed("transaction_type", "depoist_withdrawl")

# Dropping columns
customers_df = customers_df.drop("last_name","address")
loans_df = loans_df.drop("loan_term")
transactions_df = transactions_df.drop("transaction_date")

# Define the paths to the Silver container
silver_accounts = "abfss://silver@microdata23.dfs.core.windows.net/delta/accounts_delta"
silver_customers = "abfss://silver@microdata23.dfs.core.windows.net/delta/customers_delta"
silver_loan_payments = "abfss://silver@microdata23.dfs.core.windows.net/delta/loan_payments_delta"
silver_loans = "abfss://silver@microdata23.dfs.core.windows.net/delta/loans_delta"
silver_transactions = "abfss://silver@microdata23.dfs.core.windows.net/delta/transactions_delta"

# COMMAND ----------

# Save the cleaned DataFrames to the Silver container
accounts_df.write.format("delta").mode("overwrite").save(silver_accounts)
customers_df.write.format("delta").mode("overwrite").save(silver_customers)
loan_payments_df.write.format("delta").mode("overwrite").save(silver_loan_payments)
loans_df.write.format("delta").mode("overwrite").save(silver_loans)
transactions_df.write.format("delta").mode("overwrite").save(silver_transactions)

# COMMAND ----------

# Displaying Data Frame in Tabular Format

display(dbutils.fs.ls("abfss://silver@microdata23.dfs.core.windows.net"))