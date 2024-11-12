# Databricks notebook source
# Spark configuration value

spark.conf.set(
    "fs.azure.account.key.microdata23.dfs.core.windows.net",
    "kMyO4k30H9duja12tOKGNuxFMttKW0111kGrpqJaGHwG/0UyIGGighmGCmOUvcWRzV6ARBD5624U+ASt2rdN7g==")

# COMMAND ----------

# Displaying Data Frame in Tabular Format

display(dbutils.fs.ls("abfss://silver@microdata23.dfs.core.windows.net"))

# COMMAND ----------

# Reading data from silver container
accounts_df = spark.read.format("delta").load("abfss://silver@microdata23.dfs.core.windows.net/delta/accounts_delta")
customers_df = spark.read.format("delta").load("abfss://silver@microdata23.dfs.core.windows.net/delta/customers_delta")

# COMMAND ----------

# Register the DataFrames as temporary views
accounts_df.createOrReplaceTempView("accounts")
customers_df.createOrReplaceTempView("customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL Query for total balance calculation
# MAGIC SELECT 
# MAGIC     c.customer_id, 
# MAGIC     c.first_name, 
# MAGIC     SUM(a.balance) AS total_balance
# MAGIC FROM 
# MAGIC     customers as c
# MAGIC JOIN 
# MAGIC     accounts as a ON c.customer_id = a.customer_id
# MAGIC GROUP BY 
# MAGIC     c.customer_id, c.first_name;
# MAGIC

# COMMAND ----------

# Rename _sqldf to a new variable name
gold_df = _sqldf


# COMMAND ----------

# Define the path for gold container
gold_delta = "abfss://gold@microdata23.dfs.core.windows.net/delta/gold_delta"

# Save the DataFrame in Delta format, overwriting if it exists
gold_df.write.format("delta").mode("overwrite").save(gold_delta)

# COMMAND ----------

# Displaying Data Frame in Tabular Format

display(dbutils.fs.ls("abfss://gold@microdata23.dfs.core.windows.net/"))