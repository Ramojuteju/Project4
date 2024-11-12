# Databricks notebook source
# Spark configuration value

spark.conf.set(
    "fs.azure.account.key.microdata23.dfs.core.windows.net",
    "kMyO4k30H9duja12tOKGNuxFMttKW0111kGrpqJaGHwG/0UyIGGighmGCmOUvcWRzV6ARBD5624U+ASt2rdN7g==")

# COMMAND ----------

application_id = "548ea50c-8e87-47b3-a33d-a1e519a81173"
service_credential = "j0q8Q~pP6TYF.2PI3WRTjcj7ZsOwVTwAJDWWocgJ"
directory_id = "32e6e18b-76be-433f-82ae-805603a70e49"

spark.conf.set("fs.azure.account.auth.type.microdata23.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.microdata23.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.microdata23.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.microdata23.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.microdata23.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

client_id = "548ea50c-8e87-47b3-a33d-a1e519a81173"
client_secret = "j0q8Q~pP6TYF.2PI3WRTjcj7ZsOwVTwAJDWWocgJ"
directory_id = "32e6e18b-76be-433f-82ae-805603a70e49"

spark.conf.set("fs.azure.account.auth.type.microdata23.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.microdata23.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.microdata23.dfs.core.windows.net", f"{client_id}")
spark.conf.set("fs.azure.account.oauth2.client.secret.microdata23.dfs.core.windows.net", f"{client_secret}")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.microdata23.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")


# COMMAND ----------

dbutils.secrets.list('microkeyvault')

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"
}

# COMMAND ----------

mount_point = "/mnt/microdata23/raw"

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Unmount the existing mount point
    dbutils.fs.unmount(mount_point)
    print(f"Unmounted existing mount at {mount_point}")

try:
    dbutils.fs.mount(
        source="abfss://raw@storagedb23.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted successfully at{mount_point}")
except Exception as e:
    print(f"Error mounting: {e}")

# COMMAND ----------

mount_point = "/mnt/storagedb23/processed"

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    # Unmount the existing mount point
    dbutils.fs.unmount(mount_point)
    print(f"Unmounted existing mount at {mount_point}")

try:
    dbutils.fs.mount(
        source="abfss://processed@storagedb23.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted successfully at{mount_point}")
except Exception as e:
    print(f"Error mounting: {e}")

# COMMAND ----------

df1 = spark.read.csv("/mnt/storagedb23/raw/Sales.csv", header=True, inferSchema="true");
df1.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df2 = df1.withColumn("ingestion_timestamp",current_timestamp())

df2.show()

# COMMAND ----------

df2.write.format("delta").mode("overwrite").save("/mnt/storagedb23/processed/delta/sales")

# COMMAND ----------

from delta.tables import DeltaTable

existing_data = DeltaTable.forPath(spark,"/mnt/storagedb23/processed/delta/sales")

existing_data.alias("existing").merge(df2.alias("new"),"existing.OrderID=new.OrderID").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

spark.read.format("delta").load("/mnt/storagedb23/processed/abc/delta/sales").show(30)