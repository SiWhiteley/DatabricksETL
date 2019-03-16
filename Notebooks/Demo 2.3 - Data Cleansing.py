# Databricks notebook source
# DBTITLE 1,Configure ADLS
#Gather relevant keys from our Secret Scope
ServicePrincipalID = dbutils.secrets.get(scope = "Analysts", key = "SPID")
ServicePrincipalKey = dbutils.secrets.get(scope = "Analysts", key = "SPKey")
DirectoryID = dbutils.secrets.get(scope = "Analysts", key = "DirectoryID")
DBUser = dbutils.secrets.get(scope = "Analysts", key = "DBUser")
DBPassword = dbutils.secrets.get(scope = "Analysts", key = "DBPword")


#Combine DirectoryID into full string
Directory = "https://login.microsoftonline.com/{}/oauth2/token".format(DirectoryID)

#Configure our ADLS Gen 2 connection with our service principal details
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", ServicePrincipalID)
spark.conf.set("fs.azure.account.oauth2.client.secret", ServicePrincipalKey)
spark.conf.set("fs.azure.account.oauth2.client.endpoint", Directory)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Data
# MAGIC In this notebook we will take a dataset that has some noticeable issues - some string issues, duplicates etc, and we will create a cleaned dataset that we can use for later transformation
# MAGIC 
# MAGIC We will start by doing a PERMISSIVE read of our source data and stripping out the null records

# COMMAND ----------

# DBTITLE 1,Import Dataset without Failed Rows
from pyspark.sql.types import *
from pyspark.sql.functions import *

mySchema = StructType([
  StructField("LocationID", IntegerType(), True),
  StructField("Borough", StringType(), True),
  StructField("Zone", StringType(), True),
  StructField("service_zone", StringType(), True),
  StructField("_corrupt_record", StringType(), True)])

# Now create our DataFrame over the whole fileset, but applying our the schema from the sample rather than inferring
rawdf = (spark
       .read
       .option("header","true")
       .option("mode", "PERMISSIVE")
       .schema(mySchema)
       .csv("abfss://root@dblake.dfs.core.windows.net/RAW/Public/TaxiZones/V1/taxi+_zone_lookup.csv")
     )

# COMMAND ----------

# Let's get a baseline count before we start cleaning
rawdf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop unparsable records
# MAGIC As we've used the PERMISSIVE read mode, we have our two types of malformed records - those with additional columns and those with unparsable values. We want to skip just the unparsable values, so let's drop any row where any of the base attributes are null
# MAGIC 
# MAGIC The dropna() function does this for us - we can specify "any" or "all" to switch between dropping rows where any of the attributes are null, or only dropping when all attributes are null

# COMMAND ----------

# Drop rows that are entirely null from the dataset
rawdf =  rawdf.select("LocationID","Borough","Zone","service_zone").dropna("all")
rawdf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Ok, that dropped off the N/A row, the rest of the rows have some useful information, so let's take a look at what's in there

# COMMAND ----------

#Let's take a look at the data and see where we think there are problems
display(rawdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Cleaned Columns
# MAGIC Ok, so we clearly have a couple of issues within the service_zone attribute at least. 
# MAGIC 
# MAGIC Looks like we at least have some leading tabs/spaces, so let's do some basic string cleaning on each of our string values. We'll add new columns, calling simple pyspark.sql functions to trim the leading/trailing whitespace
# MAGIC 
# MAGIC You can use any of the pyspark sql functions to clean up your data - there are way more options than we have time on this course, so take this as a simple pattern and go explore the options you can use!

# COMMAND ----------

# Add cleaned columns
rawdf = (rawdf
           .withColumn("Borough", trim(rawdf.Borough))
           .withColumn("Zone", trim(rawdf.Zone))
           .withColumn("service_zone", trim(rawdf.service_zone))
         )

# COMMAND ----------

display(rawdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicate Records
# MAGIC Looking further into our dataset, we look to have several duplicate records
# MAGIC 
# MAGIC The spark DataFrame has a useful dropDuplicates() function that will automatically drop any rows that are exact duplicates of an earlier row. All fields have to match for this to work, other

# COMMAND ----------

dedupedf = rawdf.dropDuplicates()
dedupedf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write out table to BASE
# MAGIC Now that we have a cleaned dataset, let's land it to BASE so we can use it in our next step

# COMMAND ----------

dedupedf.write.parquet("abfss://root@dblake.dfs.core.windows.net/BASE/Public/TaxiZones/v1/parquet/")