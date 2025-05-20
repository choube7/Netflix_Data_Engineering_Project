# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver Data Transformation

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("abfss://bronze@netflixprojectshivansh.dfs.core.windows.net/netflix_titles") 

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.fillna({"duration_minutes": 0,"duration_seasons":1})

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("duration_minutes",df["duration_minutes"].cast(IntegerType()))\
    .withColumn("duration_seasons",df["duration_seasons"].cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("Shorttitle",split(col('title'),':')[0])
df.display()

# COMMAND ----------

df=df.withColumn("rating",split(col('rating'),'-')[0])
df.display()

# COMMAND ----------

df =df.withColumn("type_flag",when(col('type')=='Movie',1)\
    .when(col('type')=='TV Show',2)\
    .otherwise(0))

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("duration_ranking",dense_rank().over(Window.orderBy(col('duration_minutes').desc())))

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("temp_view")

# COMMAND ----------

df=spark.sql("""
             select * from temp_view
             """
            
             )

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

df_visual=df.groupBy("type").agg(count("*").alias("total_count"))
display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@netflixprojectshivansh.dfs.core.windows.net/netflix_titles").save()