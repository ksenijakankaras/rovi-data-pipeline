# Databricks notebook source
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.rova")

# COMMAND ----------

import re

path = "/Volumes/workspace/default/raw/en.openfoodfacts.org.products.csv.gz"

raw = (spark.read
       .option("header", True)
       .option("sep", "\t")
       .option("quote", "\u0000")
       .option("inferSchema", False)
       .csv(path))

raw = raw.toDF(*[re.sub(r"[^0-9a-zA-Z_]", "_", c) for c in raw.columns])

print(f"{len(raw.columns)} columns")
raw.write.mode("overwrite").saveAsTable("workspace.rova.bronze_products")

# COMMAND ----------

bronze = spark.table("workspace.rova.bronze_products")
print(f"{bronze.count():,} products worldwide")

# COMMAND ----------

from pyspark.sql import functions as F

wanted = ["code", "product_name", "brands", "categories_tags", "countries_tags",
          "ingredients_text", "additives_tags", "allergens", "labels_tags",
          "nova_group", "last_modified_t"]
cols = [c for c in wanted if c in bronze.columns]

def to_array(col):
    return F.when(F.col(col).isNull() | (F.col(col) == ""), F.array()).otherwise(F.split(col, ","))

silver = (bronze.select(*cols)
          .filter(F.col("code").isNotNull())
          .filter(F.col("countries_tags").contains("en:italy"))
          .withColumn("categories_tags", to_array("categories_tags"))
          .withColumn("countries_tags", to_array("countries_tags"))
          .withColumn("additives_tags", to_array("additives_tags"))
          .withColumn("labels_tags", to_array("labels_tags"))
          .withColumn("nova_group", F.col("nova_group").cast("int"))
          .withColumn("last_modified", F.from_unixtime(F.col("last_modified_t").cast("long")).cast("timestamp"))
          .drop("last_modified_t")
          .dropDuplicates(["code"]))

silver.write.mode("overwrite").saveAsTable("workspace.rova.silver_products_it")
print(f"{spark.table('workspace.rova.silver_products_it').count():,} Italian products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS products,
# MAGIC   ROUND(100 * AVG(CASE WHEN ingredients_text IS NOT NULL THEN 1 ELSE 0 END), 1) AS pct_with_ingredients,
# MAGIC   ROUND(100 * AVG(CASE WHEN size(additives_tags) > 0 THEN 1 ELSE 0 END), 1)     AS pct_with_additives,
# MAGIC   ROUND(100 * AVG(CASE WHEN size(categories_tags) = 0 THEN 1 ELSE 0 END), 1)    AS pct_missing_category
# MAGIC FROM workspace.rova.silver_products_it;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT additive, COUNT(*) AS products
# MAGIC FROM workspace.rova.silver_products_it
# MAGIC LATERAL VIEW explode(additives_tags) t AS additive
# MAGIC GROUP BY additive
# MAGIC ORDER BY products DESC
# MAGIC LIMIT 20;