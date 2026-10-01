# Databricks notebook source
from pyspark.sql import functions as F
import json

silver = spark.table("workspace.rova.silver_products_it")
bronze = spark.table("workspace.rova.bronze_products")

def deslug(col):
    # "en:breakfast-cereals" -> "Breakfast Cereals"
    return F.initcap(F.regexp_replace(F.regexp_replace(col, r"^[a-z]{2}:", ""), "-", " "))

def first_item(col):
    return F.when(F.size(col) > 0, F.element_at(col, 1))

def last_item(col):
    return F.when(F.size(col) > 0, F.element_at(col, -1))

# COMMAND ----------

base = silver

# Open Food Facts has a "main_category" column (the most specific category); use it if present
if "main_category" in bronze.columns:
    main_cat = bronze.select("code", "main_category").dropDuplicates(["code"])
    base = base.join(main_cat, "code", "left")
else:
    base = base.withColumn("main_category", F.lit(None).cast("string"))

brand_raw = F.trim(F.split(F.col("brands"), ",").getItem(0))  # first brand only

base = (base
    .withColumn("main_category", F.when(F.trim("main_category") != "", F.trim("main_category")))
    .withColumn("category_key", F.coalesce("main_category", last_item("categories_tags"), F.lit("unknown")))
    .withColumn("category_group_key", F.coalesce(first_item("categories_tags"), F.lit("unknown")))
    .withColumn("brand_display", F.when(brand_raw != "", brand_raw))
    .withColumn("brand_key", F.coalesce(F.lower("brand_display"), F.lit("unknown")))
    .withColumn("is_organic", F.arrays_overlap("labels_tags", F.array(F.lit("en:organic"), F.lit("en:eu-organic"))))
    .withColumn("has_ingredients", F.col("ingredients_text").isNotNull() & (F.trim("ingredients_text") != "")))

# Remove parent tags when a more specific child is present (e.g. drop en:e322 if en:e322i is there).
# The next character must be a letter, so en:e150 is NOT treated as a parent of en:e1505 (a different additive).
base = base.withColumn("additives_tags",
    F.array_distinct(F.transform("additives_tags", lambda x: F.trim(x))))

base = base.withColumn("additives_tags",
    F.filter("additives_tags", lambda t: ~F.exists("additives_tags", lambda u:
        (F.length(u) > F.length(t))
        & u.startswith(t)
        & u.substr(F.length(t) + 1, F.lit(1)).rlike("^[a-z]$"))))

print(f"{base.count():,} products")

# COMMAND ----------

with open("/Volumes/workspace/default/raw/additives.json") as f:
    additives = json.load(f)

# Same mapping as src/data/additive-info.ts in the app
CLASS_PURPOSE = {
    "en:preservative": "safety", "en:antioxidant": "safety", "en:acidity-regulator": "safety",
    "en:sequestrant": "safety", "en:packaging-gas": "safety",
    "en:emulsifier": "texture", "en:stabiliser": "texture", "en:thickener": "texture",
    "en:gelling-agent": "texture", "en:raising-agent": "texture", "en:anti-caking-agent": "texture",
    "en:humectant": "texture", "en:firming-agent": "texture", "en:flour-treatment-agent": "texture",
    "en:bulking-agent": "texture", "en:emulsifying-salts": "texture", "en:foaming-agent": "texture",
    "en:anti-foaming-agent": "texture",
    "en:sweetener": "taste", "en:flavour-enhancer": "taste", "en:acid": "taste",
    "en:colour": "appearance", "en:glazing-agent": "appearance",
    "en:carrier": "texture", "en:propellent-gas": "texture", "en:coagulant": "texture",
}
PURPOSE_ORDER = ["safety", "texture", "taste", "appearance", "other"]
EU_WARNING_COLOURS = {"en:e102", "en:e104", "en:e110", "en:e122", "en:e124", "en:e129"}

# Hand-reviewed primary purpose for common multi-function additives.
# The general rule ("safety wins") mislabels these; each line states why.
PURPOSE_OVERRIDES = {
    "en:e322": "texture",  # lecithins: mainly an emulsifier (e.g. chocolate); antioxidant role is secondary
    "en:e450": "texture",  # diphosphates: mainly a raising agent / emulsifying salt
    "en:e503": "texture",  # ammonium carbonates: raising agent; no classes in the source taxonomy
    "en:e341": "texture",  # calcium phosphates: raising agent / anti-caking agent
    "en:e270": "safety",   # lactic acid: acidity regulator; no classes in the source taxonomy
}

def override_for(additive_id):
    # Applies to the parent and its sub-types (en:e322 -> en:e322i, en:e322ii)
    for key, purpose in PURPOSE_OVERRIDES.items():
        suffix = additive_id[len(key):]
        if additive_id.startswith(key) and (suffix == "" or suffix.isalpha()):
            return purpose
    return None

rows = []
for additive_id, e in additives.items():
    classes = e.get("classes") or []
    purposes = {CLASS_PURPOSE.get(c, "other") for c in classes}
    override = override_for(additive_id)
    primary = override or next((p for p in PURPOSE_ORDER if p in purposes), "other")
    full_name = e.get("name") or additive_id
    rows.append((
        additive_id,
        additive_id.split(":")[1].upper(),
        full_name.split(" - ", 1)[-1],
        e.get("nameIt"),
        primary,
        "override" if override else "rule",
        classes,
        e.get("overexposureRisk"),
        additive_id in EU_WARNING_COLOURS,
    ))

dim_additive = spark.createDataFrame(rows, """
    additive_id string, e_number string, name_en string, name_it string,
    primary_purpose string, purpose_source string, classes array<string>,
    efsa_overexposure_risk string, has_eu_colour_warning boolean
""")

# Unknown members: tags found in products but not in our additive dictionary
all_tags = (base.select(F.explode("additives_tags").alias("additive_id"))
                .select(F.trim("additive_id").alias("additive_id"))
                .distinct())

unknown_additives = (all_tags
    .join(dim_additive.select("additive_id"), "additive_id", "left_anti")
    .withColumn("e_number", F.lit(None).cast("string"))
    .withColumn("name_en", deslug("additive_id"))
    .withColumn("name_it", F.lit(None).cast("string"))
    .withColumn("primary_purpose", F.lit("unknown"))
    .withColumn("purpose_source", F.lit("unknown"))
    .withColumn("classes", F.array().cast("array<string>"))
    .withColumn("efsa_overexposure_risk", F.lit(None).cast("string"))
    .withColumn("has_eu_colour_warning", F.lit(False)))

dim_additive = dim_additive.unionByName(unknown_additives)
print(f"Added {unknown_additives.count()} unknown additive members")

display(dim_additive.limit(10))

# COMMAND ----------

dim_category = (base
    .groupBy("category_key")
    .agg(F.mode("category_group_key").alias("category_group_key"))
    .withColumn("category_name", deslug("category_key"))
    .withColumn("category_group_name", deslug("category_group_key")))

dim_brand = (base
    .groupBy("brand_key")
    .agg(F.mode("brand_display").alias("brand_name"))
    .withColumn("brand_name", F.coalesce("brand_name", F.lit("Unknown"))))

dim_product = base.select(
    F.col("code").alias("product_id"), "product_name", "brand_key", "category_key",
    "nova_group", "is_organic", "has_ingredients", "last_modified")

# COMMAND ----------

fact_product_additive = (base
    .select(F.col("code").alias("product_id"), "category_key", "brand_key",
            F.explode("additives_tags").alias("additive_id"))
    .withColumn("additive_id", F.trim("additive_id"))
    .dropDuplicates(["product_id", "additive_id"]))

purposes = ["safety", "texture", "taste", "appearance", "other", "unknown"]

purpose_counts = (fact_product_additive
    .join(dim_additive.select("additive_id", "primary_purpose"), "additive_id", "left")
    .withColumn("primary_purpose", F.coalesce("primary_purpose", F.lit("unknown")))
    .groupBy("product_id")
    .pivot("primary_purpose", purposes)
    .count())

for p in purposes:
    purpose_counts = purpose_counts.withColumnRenamed(p, f"n_{p}")
count_cols = [f"n_{p}" for p in purposes]

fact_product_profile = (base
    .select(F.col("code").alias("product_id"), "category_key", "brand_key",
            "is_organic", "has_ingredients", "nova_group")
    .join(purpose_counts, "product_id", "left")
    .fillna(0, subset=count_cols)
    .withColumn("additive_count", sum(F.col(c) for c in count_cols)))

# COMMAND ----------

tables = {
    "dim_product": dim_product,
    "dim_additive": dim_additive,
    "dim_category": dim_category,
    "dim_brand": dim_brand,
    "fact_product_additive": fact_product_additive,
    "fact_product_profile": fact_product_profile,
}
for name, df in tables.items():
    full = f"workspace.rova.gold_{name}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    print(f"{full}: {spark.table(full).count():,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'additive tags missing from dim_additive' AS check_name, COUNT(*) AS n
# MAGIC FROM workspace.rova.gold_fact_product_additive f
# MAGIC LEFT JOIN workspace.rova.gold_dim_additive d ON f.additive_id = d.additive_id
# MAGIC WHERE d.additive_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'products with unknown category', COUNT(*)
# MAGIC FROM workspace.rova.gold_dim_product WHERE category_key = 'unknown'
# MAGIC UNION ALL
# MAGIC SELECT 'products with unknown brand', COUNT(*)
# MAGIC FROM workspace.rova.gold_dim_product WHERE brand_key = 'unknown'
# MAGIC UNION ALL
# MAGIC SELECT 'duplicate product ids', COUNT(*) - COUNT(DISTINCT product_id)
# MAGIC FROM workspace.rova.gold_dim_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Additive density by broad category (only products with an ingredient list)
# MAGIC SELECT c.category_group_name,
# MAGIC        COUNT(*)                                                           AS products,
# MAGIC        ROUND(AVG(p.additive_count), 2)                                    AS avg_additives,
# MAGIC        ROUND(100 * AVG(CASE WHEN p.n_appearance > 0 THEN 1 ELSE 0 END), 1) AS pct_with_appearance_additive
# MAGIC FROM workspace.rova.gold_fact_product_profile p
# MAGIC JOIN workspace.rova.gold_dim_category c ON p.category_key = c.category_key
# MAGIC WHERE p.has_ingredients
# MAGIC GROUP BY c.category_group_name
# MAGIC HAVING COUNT(*) >= 200
# MAGIC ORDER BY avg_additives DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                               AS products,
# MAGIC   SUM(CASE WHEN has_ingredients THEN 1 ELSE 0 END)                       AS with_ingredients,
# MAGIC   ROUND(100 * AVG(CASE WHEN has_ingredients THEN 1 ELSE 0 END), 1)       AS pct_with_ingredients,
# MAGIC   ROUND(100 * AVG(CASE WHEN has_ingredients AND category_key <> 'unknown' THEN 1 ELSE 0 END), 1) AS pct_usable_for_analysis
# MAGIC FROM workspace.rova.gold_dim_product;

# COMMAND ----------

bronze = spark.table("workspace.rova.bronze_products")
print([c for c in bronze.columns if c.startswith("ingredients")])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.additive_id, COUNT(*) AS rows_affected
# MAGIC FROM workspace.rova.gold_fact_product_additive f
# MAGIC LEFT JOIN workspace.rova.gold_dim_additive d ON f.additive_id = d.additive_id
# MAGIC WHERE d.additive_id IS NULL
# MAGIC GROUP BY f.additive_id
# MAGIC ORDER BY rows_affected DESC;