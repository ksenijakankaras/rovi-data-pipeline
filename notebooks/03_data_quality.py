# Databricks notebook source
from datetime import datetime, timezone

S = "workspace.rova"

# (check name, table, SQL returning one number, rule, threshold, severity)
# Thresholds are set just past today's baseline, so they flag regressions rather than known gaps.
CHECKS = [
    ("duplicate_product_ids", "gold_dim_product",
     f"SELECT COUNT(*) - COUNT(DISTINCT product_id) FROM {S}.gold_dim_product", "max", 0, "fail"),
    ("product_count", "gold_dim_product",
     f"SELECT COUNT(*) FROM {S}.gold_dim_product", "min", 250000, "fail"),
    ("pct_with_ingredients", "gold_dim_product",
     f"SELECT 100 * AVG(CASE WHEN has_ingredients THEN 1 ELSE 0 END) FROM {S}.gold_dim_product", "min", 12, "warn"),
    ("pct_unknown_category", "gold_dim_product",
     f"SELECT 100 * AVG(CASE WHEN category_key = 'unknown' THEN 1 ELSE 0 END) FROM {S}.gold_dim_product", "max", 55, "warn"),
    ("pct_unknown_brand", "gold_dim_product",
     f"SELECT 100 * AVG(CASE WHEN brand_key = 'unknown' THEN 1 ELSE 0 END) FROM {S}.gold_dim_product", "max", 40, "warn"),
    # Valid barcodes are EAN-8, UPC-A (12), EAN-13 or GTIN-14. {{ }} escapes braces inside an f-string.
    ("pct_invalid_barcode", "gold_dim_product",
     f"SELECT 100 * AVG(CASE WHEN product_id RLIKE '^[0-9]{{8}}$|^[0-9]{{12,14}}$' THEN 0 ELSE 1 END) FROM {S}.gold_dim_product", "max", 5, "warn"),
    ("pct_additive_rows_unclassified", "gold_fact_product_additive",
     f"""SELECT 100 * AVG(CASE WHEN d.additive_id IS NULL OR d.primary_purpose = 'unknown' THEN 1 ELSE 0 END)
         FROM {S}.gold_fact_product_additive f
         LEFT JOIN {S}.gold_dim_additive d ON f.additive_id = d.additive_id""", "max", 0.5, "warn"),
    ("orphan_fact_rows", "gold_fact_product_additive",
     f"""SELECT COUNT(*) FROM {S}.gold_fact_product_additive f
         LEFT JOIN {S}.gold_dim_product p ON f.product_id = p.product_id
         WHERE p.product_id IS NULL""", "max", 0, "fail"),
    ("additives_without_ingredient_text", "gold_fact_product_profile",
     f"SELECT COUNT(*) FROM {S}.gold_fact_product_profile WHERE additive_count > 0 AND NOT has_ingredients", "max", 0, "warn"),
    ("it_ingredient_tags_without_text", "bronze_products",
     f"""SELECT COUNT(*) FROM {S}.bronze_products
         WHERE countries_tags LIKE '%en:italy%'
           AND (ingredients_text IS NULL OR trim(ingredients_text) = '')
           AND ingredients_tags IS NOT NULL AND trim(ingredients_tags) <> ''""", "max", 0, "warn"),
    ("parent_child_additive_pairs", "gold_fact_product_additive",
     f"""SELECT COUNT(*) FROM {S}.gold_fact_product_additive p
         JOIN {S}.gold_fact_product_additive c
           ON p.product_id = c.product_id AND c.additive_id <> p.additive_id
          AND c.additive_id LIKE CONCAT(p.additive_id, '%')
          AND substr(c.additive_id, length(p.additive_id) + 1, 1) RLIKE '[a-z]'""", "max", 0, "fail"),
]

# COMMAND ----------

run_ts = datetime.now(timezone.utc)
rows = []
for name, table, sql, rule, threshold, severity in CHECKS:
    value = spark.sql(sql).collect()[0][0]
    value = float(value) if value is not None else None
    passed = value is not None and (value <= threshold if rule == "max" else value >= threshold)
    rows.append((run_ts, name, table, value, rule, float(threshold), "pass" if passed else severity))

results = spark.createDataFrame(rows, """
    run_ts timestamp, check_name string, table_name string, value double,
    rule string, threshold double, status string
""")
results.write.mode("append").saveAsTable(f"{S}.dq_results")
display(results)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT check_name, table_name, ROUND(value, 2) AS value, rule, threshold, status
# MAGIC FROM workspace.rova.dq_results
# MAGIC WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.rova.dq_results)
# MAGIC ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, check_name;