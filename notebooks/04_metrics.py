# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.rova.v_category_metrics AS
# MAGIC WITH eu_colour_products AS (
# MAGIC   SELECT DISTINCT f.product_id
# MAGIC   FROM workspace.rova.gold_fact_product_additive f
# MAGIC   JOIN workspace.rova.gold_dim_additive d ON f.additive_id = d.additive_id
# MAGIC   WHERE d.has_eu_colour_warning
# MAGIC )
# MAGIC SELECT
# MAGIC   c.category_group_name,
# MAGIC   COUNT(*)                                                        AS products,
# MAGIC   SUM(CASE WHEN p.has_ingredients THEN 1 ELSE 0 END)              AS products_with_ingredients,
# MAGIC   ROUND(100 * AVG(CASE WHEN p.has_ingredients THEN 1 ELSE 0 END), 1) AS ingredient_coverage_pct,
# MAGIC   -- AVG ignores NULLs, so "CASE WHEN has_ingredients THEN ... END" limits the denominator to products with ingredients
# MAGIC   ROUND(AVG(CASE WHEN p.has_ingredients THEN p.additive_count END), 2) AS additive_density,
# MAGIC   ROUND(100 * AVG(CASE WHEN p.has_ingredients THEN IF(p.n_appearance > 0, 1, 0) END), 1) AS appearance_share_pct,
# MAGIC   ROUND(100 * AVG(CASE WHEN p.has_ingredients THEN IF(e.product_id IS NOT NULL, 1, 0) END), 1) AS eu_colour_share_pct
# MAGIC FROM workspace.rova.gold_fact_product_profile p
# MAGIC JOIN workspace.rova.gold_dim_category c ON p.category_key = c.category_key
# MAGIC LEFT JOIN eu_colour_products e ON p.product_id = e.product_id
# MAGIC GROUP BY c.category_group_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.rova.v_additive_usage AS
# MAGIC SELECT
# MAGIC   d.additive_id,
# MAGIC   d.e_number,
# MAGIC   d.name_en,
# MAGIC   d.primary_purpose,
# MAGIC   d.has_eu_colour_warning,
# MAGIC   COUNT(DISTINCT f.product_id) AS products,
# MAGIC   ROUND(100 * COUNT(DISTINCT f.product_id) /
# MAGIC         (SELECT COUNT(*) FROM workspace.rova.gold_dim_product WHERE has_ingredients), 2) AS pct_of_products_with_ingredients
# MAGIC FROM workspace.rova.gold_fact_product_additive f
# MAGIC JOIN workspace.rova.gold_dim_additive d ON f.additive_id = d.additive_id
# MAGIC GROUP BY d.additive_id, d.e_number, d.name_en, d.primary_purpose, d.has_eu_colour_warning;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.rova.v_kpi_summary AS
# MAGIC SELECT
# MAGIC   COUNT(*)                                                            AS products,
# MAGIC   ROUND(100 * AVG(CASE WHEN has_ingredients THEN 1 ELSE 0 END), 1)    AS ingredient_coverage_pct,
# MAGIC   ROUND(AVG(CASE WHEN has_ingredients THEN additive_count END), 2)    AS additive_density,
# MAGIC   ROUND(100 * AVG(CASE WHEN has_ingredients THEN IF(n_appearance > 0, 1, 0) END), 1) AS appearance_share_pct,
# MAGIC   ROUND(100 * AVG(CASE WHEN has_ingredients THEN IF(category_key = 'unknown', 1, 0) END), 1) AS category_gap_pct,
# MAGIC   (SELECT ROUND(100 * AVG(CASE WHEN status = 'pass' THEN 1 ELSE 0 END), 0)
# MAGIC      FROM workspace.rova.dq_results
# MAGIC     WHERE run_ts = (SELECT MAX(run_ts) FROM workspace.rova.dq_results)) AS dq_pass_rate_pct
# MAGIC FROM workspace.rova.gold_fact_product_profile;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.rova.v_kpi_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.rova.v_additive_usage ORDER BY products DESC LIMIT 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Self-join: pairs like en:e322 + en:e322i on the same product
# MAGIC SELECT parent.additive_id AS parent_tag,
# MAGIC        child.additive_id  AS child_tag,
# MAGIC        COUNT(*)           AS products_with_both
# MAGIC FROM workspace.rova.gold_fact_product_additive parent
# MAGIC JOIN workspace.rova.gold_fact_product_additive child
# MAGIC   ON  parent.product_id = child.product_id
# MAGIC   AND child.additive_id <> parent.additive_id
# MAGIC   AND child.additive_id LIKE CONCAT(parent.additive_id, '%')
# MAGIC   -- the next character must be a letter (e322 -> e322i), not a digit (e150 -> e1505 is a different additive)
# MAGIC   AND substr(child.additive_id, length(parent.additive_id) + 1, 1) RLIKE '[a-z]'
# MAGIC GROUP BY parent.additive_id, child.additive_id
# MAGIC ORDER BY products_with_both DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT e_number, name_en, primary_purpose, classes
# MAGIC FROM workspace.rova.gold_dim_additive
# MAGIC WHERE e_number IN ('E322', 'E450', 'E503', 'E270', 'E330', 'E300');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cls AS function_class, COUNT(DISTINCT f.product_id) AS products
# MAGIC FROM workspace.rova.gold_fact_product_additive f
# MAGIC JOIN workspace.rova.gold_dim_additive d ON f.additive_id = d.additive_id
# MAGIC LATERAL VIEW explode(d.classes) t AS cls
# MAGIC GROUP BY cls
# MAGIC ORDER BY products DESC;