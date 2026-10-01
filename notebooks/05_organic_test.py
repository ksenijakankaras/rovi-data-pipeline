# Databricks notebook source
import pandas as pd
import numpy as np
from scipy import stats

df = spark.sql("""
    SELECT p.product_id, p.additive_count, p.is_organic, c.category_group_name AS category
    FROM workspace.rova.gold_fact_product_profile p
    JOIN workspace.rova.gold_dim_category c ON p.category_key = c.category_key
    WHERE p.has_ingredients AND c.category_group_name <> 'Unknown'
""").toPandas()

df["is_organic"] = df["is_organic"].fillna(False).astype(bool)
print(f"{len(df):,} products, {df.is_organic.mean():.1%} organic")

# COMMAND ----------

display(df.groupby("is_organic")["additive_count"].agg(["count", "mean", "median"]).reset_index())

u = stats.mannwhitneyu(df.loc[df.is_organic, "additive_count"],
                       df.loc[~df.is_organic, "additive_count"], alternative="two-sided")
print(f"Mann-Whitney U p-value: {u.pvalue:.2g}")

# COMMAND ----------

by_cat = (df.groupby("category")
            .agg(products=("product_id", "count"),
                 organic_share=("is_organic", "mean"),
                 avg_additives=("additive_count", "mean"))
            .round(3).sort_values("products", ascending=False).reset_index())
display(by_cat)

# COMMAND ----------

rows = []
for cat, g in df.groupby("category"):
    org, non = g.loc[g.is_organic, "additive_count"], g.loc[~g.is_organic, "additive_count"]
    if len(org) >= 30 and len(non) >= 30:   # skip categories too small to compare
        p = stats.mannwhitneyu(org, non, alternative="two-sided").pvalue
        rows.append((cat, len(org), len(non), org.mean(), non.mean(), org.mean() - non.mean(), p))

strat = pd.DataFrame(rows, columns=["category", "n_organic", "n_non_organic",
                                    "mean_organic", "mean_non_organic", "difference", "p_value"])
strat["significant_bonferroni"] = strat["p_value"] < 0.05 / len(strat)
display(strat.round(3).sort_values("difference"))

# COMMAND ----------

import statsmodels.formula.api as smf

model = smf.poisson("additive_count ~ is_organic + C(category)",
                    data=df.assign(is_organic=df.is_organic.astype(int))
                   ).fit(cov_type="HC0", disp=False)

rr = np.exp(model.params["is_organic"])
lo, hi = np.exp(model.conf_int().loc["is_organic"])
print(f"Organic vs non-organic, same category: rate ratio {rr:.2f} "
      f"(95% CI {lo:.2f}–{hi:.2f}), p = {model.pvalues['is_organic']:.2g}")

# COMMAND ----------

# MAGIC %md
# MAGIC The first model did not converge because some small categories contain only zero-additive products. Re-estimated on the 12 categories with at least 30 organic and 30 non-organic products.

# COMMAND ----------

comparable = strat["category"]   # the 12 categories from cell 4
df12 = df[df["category"].isin(comparable)]

model = smf.poisson("additive_count ~ is_organic + C(category)",
                    data=df12.assign(is_organic=df12.is_organic.astype(int))
                   ).fit(cov_type="HC0", disp=False, maxiter=200)

print("Converged:", model.mle_retvals["converged"])
rr = np.exp(model.params["is_organic"])
lo, hi = np.exp(model.conf_int().loc["is_organic"])
print(f"Organic vs non-organic, same category: rate ratio {rr:.2f} "
      f"(95% CI {lo:.2f}–{hi:.2f}), p = {model.pvalues['is_organic']:.2g}")