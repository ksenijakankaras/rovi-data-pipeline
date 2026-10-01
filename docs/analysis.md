# Which product categories should Rovi explain first?

*Analysis of 266,749 Italian products from Open Food Facts*

## Summary

Rovi should build its additive explanations first for **snacks and plant-based foods**,
which together account for 52.5% of additive occurrences among categorised Italian
products (snacks alone: 34.8%), followed by **meats, desserts and drink preparations**,
which contain the additives that most need context: preservatives such as nitrites in
cured meats, and colours used only for appearance in desserts and drinks. Additive use
is highly concentrated: the 20 most common additives make up 60.3% of all occurrences
out of 355 in use, so a small curated knowledge base covers most of what users will
scan. However, only 13.6% of products have an ingredient list, so a label-photo fallback
is essential for the app to be useful in stores.

## Data and method

- **Pipeline:** Open Food Facts export processed in Databricks (bronze → silver → gold),
  modelled as a star schema (2 fact and 4 dimension tables). 11 automated data quality
  checks, all passing.
- **Classification:** each additive assigned a primary purpose (safety, texture, taste,
  appearance) from its EU function classes, plus a documented override list for five
  multi-function additives such as lecithin.
- **Metrics:** defined once as SQL views (see `docs/metrics.md`).
- **Statistics:** Mann-Whitney tests per category with Bonferroni correction; Poisson
  regression with category fixed effects and robust standard errors.

## Findings

1. **Ingredient data is the main constraint.** Only 13.6% of products (36,277) have an
   ingredient list. Verified at source: no recoverable ingredient data was found elsewhere
   in the export.
2. **Additives are concentrated in a few categories and a few additives.** Products
   average 1.09 additives. Snacks combine high density (2.03) with high volume and account
   for 34.8% of all additive occurrences; plant-based foods have low density (0.53) but
   are so numerous that they add another 17.7%. Four categories cover 66.1% of
   occurrences, and the top 20 of 355 additives cover 60.3%.
3. **Appearance-only additives are uncommon overall but clustered.** They appear in 4.5% of
   products, but in about 19% of desserts, 14% of supplements and 13% of drink
   preparations. No colour appears among the 20 most common additives.
4. **Organic products contain fewer additives, even within the same category.** The simple
   comparison shows 0.40 vs 1.17 additives per product (−66%). Controlling for category,
   organic products have 58% fewer (rate ratio 0.42, 95% CI 0.39–0.45). Organic is lower
   in all 12 comparable categories, and significantly so in 9 after Bonferroni correction.
   Part of the simple gap reflects category mix, since organic products cluster in
   low-additive categories such as plant-based foods. In both groups the median is zero:
   the difference lies in the minority of heavily processed products.

## Recommendation

1. **Volume first:** write knowledge-base entries for the top 20 additives, then for the
   additives most common in snacks and plant-based foods.
2. **Context second:** add category-specific explanations for meats (e.g. why nitrites
   are used in cured meat) and for appearance-only additives in desserts and drinks.
3. Prioritise label-photo scanning in the product roadmap, since barcode lookup alone
   covers too few products.
4. Use the organic comparison as educational content, presented as information rather
   than a rating.
   
## Limitations

- Open Food Facts is crowd-sourced. Counts reflect products listed, not products sold.
- Products with an ingredient list may differ systematically from those without.
- Organic status relies on labels. Unlabelled organic products are counted as
  non-organic, which would understate the difference.
- All results are associations, not causal effects.
- Category shares exclude products without a category.

## Next steps

- Predict categories for the 6.9% of analysable products that have none.
- Once the app has users, weight priorities by what people actually scan rather than by
  what exists in the database.
