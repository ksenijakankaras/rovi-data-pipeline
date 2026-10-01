# Metric definitions

Every metric is defined once, here and in SQL views (`notebooks/04_metrics.py`). The dashboard and the analysis read from the views, so a number means the same thing everywhere.

| Metric | Definition | Filter | View | Why it matters |
|---|---|---|---|---|
| **Ingredient coverage** | Products with an ingredient list ÷ all products | None | `v_kpi_summary`, `v_category_metrics` | How often the app can explain a scanned product |
| **Additive density** | Average number of distinct additives per product | Products with an ingredient list | `v_kpi_summary`, `v_category_metrics` | Products without a list show 0 additives because the information is missing, not because they have none |
| **Appearance-only share** | % of products with at least one additive whose primary purpose is appearance (colours, glazing agents) | Products with an ingredient list | `v_kpi_summary`, `v_category_metrics` | Additives with no safety or freshness role |
| **EU-warning colour share** | % of products containing one of the six colours that must carry an EU label warning (E102, E104, E110, E122, E124, E129) | Products with an ingredient list | `v_category_metrics` | Tied directly to a legal label requirement |
| **Category gap** | % of products with an ingredient list but no category | Products with an ingredient list | `v_kpi_summary` | Target group for a category-prediction model |
| **Data quality pass rate** | Checks passing ÷ all checks in the latest run | Latest run in `dq_results` | `v_kpi_summary` | Whether the numbers above can be trusted today |

## Additive purpose categories

Each additive gets one primary purpose from its EU function classes. When an additive has several functions, the first match in this order wins: safety → texture → taste → appearance → other. Five common multi-function additives use a documented override (see `PURPOSE_OVERRIDES` in `02_gold_star_schema.py`).

| Purpose | Function classes |
|---|---|
| Safety | preservatives, antioxidants, acidity regulators, sequestrants, packaging gases |
| Texture | emulsifiers, stabilisers, thickeners, gelling and raising agents, humectants, carriers |
| Taste | sweeteners, flavour enhancers, acids |
| Appearance | colours, glazing agents |

## Pattern used in the views

`AVG(CASE WHEN condition THEN value END)` averages only over rows where the condition is true, because non-matching rows become NULL and `AVG` ignores NULLs. This lets each metric in one query use its own filter.
