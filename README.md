# Mom's Flower Shop - dbt Project
```
                    _
                  _(_)_                          wWWWw   _
      @@@@       (_)@(_)   vVVVv     _     @@@@  (___) _(_)_
     @@()@@ wWWWw  (_)\    (___)   _(_)_  @@()@@   Y  (_)@(_)
      @@@@  (___)     `|/    Y    (_)@(_)  @@@@   \|/   (_)\
       /      Y       \|    \|/    /(_)    \|      |/      |
    \ |     \ |/       | / \ | /  \|/       |/    \|      \|/
    \\|//   \\|///  \\\|//\\\|/// \|///  \\\|//  \\|//  \\\|// 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

This project was the default sample project for SDF, now available to use with the dbt Fusion Engine. 

This git repo is used in the Mom's Flower Shop dbt Platform Sandbox Account for Coalesce attendees to get hands on with the dbt Fusion Engine!

The project contains data about Mom's Flower Shop, including:
1. **Customers** - Customer information
2. **Marketing campaigns** - Marketing campaign events and costs  
3. **Website events** - User interactions within the website
4. **Street addresses** - Customer address information
5. **Flower Shop Orders** - Flower, Chocolates, and Vase orders from Mom's Flower Shop! 

## Project Models Organization

### `staging`
Staging models live in `models/staging/`. These are lightweight views, prefixed with `stg_` that clean and standardize source data for reuse by downstream models.

### `analytics`
Analytics models live in `models/analytics/`. These models consume staging outputs and produce business-facing tables and views (aggregations, weekly/monthly trends, campaign summaries). They may be materialized as tables, incremental models, or ephemeral models depending on performance and use case.


## Model Lineage

```
Raw (source tables)
    ↓  
Staging Models (Views with stg_ prefix)
    ↓
Analytics 
```

## Troubleshooting

If you encounter issues, please reach out to dbt Support at support@getdbt.com!

For more information, see the [dbt documentation](https://docs.getdbt.com/).

