# Mom's Flower Shop - dbt Project (formerly SDF Sample)

This project was the default sample project for SDF. This is a version ported to dbt. 

## Project Structure

The project contains data about Mom's Flower Shop, including:
1. **Customers** - Customer information from the mobile app
2. **Marketing campaigns** - Marketing campaign events and costs  
3. **Mobile in-app events** - User interactions within the mobile app
4. **Street addresses** - Customer address information

## Project Configuration

This project is meant to be used in the Mom's Flower Shop dbt Platform Sandbox Account for Coalesce attendees. The dbt Platform Sandbox Account will be live for 7 days and will be preconfigured to connect to a Sandbox data platform account and this Github repo. No additional dbt Platform account configuration is required to leverage this project.

## Analytics Models Organization

The analytics layer is split into two directories based on complexity:

### `analytics_new/` - Basic Analytics (7 models)
Foundational, straightforward analytics models with simple aggregations:
- **Daily & Time-based Metrics**: Install aggregations, DAU/WAU/MAU, hourly patterns
- **Performance Basics**: Campaign rankings, platform metrics, geographic analysis
- **Executive Views**: Simple dashboard combining key metrics

These models use basic CTEs and aggregations, perfect for getting started or learning dbt.

### `analytics/` - Advanced Analytics (14 models)
Complex analytical models with sophisticated business logic:
- **Customer Analytics**: LTV, cohort retention, segmentation (RFM), 360° view, journey timing
- **Campaign Analytics**: Performance summary, comparison, ROI, acquisition cost, attribution
- **Behavioral Analytics**: Event funnels, session analysis, product affinity, engagement scoring
- **Revenue Analytics**: Weekly/monthly trends, repeat purchase analysis, churn risk
- **Special Features**: 
  - Incremental materialization (`user_engagement_score`)
  - Ephemeral models (`executive_kpi_summary`)

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

