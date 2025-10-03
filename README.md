# Mom's Flower Shop - dbt Project (formerly SDF Sample)

This project was the default sample project for SDF. This is a version ported to dbt. 

## Project Structure

The project contains data about:
1. **Customers** - Customer information from the mobile app
2. **Marketing campaigns** - Marketing campaign events and costs  
3. **Mobile in-app events** - User interactions within the mobile app
4. **Street addresses** - Customer address information

## dbt Project Layout

```
├── dbt_project.yml          # dbt project configuration
├── models/
│   ├── staging/             # Staging models (materialized as views)
│   │   ├── customers.sql
│   │   ├── inapp_events.sql
│   │   ├── marketing_campaigns.sql
│   │   ├── app_installs.sql
│   │   ├── app_installs_v2.sql
│   │   └── stg_installs_per_campaign.sql
│   └── analytics/           # Analytics models (materialized as tables)
│       └── agg_installs_and_campaigns.sql
├── seeds/                   # CSV seed files
│   ├── raw_customers.csv
│   ├── raw_addresses.csv
│   ├── raw_inapp_events.csv
│   └── raw_marketing_campaign_events.csv
└── tests/                   # Data quality tests (defined in schema.yml files)
```

## Database Configuration

This project uses the `internal analytics` (KW277..) profile with the following configuration:
- **Database**: RAW
- **Warehouse**: TRANSFORMING  
- **Schema**: moms_flower_shop_<your-name>
- **Role**: TRANSFORMER

__For deferring, use the production schema `moms_flower_shop`. This can be helpful for the compare changes demo__

## Getting Started

After updating your schema:

1. **Load seed data and build**:
   ```bash
   dbt build
   ```

## Model Lineage

```
Seeds (CSV files)
    ↓  
Staging Models (Views)
    ↓
Analytics Models (Tables)
```

## Troubleshooting

If you encounter issues:

1. **Profile not found**: Ensure the `ia_dev` profile exists in `~/.dbt/profiles.yml`
2. **Permission errors**: Verify the TRANSFORMER role has appropriate permissions
3. **Seed loading issues**: Check CSV file formatting and column names match model expectations
4. **Model compilation errors**: Review SQL syntax and ensure all referenced models exist

For more information, see the [dbt documentation](https://docs.getdbt.com/).

