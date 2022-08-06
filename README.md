# 🏡 Clickhouse-Transform (⚠️ Experimental)


Build custom transformation pipelines for Clickhouse using standard SQL or an intuitive and expressive dataframe API.

Clickhouse-Transform uses the [Ibis-Project](https://ibis-project.org/docs/3.0.2/ibis-for-sql-programmers/) as the
DataFrame API. See their usage guides for a detailed overview of how to create expressions with Ibis.

## Installation

```shell
pip install clickhouse_transform
```

## Usage

### Building lazily evaluated query execution models

```python
from clickhouse_transform import Session

configs = {...}

session = Session.builder.from_configs(configs).create()

opportunities = session.table(database="crm_db", table="opportunities")
lost_opportunities = opportunities.filter(opportunities.stage == "Closed: lost")
top_opportunity_losers = lost_opportunities.groupby("owner_id").size()

# retrieve results as Pandas DataFrame
df = top_opportunity_losers.execute()
```

### Creating execution pipelines

```python
from clickhouse_transform import Session, Pipeline
from clickhouse_transform.types import Model

configs = {...}

session = Session.builder.from_configs(configs).create()
pipeline = Pipeline(session)

pipeline.add_source(database="sales", table="purchases")
pipeline.add_source(database="sales", table="customers")

EU_COUNTRY_ISO_CODES = ["GBR", "DEU", "FRA", "ITA"]


@pipeline.model()
def eu_purchases(sales_purchases: Model) -> Model:
    return sales_purchases.filter(sales_purchases.region.isin(EU_COUNTRY_ISO_CODES))


@pipeline.model()
def eu_customers(sales_customers: Model) -> Model:
    return sales_customers.filter(sales_customers.region.isin(EU_COUNTRY_ISO_CODES))


@pipeline.model()
def purchases_with_customer_info(eu_purchases: Model, eu_customers: Model) -> Model:
    return (
        eu_purchases
        .left_join(eu_customers, eu_purchases.customer_id == eu_customers.id)
        .select([
            eu_purchases.purchase_amount,
            eu_customers.id.name("customer_id"),
            eu_customers.first_name.concat(" ", eu_customers.last_name).name("customer_full_name"),
            eu_customers.email
        ])
    )


@pipeline.model()
def customers_lifetime_spend(purchases_with_customer_info: Model) -> Model:
    return (
        purchases_with_customer_info
        .groupby(["customer_id", "customer_full_name", "email"])
        .aggregate(purchases_with_customer_info.purchase_amount.sum().name("lifetime_spend"))
    )


expr = pipeline.run()
df = expr.execute()
```

