# 🔄 dbt Transformations — Price Intelligence Platform

Modèles dbt pour les transformations BigQuery.

## Modèles

price_raw.price_events (source BigQuery)
↓
staging/stg_prices      (view — cast types)
↓
cleaned/clean_prices    (table — déduplication)
↓
aggregated/agg_prices   (table — min/max/avg/change%)
↓
marts/price_timeseries  (table — séries temporelles)
marts/price_intelligence (table finale — jointure)

## Installation

```bash
pip install dbt-bigquery==1.5.3
```

## Configuration

```yaml
# profiles.yml
price_intel:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: price-intel-prod
      dataset: price_intelligence
      location: EU
      keyfile: "/path/to/price-key.json"
```

## Commandes

```bash
# Tester la connexion
dbt debug --profiles-dir .

# Compiler sans exécuter
dbt compile --profiles-dir .

# Exécuter tous les modèles
dbt run --profiles-dir .

# Exécuter un modèle spécifique
dbt run --select stg_prices --profiles-dir .

# Tester les modèles
dbt test --profiles-dir .

# Générer la documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## CI/CD

GitHub Actions — `.github/workflows/dbt-ci.yml`

Jobs :
- `validate-dbt` — vérifie structure + syntaxe SQL
- `lint-sql` — sqlfluff BigQuery dialect