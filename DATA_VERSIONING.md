# Data Versioning and Lineage

## Tools Used
- **DVC** (v3.66.1) for data versioning
- **Git** for metadata and code versioning

## Versioned Datasets

| Dataset | Path | Version | Source | Transformations |
|---------|------|---------|--------|-----------------|
| Raw clickstream | `data/raw/clickstream/clickstream_events.csv` | v1 | Internal logs | None (raw) |
| Raw ratings | `data/raw/products/metadata/ratings_Electronics (1).csv` | v1 | Amazon Electronics | None (raw) |
| Preprocessed | `data/processed/final_interactions.csv` | v1 | Raw data | Cleaning, deduplication |
| Feature-engineered | `data/processed/feature_engineered_data.csv` | v1 | Preprocessed data | User activity count, avg ratings |

## Lineage Workflow
Raw data → Preprocessing → Feature engineering → Model training

## Commands Used
```bash
dvc add data/raw/clickstream/clickstream_events.csv
dvc add "data/raw/products/metadata/ratings_Electronics (1).csv"
python src/feature_engineering.py
dvc add data/processed/feature_engineered_data.csv
git commit -m "feat(data): version feature-engineered data v1"