

# NPF Retail - New Product Forecasting

Forecast sales for new apparel and footwear products using similarity-based modeling and historical data.

## Quick Start

```bash
# Install dependencies
uv sync

# Load data to Databricks
uv run python 01-load-data-to-bricks.py
```

## Data

- **Product Catalog**: 50 synthetic apparel/footwear products with features
- **Monthly Actuals**: 6-month historical sales data
- **Scale Modeling**: Features for forecasting (brand strength, marketing, etc.)

## PATH TO THE SOLUTION

- [ ] Load required data into Databricks
- [ ] Generate embeddings and sync endpoint to VS Endpoint
- [ ] Optionally write back embeddings to another table
- [ ] Compute the blend ratios based on similarities and generate signatures for new products
- [ ] Model for scale based on observed actuals
- [ ] Mix steps 4 & 5 for an estimated forecast
- [ ] Optionally, present this using Genie & Agent Bricks
