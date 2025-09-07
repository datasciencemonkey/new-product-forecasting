# %%
import pandas as pd
from databricks.connect import DatabricksSession
from pathlib import Path

# %%
# Initialize Spark session
spark = (
    DatabricksSession.builder.profile("DEFAULT").remote(serverless=True).getOrCreate()
)

data_path = Path(__file__).parent / "npf-data"

# %%
# 1. Load product catalog
products_df = pd.read_csv(data_path / "Synthetic_Apparel___Footwear_Products__50_.csv")
spark_products = spark.createDataFrame(products_df)
spark_products.write.format("delta").mode("overwrite").saveAsTable(
    "main.npf_retail.product_catalog"
)
print(f"✅ Loaded {len(products_df)} products")

# %%
# 2. Load monthly actuals
actuals_df = pd.read_csv(data_path / "actuals_monthly_6m_hist.csv")
spark_actuals = spark.createDataFrame(actuals_df)
spark_actuals.write.format("delta").mode("overwrite").saveAsTable(
    "main.npf_retail.monthly_actuals"
)
print(f"✅ Loaded {len(actuals_df)} monthly actuals")

# %%
# 3. Load scale modeling features
scale_df = pd.read_csv(data_path / "scale_modeling_from_monthlies.csv")
spark_scale = spark.createDataFrame(scale_df)
spark_scale.write.format("delta").mode("overwrite").saveAsTable(
    "main.npf_retail.scale_modeling_features"
)
print(f"✅ Loaded {len(scale_df)} scale modeling records")

# %%
# Enable Change Data Feed for all tables
# tables = [
#     "main.npf_retail.product_catalog",
#     "main.npf_retail.monthly_actuals",
#     "main.npf_retail.scale_modeling_features"
# ]

spark.sql(
        "ALTER TABLE main.npf_retail.product_catalog SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )

print("\n✅ Data ingestion completed!")
# %%
