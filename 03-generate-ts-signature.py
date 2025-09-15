# %%
from databricks.connect import DatabricksSession
import os
import pandas as pd
from dotenv import load_dotenv
from rich import print
import warnings

from utils.funcs import get_shape_sql, run_checks_for_all_products
from utils.funcs import abs_week_to_month
from utils.funcs import normalize_month_with_year_transition
from utils.funcs import calculate_blended_signature


warnings.filterwarnings("ignore")
load_dotenv()


spark = (
    DatabricksSession.builder.profile("DEFAULT")
    .remote(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
    )
    .getOrCreate()
)

match_df = spark.sql("SELECT * FROM main.npf_retail.lookup_blend_ratios")
print(match_df.show())
# %%
df = match_df.toPandas()

# %%
# The next step is to ensure that for the query product ID, the matched products are in the same category and subcategory.
# This can be a Python function that returns true or false.
# product catalog is in main.npf_retail.product_catalog
# new products is in main.npf_retail.new_products


# Run checks for all products
run_checks_for_all_products(df, spark)
# %%

# Time series Signature generation for the shape of the new product forecast
# The next step is to generate a signature for each product.
# The signature is a string of the form:

shape_df = spark.sql(get_shape_sql("NP101"))
# %%
shape_pdf = shape_df.toPandas()
shape_pdf['min_launch_month'] = shape_pdf['launch_abs_week'].apply(lambda x: abs_week_to_month(x, 2024))

shape_pdf['normalized_month'] = shape_pdf.apply(
    lambda row: normalize_month_with_year_transition(row['launch_month'], row['min_launch_month']), 
    axis=1
)

# Now based on the normalized month, we start to generate the signature
# To accomplish this we must first take the individual product unit's actual month and divide by the unit's actual six month target.
shape_pdf['percent_of_6m_target'] = shape_pdf['units_actual_month'] / shape_pdf['units_actual_6m_target']
shape_pdf['weighted_pct'] = shape_pdf['percent_of_6m_target'] * (shape_pdf['blend_ratio'] / 100)

blended = shape_pdf.groupby('normalized_month')['weighted_pct'].sum().reset_index()

blended.columns = ['normalized_month', 'blended_pct_of_6m']
# to get the signature we need to normalize the blended percentage of 6m by the sum of the blended percentage of 6m
blended['blended_pct_of_6m_normalized'] = blended['blended_pct_of_6m']/blended['blended_pct_of_6m'].sum()



# Calculate the signature for each product
product_shape_signatures = pd.DataFrame()
for query_product_id in df['query_product_id'].unique():
    signature = calculate_blended_signature(spark, query_product_id)
    signature['query_product_id'] = query_product_id
    product_shape_signatures = pd.concat([product_shape_signatures, signature])
    print(f"Calculated signature for {query_product_id}")
    print(signature)

product_shape_signatures.head()

print("write product shape signatures to bricks")
spark.createDataFrame(product_shape_signatures).write.format("delta").mode("overwrite").saveAsTable("main.npf_retail.product_shape_signatures")

# %%

# %%