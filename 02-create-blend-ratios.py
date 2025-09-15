# %%
from pathlib import Path
from databricks.connect import DatabricksSession
from databricks import sql
from databricks.vector_search.index import VectorSearchIndex
from dotenv import load_dotenv
import os
import pandas as pd
from databricks.vector_search.client import VectorSearchClient

load_dotenv()

VS_ENDPOINT_NAME = os.getenv("VS_ENDPOINT_NAME")
VS_INDEX_NAME = os.getenv("VS_INDEX_NAME")
HTTP_PATH = os.getenv("HTTP_PATH")

print(VS_ENDPOINT_NAME, VS_INDEX_NAME)

# %%

vsc = VectorSearchClient(
    disable_notice=True,
    workspace_url=os.getenv("DATABRICKS_HOST"),
    personal_access_token=os.getenv("DATABRICKS_TOKEN"),
)
spark = (
    DatabricksSession.builder.profile("DEFAULT")
    .remote(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
    )
    .getOrCreate()
)
# %%
# help(VectorSearchClient)
# %%
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
# this is the new product query text
# QUERY_TEXT="""Fieldline Performance Neutral Road Running Shoe
#   - Navy pairs a athletic fit with recycled mesh for all season wear.
#   - Designed for running shoes, it features cushioned collar and ortholite insole for dependable performance.
#   - Expect neutral cushioning, responsive midsole, and heel-to-toe transition in a versatile navy colorway.
#   - Ideal for daily miles, tempo efforts, and recovery jogs.
#   - Fit notes: secure lockdown with responsive feel. Grippy outsole and easy on/off details support everyday use."""

data_path = Path(__file__).parent / "npf-data"
print(data_path)


def load_new_products(data_path=data_path):
    new_products_df = pd.read_csv(data_path / "new_apparel_footwear_products_5.csv")
    return new_products_df


new_products_df = load_new_products()
new_products_df = new_products_df.rename(columns={"product_id": "new_product_id"})


def process_new_products(
    new_products_df: pd.DataFrame,
    index: VectorSearchIndex,
    spark: DatabricksSession,
    num_results=3,
):
    all_results = []
    for _, new_product in new_products_df.iterrows():
        query_text = new_product["description"]
        new_product_id = new_product["new_product_id"]

        # Perform vector search for this new product's description
        results = index.similarity_search(
            query_text=query_text,
            columns=["product_id", "title", "description"],
            num_results=num_results,
        )

        # Build DataFrame for matches
        lookup_df = pd.DataFrame.from_dict(results["result"]["data_array"])
        lookup_df.columns = [
            "matched_product_id",
            "title",
            "description",
            "match_score",
        ]

        # Compute ai_similarity between new product description and each matched product's description
        similarity_scores = []
        for _, row in lookup_df.iterrows():
            result = spark.sql(
                f"SELECT ai_similarity('{row['description'].replace('\'', '\\\'')}', '{query_text.replace('\'', '\\\'')}') as ai_sim"
            )
            similarity_scores.append(result.collect()[0][0])

        lookup_df["ai_similarity"] = similarity_scores
        lookup_df["query_product_id"] = new_product_id
        lookup_df["query_description"] = query_text

        all_results.append(lookup_df)

    # Concatenate all results into a single DataFrame
    final_df = pd.concat(all_results, ignore_index=True)
    return final_df


lookup_df = process_new_products(new_products_df, index, spark, num_results=3)

# %%
# Normalize the values of the AI similarity by query product ID between 0 to 100.
lookup_df["blend_ratio"] = lookup_df.groupby("query_product_id")["ai_similarity"].transform(lambda x: x / x.sum())
lookup_df["blend_ratio"] = lookup_df["blend_ratio"] * 100
lookup_df
# %%
# Now compute the blend ratio based on ai_similarity by normalizing ai_similarity by query_product_id
lookup_df["blend_ratio"] = lookup_df["ai_similarity"] / lookup_df["ai_similarity"].sum()

spark.createDataFrame(lookup_df).write.format("delta").mode("overwrite").saveAsTable("main.npf_retail.lookup_blend_ratios")
# %%
# also write new products df to bricks
spark.createDataFrame(new_products_df).write.format("delta").mode("overwrite").saveAsTable("main.npf_retail.new_products")
# %%