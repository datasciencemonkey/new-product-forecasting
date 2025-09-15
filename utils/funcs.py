import pandas as pd
from databricks.connect import DatabricksSession


def get_shape_sql(product_id):
    return f"""
    select b.*, a.launch_abs_week, a.units_actual_6m_target,
    c.blend_ratio
    from main.npf_retail.scale_modeling_features a
    join main.npf_retail.monthly_actuals_from_launch b
    on a.item_id=b.item_id 
    join main.npf_retail.lookup_blend_ratios c 
    on a.item_id = c.matched_product_id where c.query_product_id="{product_id}"
    """


def abs_week_to_month(abs_week, year=2024):
    """
    Convert absolute week number to month.
    
    Args:
        abs_week (int): Absolute week number (1-53)
        year (int): Year for reference (default: 2024)
    
    Returns:
        int: Month number (1-12)
    """
    from datetime import datetime, timedelta
    
    # Calculate the date for the given absolute week
    # Week 1 starts on January 1st
    jan_1 = datetime(year, 1, 1)
    
    # Calculate days from start of year (week 1 = day 0, week 2 = day 7, etc.)
    days_from_start = (abs_week - 1) * 7
    
    # Get the actual date
    target_date = jan_1 + timedelta(days=days_from_start)
    
    # Return the month
    return target_date.month



def _generate_query_sql(query_product_id):
    return f"""
    with new_product_cat_subcat as (
  (
    select
      category,
      subcategory
    from
      main.npf_retail.new_products
    where
      new_product_id = '{query_product_id}'
  )
),
product_catalog_cat_subcat as (
  select
    category,
    subcategory
  from
    main.npf_retail.product_catalog
  where
    product_id in (
      select
        matched_product_id
      from
        main.npf_retail.lookup_blend_ratios
      where
        query_product_id = '{query_product_id}'
    )
)
select
  count(distinct new_product_cat_subcat.category) = count(distinct
    product_catalog_cat_subcat.category
  ) product_category_count_match,
  count(distinct new_product_cat_subcat.subcategory) = count(distinct
    product_catalog_cat_subcat.subcategory
  ) product_subcategory_count_match,
  case
    when new_product_cat_subcat.category = product_catalog_cat_subcat.category then True
    else False
  end as category_match,
  case
    when new_product_cat_subcat.subcategory = product_catalog_cat_subcat.subcategory then True
    else False
  end as subcategory_match
from
  new_product_cat_subcat
    join product_catalog_cat_subcat
      on new_product_cat_subcat.category = product_catalog_cat_subcat.category
      and new_product_cat_subcat.subcategory = product_catalog_cat_subcat.subcategory
group by
  category_match,
  subcategory_match;
    """


def run_checks_for_all_products(
    match_df: pd.DataFrame, 
    spark: DatabricksSession
):
    """
    Run checks for all products in the match dataframe.

    Args:
        match_df (pd.DataFrame): DataFrame containing product matches, must have 'query_product_id' column.
        spark (DatabricksSession): Spark session to execute SQL queries.

    Returns:
        list: List of tuples (query_product_id, passed) where passed is True if all checks pass.
    """
    results = []
    for query_product_id in match_df["query_product_id"].unique():
        check_df = spark.sql(_generate_query_sql(query_product_id))
        test_df = check_df.toPandas()
        # Check if all boolean columns in the first row are True
        if not test_df.empty:
            passed = test_df.iloc[0].astype(bool).all()
        else:
            passed = False
        results.append((query_product_id, passed))
        if passed:
            print(f"✅ All checks passed for {query_product_id}")
        else:
            print(f"❌ Some checks failed for {query_product_id}")
    return results




def normalize_month_with_year_transition(launch_month, min_launch_month):
    """
    Normalize month values accounting for year transitions.
    
    Args:
        launch_month (int): The month to normalize (1-12)
        min_launch_month (int): The minimum launch month (1-12)
    
    Returns:
        int: Normalized month (0-based, where 0 = launch month)
    """
    # Handle year transition: if we go from Dec (12) to Jan (1), 
    # or from Nov (11) to Jan (1), we need to add 12
    if launch_month < min_launch_month and min_launch_month - launch_month > 6:
        # This indicates a year transition (e.g., Dec=12 to Jan=1)
        return launch_month + 12 - min_launch_month
    else:
        # Normal case within the same year
        return launch_month - min_launch_month


def normalize_time_series_by_launch(df):
    """
    Normalize time series data by launch month, handling year transitions.
    
    This function applies the same month normalization logic as used in the shape pattern
    generation, ensuring that products launching across year boundaries (e.g., Nov->Dec->Jan)
    are properly normalized without negative values.
    
    Args:
        df (pd.DataFrame): DataFrame with time series data containing:
            - item_id: Product identifier
            - launch_month: Month of launch (1-12)
            - launch_abs_week: Absolute week of launch
            - units_actual_month: Actual units sold in that month
    
    Returns:
        pd.DataFrame: DataFrame with additional columns:
            - actual_launch_month: The actual launch month (from abs_week_to_month)
            - normalized_month: Month normalized relative to minimum launch month (0-based)
    """
    # Convert to pandas if it's a Spark DataFrame
    if hasattr(df, 'toPandas'):
        df_pandas = df.toPandas()
    else:
        df_pandas = df.copy()
    
    # Calculate the actual launch month from absolute week
    df_pandas['actual_launch_month'] = df_pandas['launch_abs_week'].apply(
        lambda x: abs_week_to_month(x, 2024)
    )
    
    # Find the minimum launch month for each item
    df_pandas['min_launch_month'] = df_pandas.groupby('item_id')['actual_launch_month'].transform('min')
    
    # Apply the year-transition-aware normalization
    df_pandas['normalized_month'] = df_pandas.apply(
        lambda row: normalize_month_with_year_transition(
            row['actual_launch_month'], 
            row['min_launch_month']
        ), 
        axis=1
    )
    
    return df_pandas




def calculate_blended_signature(spark, shape_id, base_year=2024):
    """
    Calculate blended signature percentages for a given shape.
    
    Parameters:
    -----------
    spark : SparkSession
        The Spark session for SQL queries
    shape_id : str
        The shape identifier (e.g., "NP101")
    base_year : int, optional
        Base year for week-to-month conversion (default: 2024)
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with columns: 'normalized_month', 'blended_pct_of_6m', 'blended_pct_of_6m_normalized'
    """
    # Get shape data from SQL
    shape_df = spark.sql(get_shape_sql(shape_id))
    
    # Convert to Pandas
    shape_pdf = shape_df.toPandas()
    
    # Add min_launch_month column
    shape_pdf['min_launch_month'] = shape_pdf['launch_abs_week'].apply(
        lambda x: abs_week_to_month(x, base_year)
    )
    
    # Normalize months with year transition
    shape_pdf['normalized_month'] = shape_pdf.apply(
        lambda row: normalize_month_with_year_transition(
            row['launch_month'], 
            row['min_launch_month']
        ), 
        axis=1
    )
    
    # Calculate percent of 6-month target
    shape_pdf['percent_of_6m_target'] = (
        shape_pdf['units_actual_month'] / shape_pdf['units_actual_6m_target']
    )
    
    # Calculate weighted percentage using blend ratio from the shape data itself
    shape_pdf['weighted_pct'] = (
        shape_pdf['percent_of_6m_target'] * (shape_pdf['blend_ratio'] / 100)
    )
    
    # Group by normalized month and sum weighted percentages
    blended = shape_pdf.groupby('normalized_month')['weighted_pct'].sum().reset_index()
    blended.columns = ['normalized_month', 'blended_pct_of_6m']
    
    # Normalize the blended percentages to create the signature
    blended['blended_pct_of_6m_normalized'] = (
        blended['blended_pct_of_6m'] / blended['blended_pct_of_6m'].sum()
    )
    
    return blended
