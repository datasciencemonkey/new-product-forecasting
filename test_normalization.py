#!/usr/bin/env python3
"""
Test script to verify the normalize_time_series_by_launch function works correctly.
This simulates the data structure from the image provided by the user.
"""

import pandas as pd

def normalize_time_series_by_launch(df):
    """
    Normalize time series data by identifying the actual launch month for each item_id
    and creating a normalized month sequence where launch month = 0, next month = 1, etc.
    
    Args:
        df (pd.DataFrame): DataFrame with columns including 'item_id', 'launch_month', 'launch_abs_week'
    
    Returns:
        pd.DataFrame: DataFrame with added 'normalized_month' column
    """
    df = df.copy()
    
    # For each item_id, find the minimum launch_month (actual launch month)
    launch_months = df.groupby('item_id')['launch_month'].min().reset_index()
    launch_months.columns = ['item_id', 'actual_launch_month']
    
    # Merge back to get actual launch month for each row
    df = df.merge(launch_months, on='item_id')
    
    # Calculate normalized month: current_month - actual_launch_month
    df['normalized_month'] = df['launch_month'] - df['actual_launch_month']
    
    # Handle year boundaries (e.g., if launch is month 11 and we have months 12, 1, 2, 3)
    # For months that are less than the launch month, they're likely from the next year
    year_boundary_mask = df['launch_month'] < df['actual_launch_month']
    df.loc[year_boundary_mask, 'normalized_month'] = df.loc[year_boundary_mask, 'launch_month'] + 12 - df.loc[year_boundary_mask, 'actual_launch_month']
    
    return df

# Create test data based on the image provided
test_data = {
    'item_id': ['P1000', 'P1000', 'P1000', 'P1000', 'P1000', 'P1000',
                'P1001', 'P1001', 'P1001', 'P1001', 'P1001', 'P1001',
                'P1003', 'P1003', 'P1003', 'P1003', 'P1003'],
    'launch_month': [5, 6, 7, 8, 9, 10,  # P1000: months 5-10
                     10, 11, 12, 1, 2, 3,  # P1001: months 10, 11, 12, 1, 2, 3 (year boundary)
                     11, 12, 1, 2, 3],    # P1003: months 11, 12, 1, 2, 3 (year boundary)
    'launch_abs_week': [21, 21, 21, 21, 21, 21,  # P1000: all have week 21
                        41, 41, 41, 41, 41, 41,  # P1001: all have week 41
                        45, 45, 45, 45, 45],     # P1003: all have week 45
    'units_actual_month': [83, 666, 361, 134, 195, 176,  # P1000 units
                           442, 331, 530, 183, 26, 109,   # P1001 units
                           245, 398, 93, 156, 234],       # P1003 units
    'promo_weeks': [0, 1, 0, 0, 0, 1,  # P1000 promo weeks
                    0, 2, 2, 1, 1, 1,  # P1001 promo weeks
                    1, 0, 2, 1, 0]     # P1003 promo weeks
}

df = pd.DataFrame(test_data)

print("Original test data:")
print(df)

print("\n" + "="*60)
print("APPLYING NORMALIZATION")
print("="*60)

# Apply normalization
normalized_df = normalize_time_series_by_launch(df)

print("\nNormalized data:")
print(normalized_df[['item_id', 'launch_month', 'actual_launch_month', 'normalized_month', 'units_actual_month']])

print("\n" + "="*60)
print("VERIFICATION BY ITEM_ID")
print("="*60)

# Verify the normalization for each item_id
for item_id in normalized_df['item_id'].unique():
    item_data = normalized_df[normalized_df['item_id'] == item_id][
        ['launch_month', 'actual_launch_month', 'normalized_month', 'units_actual_month']
    ].sort_values('launch_month')
    
    print(f"\n{item_id}:")
    print(f"  Actual launch month: {item_data['actual_launch_month'].iloc[0]}")
    print(f"  Month sequence: {item_data['launch_month'].tolist()}")
    print(f"  Normalized sequence: {item_data['normalized_month'].tolist()}")
    print(f"  Units: {item_data['units_actual_month'].tolist()}")
    
    # Verify that normalized months start from 0 and are sequential
    normalized_months = sorted(item_data['normalized_month'].unique())
    expected_months = list(range(len(normalized_months)))
    
    if normalized_months == expected_months:
        print(f"  ✅ Normalization correct: {normalized_months}")
    else:
        print(f"  ❌ Normalization incorrect: got {normalized_months}, expected {expected_months}")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print("The normalization function should:")
print("1. Identify the minimum launch_month for each item_id as the actual launch month")
print("2. Set the actual launch month as normalized_month = 0")
print("3. Set subsequent months as 1, 2, 3, etc.")
print("4. Handle year boundaries correctly (e.g., 11→0, 12→1, 1→2, 2→3)")
