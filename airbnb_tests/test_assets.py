import pytest
import pandas as pd
from dagster import AssetMaterialization
from airbnb.assets import listings_silver, hosts_silver, listings_w_hosts_gold


def test_listings_silver():

    listings_df = listings_silver()

    # Test 1: Check if 'listing_id' column is unique
    assert listings_df['listing_id'].is_unique, "'listing_id' should be unique"
    
    # Test 2: Check if 'listing_id' column is not null
    assert listings_df['listing_id'].notnull().all(), "'listing_id' should not contain null values"
    
    # Test 3: Check if 'host_id' is not null
    assert listings_df['host_id'].notnull().all(), "'host_id' should not contain null values"
    
    # Test 4: Check if 'room_type' contains only accepted values
    accepted_room_types = ['Entire home/apt', 'Private room', 'Shared room', 'Hotel room']
    assert listings_df['room_type'].isin(accepted_room_types).all(), "'room_type' contains invalid values"

    # Test 5: Check if 'minimum_nights' column contains only values greater than 0
    assert (listings_df['minimum_nights'] > 0).all(), "'minimum_nights' should only contain values greater than 0"
    
    # Test 6: Ensure the 'minimum_nights' query (that it doesn't have less than 1) returns no results
    invalid_min_nights = listings_df[listings_df['minimum_nights'] < 1]
    assert invalid_min_nights.empty, "'minimum_nights' contains values less than 1"


def test_hosts_silver():

    hosts_df = hosts_silver()

    # Test 1: Check if 'host_id' is unique
    assert hosts_df['host_id'].is_unique, "'host_id' should be unique"
    
    # Test 2: Check if 'host_id' is not null
    assert hosts_df['host_id'].notnull().all(), "'host_id' should not contain null values"
    
    # Test 3: Check if 'host_name' is not null
    assert hosts_df['host_name'].notnull().all(), "'host_name' should not contain null values"
    
    # Test 4: Check if 'is_superhost' contains only accepted values
    accepted_superhost_values = ['t', 'f']
    assert hosts_df['is_superhost'].isin(accepted_superhost_values).all(), "'is_superhost' contains invalid values"


def test_listings_w_hosts_gold():

    listings_silver_df = listings_silver()
    hosts_silver_df = hosts_silver()
    gold_df = listings_w_hosts_gold(listings_silver_df, hosts_silver_df)

    # Test 1: Check if the price column contains numeric values
    assert pd.api.types.is_numeric_dtype(gold_df['price']), "'price' column should contain numeric values"

    # Test 2: Ensure 'price' column values are within the expected max range
    assert gold_df['price'].max() <= 5000, "'price' should not exceed 5000"

    # Test 3: Check if the number of rows in gold table matches the listings_silver rows
    assert len(gold_df) == len(listings_silver_df), "Row count mismatch between listings_silver and listings_w_hosts_gold"

    # Test 4: Verify relationship between 'host_id' in listings_w_hosts_gold and hosts_silver
    merged_df = pd.merge(
        gold_df,
        hosts_silver_df[['host_id']],
        on='host_id',
        how='left'
    )
    assert len(merged_df) == len(gold_df), "There are 'host_id' values in gold that do not have a match in hosts_silver"
