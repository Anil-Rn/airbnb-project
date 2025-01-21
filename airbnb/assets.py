import pandas as pd
from dagster import asset
from datetime import datetime
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
bronze_dir = os.path.join(script_dir, 'data', 'bronze')
silver_dir = os.path.join(script_dir, 'data', 'silver')
gold_dir = os.path.join(script_dir, 'data', 'gold')

@asset
def hosts_silver():
    """Load and cleanse the Hosts data as a silver layer."""

    csv_file = os.path.join(bronze_dir, 'hosts_bronze.csv')
    hosts_df = pd.read_csv(csv_file)
    
    # Replace NULL host_name with 'Anonymous'
    hosts_df['host_name'] = hosts_df['name'].fillna('Anonymous')
    hosts_df = hosts_df[['id', 'host_name', 'is_superhost', 'created_at', 'updated_at']]
    
    # Rename 'id' column to 'host_id'
    hosts_df.rename(columns={'id': 'host_id'}, inplace=True)
    
    # Convert 'created_at' and 'updated_at' from string to datetime (naive timestamps)
    hosts_df['created_at'] = pd.to_datetime(hosts_df['created_at'], format='%d-%m-%y %H:%M')
    hosts_df['updated_at'] = pd.to_datetime(hosts_df['updated_at'], format='%d-%m-%y %H:%M')
    
    # Materialize (save as CSV)
    materialized_file = os.path.join(silver_dir, 'hosts_silver.csv')
    hosts_df.to_csv(materialized_file, index=False)

    return hosts_df


@asset
def listings_silver():
    """Load and cleanse the Listings data with hourly partitioning."""

    csv_file = os.path.join(bronze_dir, 'listings_bronze.csv')
    listings_df = pd.read_csv(csv_file)
    
    # Cleanse the listings data based on the provided logic
    listings_df['minimum_nights'] = listings_df['minimum_nights'].apply(lambda x: 1 if x == 0 else x)
    
    # Clean the 'price' column by removing symbols and converting to float
    listings_df['price'] = listings_df['price'].replace({'\\$': '', ',': ''}, regex=True).astype(float)
    listings_df = listings_df[['id', 'name', 'room_type', 'minimum_nights', 'host_id', 'price', 'created_at', 'updated_at']]

    # Rename 'id' column to 'listing_id' and 'name' to 'listing_name'
    listings_df.rename(columns={'id': 'listing_id', 'name': 'listing_name'}, inplace=True)
    
    # Convert 'created_at' and 'updated_at' to datetime
    listings_df['created_at'] = pd.to_datetime(listings_df['created_at'])
    listings_df['updated_at'] = pd.to_datetime(listings_df['updated_at'])
    
    # **Partitioning by hour based on the 'created_at' column**
    listings_df['hour_partition'] = listings_df['created_at'].dt.floor('h')  # Round down to the nearest hour
    
    # Materialize (save as CSV)
    materialized_file = os.path.join(silver_dir, 'listings_silver.csv')
    listings_df.to_csv(materialized_file, index=False)

    return listings_df


@asset
def listings_w_hosts_gold(listings_silver: pd.DataFrame, hosts_silver: pd.DataFrame):
    """Join Listings Silver with Hosts Silver to create the Gold table."""

    # Convert both 'updated_at' columns to timezone-naive (if they are timezone-aware)
    listings_silver['updated_at'] = listings_silver['updated_at'].dt.tz_localize(None)
    hosts_silver['updated_at'] = hosts_silver['updated_at'].dt.tz_localize(None)

    # Perform the merge operation on the 'host_id' column for both DataFrames
    merged_df = pd.merge(
        listings_silver,
        hosts_silver[['host_id', 'host_name', 'is_superhost', 'updated_at']],
        on='host_id', 
        how='left'
    )
    
    # Calculate the 'updated_at' column as the greatest of L.updated_at and H.updated_at
    merged_df['updated_at'] = merged_df[['updated_at_x', 'updated_at_y']].max(axis=1)
    merged_df = merged_df[['listing_id', 'listing_name', 'room_type', 'minimum_nights', 'price', 
                          'host_id', 'host_name', 'is_superhost', 'created_at', 'updated_at']]
    
    # Materialize (save as CSV)
    materialized_file = os.path.join(gold_dir, 'listings_w_hosts_gold.csv')
    merged_df.to_csv(materialized_file, index=False)

    return merged_df