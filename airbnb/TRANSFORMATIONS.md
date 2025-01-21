# Data Transformations in the Airbnb Dagster Pipeline

This document outlines the transformations applied to the raw data as it moves through different layers (Bronze, Silver, Gold) in the Airbnb data pipeline.

## Hosts Silver Transformation

### Description:
The `hosts_silver` asset loads the raw Hosts data from the `hosts_bronze.csv` file and applies the following transformations:

- **Null Handling**: If the `host_name` column has missing values, it is replaced with "Anonymous".
- **Column Filtering**: The dataset is reduced to the following columns: `id`, `host_name`, `is_superhost`, `created_at`, `updated_at`.
- **Renaming**: The `id` column is renamed to `host_id`.
- **Date Parsing**: The `created_at` and `updated_at` columns are converted from string format to datetime (naive).

### Output:
A DataFrame with cleaned and transformed Hosts data.

---

## Listings Silver Transformation

### Description:
The `listings_silver` asset processes the raw Listings data from the `listings_bronze.csv` file and applies the following transformations:

- **Zero Minimum Nights**: Any listings with `minimum_nights` set to 0 are corrected to 1.
- **Price Cleaning**: The `price` column is stripped of non-numeric characters (like `$` and `,`) and converted to a float.
- **Column Filtering**: The dataset is reduced to the following columns: `id`, `name`, `room_type`, `minimum_nights`, `host_id`, `price`, `created_at`, `updated_at`.
- **Renaming**: The `id` column is renamed to `listing_id`, and `name` is renamed to `listing_name`.
- **Date Parsing**: The `created_at` and `updated_at` columns are converted to datetime.
- **Partitioning**: The data is partitioned by hour based on the `created_at` column.

### Output:
A DataFrame with cleaned and partitioned Listings data.

---

## Listings with Hosts Gold Transformation

### Description:
The `listings_w_hosts_gold` asset merges the transformed Listings data from `listings_silver` with the Hosts data from `hosts_silver` to create the Gold layer. It applies the following transformations:

- **Join**: The `listings_silver` DataFrame is merged with the `hosts_silver` DataFrame on the `host_id` column.
- **Handling of `updated_at`**: The `updated_at` column is calculated as the maximum of `updated_at_x` (from Listings) and `updated_at_y` (from Hosts).
- **Final Column Selection**: The dataset is reduced to the following columns: `listing_id`, `listing_name`, `room_type`, `minimum_nights`, `price`, `host_id`, `host_name`, `is_superhost`, `created_at`, and `updated_at`.
- **Partitioning**: The data is partitioned by hour based on the `created_at` column.

### Output:
A fully enriched DataFrame with combined Listings and Hosts data, ready for downstream use.

---

## Notes:
- The transformations preserve the integrity of the data by applying business rules and ensuring correct formatting.
- The data is partitioned by hour in the `listings_silver` step, which helps in time-based analyses.
