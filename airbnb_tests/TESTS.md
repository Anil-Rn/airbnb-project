# Test Cases for Airbnb Dagster Pipeline

This document outlines the tests performed for each of the assets in the Airbnb Dagster pipeline.

## Listings Silver Tests

The following tests validate the transformation applied in the `listings_silver` asset:

### Test 1: `listing_id` Uniqueness
- **Description**: Ensure that the `listing_id` column is unique across the dataset.
- **Reasoning**: Each listing must have a unique identifier.

### Test 2: `listing_id` Non-null
- **Description**: Verify that the `listing_id` column contains no null values.
- **Reasoning**: `listing_id` is a critical column for downstream processes and must not contain null values.

### Test 3: `host_id` Non-null
- **Description**: Ensure that the `host_id` column has no null values.
- **Reasoning**: Every listing must be associated with a host.

### Test 4: Valid Room Types
- **Description**: Check if the `room_type` column only contains valid values: 'Entire home/apt', 'Private room', 'Shared room', 'Hotel room'.
- **Reasoning**: Invalid room types could introduce errors into downstream analytics.

### Test 5: `minimum_nights` Greater Than 0
- **Description**: Ensure that `minimum_nights` contains only values greater than 0.
- **Reasoning**: Listings with a minimum night stay of 0 should be corrected to 1.

## Hosts Silver Tests

The following tests validate the transformation applied in the `hosts_silver` asset:

### Test 1: `host_id` Uniqueness
- **Description**: Ensure that the `host_id` column is unique across the dataset.

### Test 2: `host_id` Non-null
- **Description**: Ensure that the `host_id` column contains no null values.

### Test 3: `host_name` Non-null
- **Description**: Ensure that the `host_name` column has no null values.

### Test 4: Valid `is_superhost` Values
- **Description**: Verify that the `is_superhost` column contains only valid values: 't' or 'f'.
- **Reasoning**: Invalid values in the `is_superhost` column could affect downstream analyses.

## Listings with Hosts Gold Tests

The following tests validate the transformation applied in the `listings_w_hosts_gold` asset:

### Test 1: `price` Numeric
- **Description**: Ensure that the `price` column contains only numeric values.

### Test 2: `price` Range
- **Description**: Check that the `price` column contains values within an expected range (e.g., less than 5000).

### Test 3: Row Count Consistency
- **Description**: Ensure the number of rows in the Gold table matches the number of rows in the Listings Silver table.
- **Reasoning**: If there is a mismatch in row count, it indicates issues with the data merging process.

### Test 4: Correct `host_id` in Gold Table
- **Description**: Verify that the `host_id` in the Gold table matches the `host_id` in the Hosts Silver table.
