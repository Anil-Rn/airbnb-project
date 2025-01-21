# Airbnb Dagster Project

This is a data pipeline project built using [Dagster](https://dagster.io/) for processing and transforming Airbnb data. The project includes multiple stages of data processing from raw data ingestion to final business insights.

## Project Overview

This project processes raw Airbnb listings and host data, cleanses and transforms it, and materializes the results into refined datasets. It focuses on implementing data pipelines using Dagster assets and organizing the data into different layers (Bronze, Silver, Gold).

1. **Bronze Layer**: Raw data from CSV files in `airbnb/data/bronze/` folder.
2. **Silver Layer**: Cleaned and transformed data, with additional fields and partitioning applied in `airbnb/data/silver/` folder.
3. **Gold Layer**: Fully enriched dataset, combining both listings and hosts data, ready for analytics or reporting in `airbnb/data/gold/` folder..

### Core Data Assets
- **Hosts Data (Silver)**: Cleansed data with proper transformations and handling of missing values.
- **Listings Data (Silver)**: Cleaned data with partitioning by hour and other business rules.
- **Gold Layer**: A joined dataset of Listings and Hosts, providing a full, enriched dataset for downstream use.

The project is designed to support both development and deployment on Dagster Cloud, with unit tests to ensure the accuracy of the transformations.

### Dataset Source

The raw Airbnb data used in this project is sourced from the publicly available [Inside Airbnb](https://insideairbnb.com/berlin/) dataset, specifically for the city of Berlin. This dataset contains information on listings, hosts, and reviews, which are used for various analyses and transformations throughout the project.

## Getting Started

### Prerequisites

Ensure you have Python 3.8+ installed on your system. You will also need to install Dagster and other project dependencies.

### Installing Dependencies

First, install the required dependencies for the project in "editable mode" to allow for real-time updates to the code:

```bash
pip install -e ".[dev]"
```

### Run and materialize the assets

First, navigate to the project root folder and execute the following command:

```bash
  dagster dev
```
Then visit the [Dagster UI](http://localhost:3000/) to view the data lineage and materialize the tables.

**Note:** assets.py script responsible for creating assets can be found in PATH: `airbnb/assets.py`

### Test the assets for data quality

```bash
  pytest airbnb_project\airbnb_tests\test_assets.py
```