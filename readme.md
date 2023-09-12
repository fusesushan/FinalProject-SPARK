# Spark Business Analytics Project

This project is a Spark-based business analytics and data processing project that analyzes hourly gasoline prices and fuel station information. It includes various data transformations and calculations to provide insights into fuel price variations and nearest competitor price differences.

## Project Overview

The project consists of the following main components and analyses:
1. **Data preprocess**: The project reads the csv files and then conver it to parquet files after cleaning. Then it reads the parquet files and writes to the database.

2. **Database Connection**: The project connects to a PostgreSQL database to retrieve two main datasets: `hourly_gasoline_prices` and `fuel_station_information`.

3. **Daily Price Variation Analysis**: The project calculates the daily price variation for different petrol companies, identifying the most stable and volatile companies based on their average daily price variation.

4. **Geographical Analysis**: The project calculates the geographical distance between fuel stations and identifies the top 9 nearest fuel stations based on geographical distance.

5. **Price Difference from Nearest Competitor**: The project calculates the price difference between each fuel station and its nearest competitor based on geographical distance.

## Data Sources

The project uses the following data sources:
The dataset used is from [Kaggle](https://www.kaggle.com/datasets/alessandrolobello/gasoline-hourly-price-tracker-from-2022?select=Hourly_Gasoline_Prices.csv)


### Fuel Station Information (fuel_station_information.csv)

This dataset contains information about fuel stations, including petrol companies, types, station names, cities, and coordinates.

- **Id**: Unique identifier for the fuel station.
- **Fuel_station_manager**: Manager of the fuel station.
- **Petrol_company**: Name of the petrol company.
- **Type**: Type of fuel station.
- **Station_name**: Name of the fuel station.
- **City**: City where the fuel station is located.
- **Latitude**: Latitude coordinates of the fuel station.
- **Longitudine**: Longitude coordinates of the fuel station.

### Hourly Gasoline Price (hourly_gasoline_price.csv)

This dataset contains hourly gasoline prices for different fuel stations.

- **Id**: Unique identifier for the fuel station.
- **isSelf**: Indicator for self-service (1 for self-service, 0 for not self-service).
- **Price**: Gasoline price in USD.
- **Date**: Date and time of the price measurement.

## The directory

- **data**: This folder contains csv data downloaded from the above mentioned source.
- **preprocess_data.ipynb**: This notebook is for preprocessing the csv data, convert it to parquet file and then write that parquet file to database.
- **theFile.py**: This python script is to run with spark-submit.
- **theTask.ipynb**: This notebook is for the tasks provided.


## Requirements

To run this project, you need the following:

- Apache Spark
- PostgreSQL JDBC driver
- Python packages: `pyspark`, `haversine`

## Usage

    1. Set up your Spark environment and ensure the PostgreSQL JDBC driver is available.

    2. Run the `pip install -r requirements.txt` script to execute the Spark job.

    3. Run preprocess_data.ipynb to convert the csv files to parquet and hence write to database.

    4. Run `spark-submit --jars /<path to jar file>/postgresql-42.6.0.jar theFile.py` or theTask.ipynb (if you want to run in interactive python jupyter environment)

## Questions:

*(Lister Oriented: To select which company to invest on)*
### Q1. Find the `Most Stable Company` and `Most Volatile Company` based on `Average Daily Price Variation`.

we determined the "Most Stable Company" and "Most Volatile Company" based on the average daily gasoline price variation. We calculated daily price changes for each company, then ranked them by their average variation. The company with the lowest average was the most stable, while the highest was the most volatile. This analysis helps identify pricing behaviors among different companies.

*(Customer Oriented: To select station for refilling)*
### Q2. `Find 9 nearest stations to a certain reference station and calculate the price difference between the fuel station and its nearest competitor`

we found the 9 nearest fuel stations to a reference station based on latitude and longitude data and calculated the price difference between each station and its closest competitor.


## Acknowledgments

- Kaggle for providing the dataset.
- Apache Spark and PySpark community for powerful data analysis tools.
