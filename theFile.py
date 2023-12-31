import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lag, avg, abs, lit, min, udf, dense_rank
from pyspark.sql.window import Window
from haversine import haversine, Unit
from pyspark.sql.types import FloatType

yaml_file_path = 'credentials.yaml'
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

spark = SparkSession.builder.appName('finalproject')\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-17-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()

stationInfo_df = spark.read.csv("data/Fuel_Station_Information.csv", header=True, inferSchema=True)
hourlyPrices_df = spark.read.csv("data/Hourly_Gasoline_Prices.csv", header=True, inferSchema=True)
hourlyPrices_df = hourlyPrices_df.dropDuplicates()
stationInfo_df = stationInfo_df.dropDuplicates()
hourlyPrices_df = hourlyPrices_df.dropna()
stationInfo_df = stationInfo_df.dropna()
hourlyPrices_df = hourlyPrices_df.withColumn("Date", to_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss"))
hourlyPrices_df.write.parquet("data/cleaned_fuel_prices.parquet", mode= "overwrite", compression= "snappy")
stationInfo_df.write.parquet("data/cleaned_station_info.parquet", mode= "overwrite", compression= "snappy")

# Read the Parquet files
hourlyPrices_df = spark.read.parquet("data/cleaned_fuel_prices.parquet")
stationInfo_df = spark.read.parquet("data/cleaned_station_info.parquet")
stationInfo_df = stationInfo_df.withColumn("Latitude", stationInfo_df["Latitude"].cast(FloatType()))
stationInfo_df = stationInfo_df.withColumn("Longitudine", stationInfo_df["Longitudine"].cast(FloatType()))
# Define the JDBC connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/sparkProject"
jdbc_properties = {
    "user": config['postgres']["user"],
    "password": str(config['postgres']["password"]),
    "driver": "org.postgresql.Driver"
}
# Save DataFrames to PostgreSQL tables
hourlyPrices_df.write.jdbc(url=jdbc_url, table="hourly_gasoline_prices", mode="overwrite", properties=jdbc_properties)
stationInfo_df.write.jdbc(url=jdbc_url, table="fuel_station_information", mode="overwrite", properties=jdbc_properties)

# Read DataFrames to PostgreSQL tables
hourlyPrices_df = spark.read.format('jdbc').options(url="jdbc:postgresql://localhost:5432/sparkProject", driver = 'org.postgresql.Driver', dbtable='hourly_gasoline_prices', user=config['postgres']["user"],password=str(config['postgres']["password"])).load()
stationInfo_df = spark.read.format('jdbc').options(url="jdbc:postgresql://localhost:5432/sparkProject", driver = 'org.postgresql.Driver', dbtable='fuel_station_information', user=config['postgres']["user"],password=str(config['postgres']["password"])).load()

df_forAvg = hourlyPrices_df.join(stationInfo_df.select("Id", "Petrol_company"), "Id")
window_spec = Window.partitionBy("Petrol_company").orderBy("Date")
daily_var = df_forAvg.withColumn("Previous_Price", lag("Price").over(window_spec))
daily_var = daily_var.withColumn("Price_Variation", abs(col("Price") - col("Previous_Price")))
daily_var = daily_var.filter(col("Previous_Price").isNotNull())
avg_variation_df = daily_var.groupBy("Petrol_company").agg(avg("Price_Variation").alias("Avg_Daily_Variation"))

min_variation_company = avg_variation_df.orderBy("Avg_Daily_Variation").first()
max_variation_company = avg_variation_df.orderBy(avg_variation_df["Avg_Daily_Variation"].desc()).first()

print("Most Stable Company:")
print("Company:", min_variation_company["Petrol_company"])
print("Average Daily Price Variation:", min_variation_company["Avg_Daily_Variation"])
print("_*"*33)
print("Most Volatile Company:")
print("Company:", max_variation_company["Petrol_company"])
print("Average Daily Price Variation:", max_variation_company["Avg_Daily_Variation"])

data_t1 = [
    ("Most Stable Company", min_variation_company["Petrol_company"], min_variation_company["Avg_Daily_Variation"]),
    ("Most Volatile Company", max_variation_company["Petrol_company"],  max_variation_company["Avg_Daily_Variation"])
]
schema_t1 = ["Category", "Company", "Average_Daily_Price_Variation"]
df_t1 = spark.createDataFrame(data_t1, schema_t1)

jdbc_url = "jdbc:postgresql://localhost:5432/sparkProject"
jdbc_properties = {
    "user": config['postgres']["user"],
    "password": str(config['postgres']["password"]),
    "driver": "org.postgresql.Driver"
}
df_t1.write.jdbc(url=jdbc_url, table="stable_volatile_company", mode="overwrite", properties=jdbc_properties)

@udf(FloatType())
def calculate_distance(lat1, lon1, lat2, lon2):
    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)

ref_latitude = 40.7160385
ref_longitude = 14.9413282

stationInfo_df = stationInfo_df.filter((col("Latitude").isNotNull()) & (col("Longitudine").isNotNull()))

dis_stationInfo_df = stationInfo_df.withColumn(
    "distance",
    calculate_distance(
        lit(ref_latitude),
        lit(ref_longitude),
        col("Latitude"),
        col("Longitudine")
    )
)

min_distance_df = dis_stationInfo_df.groupBy("Id").agg(min("distance").alias("min_distance"))
price_diff_df = hourlyPrices_df.join(min_distance_df, "Id", "inner")

window_spec = Window.orderBy(col("min_distance"))
ranked_stations_df = price_diff_df.withColumn("rank", dense_rank().over(window_spec))
deduplicated_stations_df = ranked_stations_df.dropDuplicates(['Id']).drop("Date")
filtered_stations_df = deduplicated_stations_df.filter(col("rank") <= 9).orderBy("rank")
filtered_stations_df = filtered_stations_df.drop('rank')

window_spec = Window.orderBy("min_distance")
filtered_stations_df = filtered_stations_df.withColumn("Price_of_Nearest_Competitor", lag("Price").over(window_spec))
filtered_stations_df = filtered_stations_df.withColumn(
    "PriceDifferenceFromNearestCompetitor",
    col("Price") - col("Price_of_Nearest_Competitor")
)
filtered_stations_df.show()

filtered_stations_df.write.jdbc(url=jdbc_url, table="Nearest_competitor_comparision", mode="overwrite", properties=jdbc_properties)

spark.stop()
