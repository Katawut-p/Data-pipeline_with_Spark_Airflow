import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window
from modules import reformat_date

# Create spark session
spark = (SparkSession
    .builder
    .master("local")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

####################################
# Parameters
####################################
final_df_path = sys.argv[1]
thai_df_path = sys.argv[2]

####################################
# Read covid19_global_data final file
####################################
print("######################################")
print("READING covid19_global_data final file")
print("######################################")
global_df = spark.read.csv(final_df_path,header = True, inferSchema = True)

####################################
# Filter country Thailand
####################################
print("######################################")
print("Filter country Thailand")
print("######################################")
thai_df = global_df.sort(global_df['Date'].asc()).filter(global_df['Country'] == "Thailand").drop('Province_and_State')

print("######################################")
print("Transform data and calculate")
print("######################################")
####################################
# Create previous column for calculate confirmed and death per day
####################################
my_window = Window.partitionBy().orderBy("Date")
thai_df = thai_df.withColumn("Prev_Confirmed", f.lag(thai_df['Confirmed']).over(my_window)) \
                 .withColumn("Prev_Death", f.lag(thai_df['Death']).over(my_window))

####################################
# Fill values 0 replace Null values
####################################
thai_df = thai_df.na.fill(0, subset=["Prev_Confirmed"]) \
                 .na.fill(0, subset=["Prev_Death"])

####################################
# Fill values 0 replace Null values
####################################
thai_df = thai_df.withColumn("Confirm_today", thai_df['Confirmed'] - thai_df['Prev_Confirmed']) \
                 .withColumn("Death_today", thai_df['Death'] - thai_df['Prev_Death'])

####################################
# Drop previous column values
####################################
thai_df = thai_df.drop('Prev_Confirmed') \
                 .drop('Prev_Death')

####################################
# Export thailand covid19 data to csv file
####################################
print("######################################")
print("Export thailand covid19 data to csv file")
print("######################################")

thai_df.write.mode("overwrite").csv(thai_df_path, header = True)