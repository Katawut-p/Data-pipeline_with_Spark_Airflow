import sys
from pyspark.sql import SparkSession
from modules import reformat_date

# Create spark session
spark = (SparkSession
    .builder
    .master("local")
    .getOrCreate()
)
####################################
# Parameters
####################################
postgres_spark_url_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
final_df_path = sys.argv[4]

####################################
# Read Postgres
####################################
print("######################################")
print("READING covid19_global_data TABLES")
print("######################################")

covid19_global_df = (
    spark.read
    .format("jdbc")
    .option("url", postgres_spark_url_db)
    .option("dbtable", "covid19_global_data")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

####################################
# Change data type
####################################
print("######################################")
print("Change data type")
print("######################################")

final_df = covid19_global_df.withColumn('Confirmed', covid19_global_df['Confirmed'].cast('int')) \
                 .withColumn('Death', covid19_global_df['Death'].cast('int')) \
                 .withColumn('Date', reformat_date.PythonFunctions.re_format_date(covid19_global_df['Date']))

####################################
# Export covid19 global data to csv file
####################################
print("######################################")
print("Export covid19 global data final to csv file")
print("######################################")

final_df.write.mode("overwrite").csv(final_df_path, header = True)