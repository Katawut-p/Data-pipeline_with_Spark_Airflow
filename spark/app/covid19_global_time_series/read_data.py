import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import TimestampType
from datetime import datetime
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

def re_format_date(date):
        # yyyy-MM-dd HH:mm:ss  <- format example 11/22/21 -> 2021-11-22 00:00:00
        x = date.split('/')
        year = '20' + x[2]
        month = x[0] if int(x[0]) > 9 else '0' + x[0]
        day = x[1] if int(x[1]) > 9 else '0' + x[1]
    
        #Concat date and convert to dateTime format
        timestamp_str = "{}-{}-{}".format(year, month, day)
        new_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d')
    
        return new_timestamp

re_format_date_udf = f.udf(lambda date: re_format_date(date), TimestampType())

final_df = covid19_global_df.withColumn('Confirmed', covid19_global_df['Confirmed'].cast('int')) \
                 .withColumn('Death', covid19_global_df['Death'].cast('int')) \
                 .withColumn('Date', re_format_date_udf(covid19_global_df['Date'])
                 )

####################################
# Export covid19 global data to csv file
####################################
print("######################################")
print("Export covid19 global data final to csv file")
print("######################################")

final_df.write.mode("overwrite").csv(final_df_path, header = True)