import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import when

# Create spark session
spark = (SparkSession
    .builder
    .master("local")
    .getOrCreate()
)

source_path = sys.argv[1]
destination_path = sys.argv[2]

####################################
# Read CSV Data
####################################
df = spark.read.csv(source_path, header = True, inferSchema = True)

####################################
# FIX timestamp type
####################################
df_clean = df.withColumn("timestamp", df.timestamp.cast("timestamp"))

####################################
# FIX invalid country
####################################
df_clean_country = df_clean.withColumn("CountryUpdate", when(df_clean['Country'] == 'Japane', 'Japan').otherwise(df_clean['Country']))
df_clean = df_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

####################################
# FIX invalid user_id
####################################
df_clean_user_id = df_clean.withColumn("user_idUpdate", 
                                       when(df_clean['user_id'] == 'ca86d17200', 'ca86d172')
                                       .when(df_clean['user_id'].isNull(), '00000000')
                                       .otherwise(df_clean['user_id']))

df_clean = df_clean_user_id.drop("user_id").withColumnRenamed('user_idUpdate', 'user_id')

####################################
# Export cleaned data to csv file
####################################
df_clean.write.mode("overwrite").csv(destination_path, header = True)