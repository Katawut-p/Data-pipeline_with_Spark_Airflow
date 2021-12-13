from pyspark.sql.functions import udf
from datetime import datetime

class PythonFunctions:
    @udf
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