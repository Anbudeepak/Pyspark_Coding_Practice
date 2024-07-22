import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date,col, when, lag,first,last
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('FirstLast').getOrCreate()


data = [
    ("2020-06-01",'won'),
    ("2020-06-02",'won'),
    ("2020-06-03",'won'),
    ("2020-06-04",'lost'),
    ("2020-06-05",'lost'),
    ("2020-06-06",'lost'),
    ("2020-06-07",'won')
]
schema = ['event_data','event_status']
df_table = spark.createDataFrame(data= data,schema=schema)
df_table = df_table.withColumn('event_data',to_date(df_table['event_data']))
df_window = df_table.withColumn('event_change', 
                               when(col("event_status") != lag(col("event_status")).over(Window.orderBy("event_data")), 1)
                               .otherwise(0))
df_sum = df_window.withColumn('event_group',sum('event_change').over(Window.orderBy("event_data")))
df_final = df_sum.groupBy('event_group','event_status').agg(first('event_data').alias('start_date'),last('event_data').alias('end_date')).drop('event_group')
df_final.display()