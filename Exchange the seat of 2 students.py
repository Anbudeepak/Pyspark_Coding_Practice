import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lag,lead,col,when,coalesce
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('demo2').getOrCreate()

data = [(1,'Alice'),
        (2,'Bob'),
        (3,'Charles'),
        (4,'Daniel'),
        (5,'Edwin')
        ]

schema = ['Id','Name']

df = spark.createDataFrame(data=data,schema=schema)
df_exchange = df.withColumn('prev_val',lag('Name').over(Window.orderBy('Id')))
df_exchange = df_exchange.withColumn('next_value',lead('Name').over(Window.orderBy('Id')))
df_exchange = df_exchange.withColumn('Exchange_Seating',
                                     when(col('Id')%2 == 1 ,coalesce(col('next_value'),col('Name')))
                                     .when(col('Id')%2 == 0 ,coalesce(col('prev_val'),col('Name')))
                                     .otherwise('Name'))
df_final = df_exchange.drop('Name','prev_val','next_value')
df_final.show()