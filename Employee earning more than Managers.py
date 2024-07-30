import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('demo5').getOrCreate()

data = [(1,'John',6000,4),
        (2,'Kevin',11000,4),
        (3,'Bob',8000,5),
        (4,'Laura',9000,None),
        (5,'Sarah',10000,None)]

schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('salary',IntegerType(),True),
    StructField('managerId',IntegerType(),True)
])

df = spark.createDataFrame(data=data,schema=schema)

df_join = df.alias("employee").join(df.alias("manager"),
                                    col("employee.managerId") == col("manager.id"),
                                    'inner')

df_final = df_join.filter(col('employee.salary')>col('manager.salary')).select(col('employee.name'))

df_final.display()