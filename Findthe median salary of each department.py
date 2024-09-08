from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,avg,row_number,count
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

spark = SparkSession.builder.appName("Median_salary").getOrCreate()
data = [
    (1,"Alice",60000,"IT"),
    (2,"Bob",70000,"IT"),
    (3,"Charly",80000,"IT"),
    (4,"Danielle",55000,'HR'),
    (5,'Emily',65000,'HR'),
    (6,'Grace',75000,'Finance'),
    (7,'Isabella',80000,'Finance')
]

schema = StructType([
    StructField('id',IntegerType()),
    StructField('name',StringType()),
    StructField('Salary',IntegerType()),
    StructField('Department',StringType())
    ])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
window_spec = Window.partitionBy('Department').orderBy('Salary')
df_row_num = df.withColumn('row',row_number().over(window_spec))\
    .withColumn('Total_rows',count('*').over(Window.partitionBy('Department')))
result = df_row_num.filter((col('row')==col('Total_rows')/2) | (col('row')==(col('Total_rows')/2+1))) .groupBy('Department').agg(avg('salary'))

result.show()