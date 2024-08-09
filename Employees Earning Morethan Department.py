import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,window,col,round
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('demo5').getOrCreate()
data = [
    (1,'Alice','HR',60000),
    (2,'Bob','HR',50000),
    (3,'Charli','Finance',70000),
    (4,'David','Finance',75000),
    (5,'Eve','Engineering',90000),
    (6,'Frank','Engineering',93000),
    (7,'Grace','HR',45000),
    (8,'Hank','Engineering',98000),
    (9,'Ivy','Finance',66000)
    ]

schema = ['Employee_id','employee_name','department','salary']

df = spark.createDataFrame(data=data,schema=schema)
window_spec = Window.partitionBy('department')
df_window = df.withColumn('avg_sal',avg(col('salary')).over(window_spec))
df_result = df_window.filter('salary > avg_sal')
df_result.show()