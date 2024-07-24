import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count
spark = SparkSession.builder.appName('demo3').getOrCreate()
data = [(10,20,58),
        (20,10,12),
        (10,30,20),
        (30,40,100),
        (30,40,200),
        (30,40,200),
        (40,30,500)]
schema = ["from_id","to_id","duration"]
df = spark.createDataFrame(data=data,schema=schema)
df_classify = df.withColumn("person1",when(col("from_id")<col("to_id"),col("from_id")).otherwise(col("to_id")))\
                .withColumn("person2",when(col("from_id")<col("to_id"),col("to_id")).otherwise(col("from_id")))\
                .select('person1','person2','duration')
df_final = (df_classify.groupBy(col('person1'),col('person2'))
            .agg(sum(col("duration")).alias("total_duration"),count(col('person1')).alias("call_count"))
            .select('person1','person2','call_count','total_duration'))