import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
spark = SparkSession.builder.appName('demo4').getOrCreate()
data_p = [(1,'Nadal'),
        (2,'Federer'),
        (3,'Novak'),
        (4,'JD')]
data_c = [(2017,2,1,4,2),
          (2018,3,1,3,2),
          (2019,3,1,1,3),
          (2020,2,1,4,4)]
schema_p = ['player_id','player_name']

schema_c = ['Year','Wimbledon','Fr_open','Us_open','Au_open']
df_player = spark.createDataFrame(data=data_p,schema=schema_p)
df_champions = spark.createDataFrame(data=data_c,schema=schema_c)
df_champions1 = df_champions.select('year','Wimbledon')
df_champions2 = df_champions.select('year','Fr_open')
df_champions3 = df_champions.select('year','Us_open')
df_champions4 = df_champions.select('year','Au_open')
df_champions_union = df_champions1.unionAll(df_champions2).unionAll(df_champions3).unionAll(df_champions4)
df_champions_count = df_champions_union.groupBy('Wimbledon').agg(count('Wimbledon').alias('count'))
df_join = df_player.join(df_champions_count,df_player.player_id == df_champions_count.Wimbledon,'inner')\
            .select('player_id','player_name','count')
df_join.show()