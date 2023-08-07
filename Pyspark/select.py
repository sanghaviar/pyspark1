from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.getOrCreate()
data = [(1,'maher','m',20000)]
schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
df.show()
df.select('id','name').show() #or
df.select(df.id,df.name).show() #or
df.select(df['id'],df['name']).show() #or
df.select(col('id'),col('name')).show() #or
df.select(['id','name']).show() #or
df.select('*').show() # to show all columns
df.select([col for col in df.columns]).show()