from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.getOrCreate()
data = [(1,"Maheer",3000),
        (2,"Anu",4000)]
schema = StructType([\
    StructField(name = 'id',dataType=IntegerType()),\
    StructField(name='name', dataType=StringType()),\
    StructField(name='salary', dataType=IntegerType()) ])


df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()