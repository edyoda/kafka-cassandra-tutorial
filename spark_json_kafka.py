from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import udf,col
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

if __name__ == '__main__':
    
    spark = SparkSession.builder.appName('PySpark-App').getOrCreate()

    df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "json-topic") \
    .option("startingOffsets", "earliest") \
    .load()


    res = df_kafka.selectExpr("CAST(value AS STRING)")
    res.writeStream.outputMode("append").format("memory").queryName("table").start()

    parseSchema = StructType((
        StructField("name",StringType(),True),
        StructField("loc",StringType(),True),
        StructField("salary",IntegerType(),True)))

    while True:
        time.sleep(5)
        df = spark.sql("select * from table")
        df = df.select(from_json(col("value"), parseSchema).alias("n"))
        df.selectExpr("n.name","n.loc","n.salary").show()
        #df.selectExpr("d.name","d.loc","d.salary").show()
