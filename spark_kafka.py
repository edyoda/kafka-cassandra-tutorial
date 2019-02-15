from pyspark.sql import SparkSession
import time

if __name__ == '__main__':
    spark = SparkSession.builder.appName('PySpark-App').getOrCreate()
    df_kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark").option("startingOffsets", "earliest").load()

    res = df_kafka.selectExpr("CAST(value AS STRING)")
    res.writeStream.outputMode("append").format("memory").queryName("table").start()

    time.sleep(10)
    spark.sql("select * from table").show()
    time.sleep(10)
    spark.sql("select * from table").show()
    time.sleep(10)
    d = spark.sql("select * from table")
    d.show()
