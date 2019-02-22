from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
import time

if __name__ == '__main__':
    conf = SparkConf().setAppName("Stand Alone Python Script").set("spark.cassandra.connection.host", "172.17.0.2")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('PySpark-App').getOrCreate()
    sqlContext = SQLContext(sc)

    table = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="user", keyspace="emp").load()

    table.show()
    table = spark.createDataFrame([(222,'abc'),(332,'def')], ['id','location'])
    table.write.format("org.apache.spark.sql.cassandra").options(table="user", keyspace = "emp").option("confirm.truncate","true").save(mode ="append")
