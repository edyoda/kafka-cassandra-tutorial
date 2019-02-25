from pyspark.sql import SparkSession

if __name__ == '__main__':

    #Create Spark session & configure for cassandra
    spark = SparkSession.builder.appName('PySpark-App').getOrCreate()
    spark.conf.set("spark.cassandra.connection.host", "172.17.0.2,172.17.0.3")

    #Read table emp.user from connected cassandra 
    user_info = spark.read.format("org.apache.spark.sql.cassandra").options(table="user", keyspace="emp").load()
    user_info.show()

    #Create more data to be inserted into cassandra
    more_users = spark.createDataFrame([(99,'awantik'),(100,'edyoda')], ['id','name'])
    more_users.write.format("org.apache.spark.sql.cassandra").options(table="user", keyspace = "emp").save(mode ="append")
