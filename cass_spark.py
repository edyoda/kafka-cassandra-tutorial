from pyspark.sql import SparkSession

def load_and_get_table_df(spark,keys_space_name, table_name):
    table_df = spark.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df

if __name__ == '__main__':

    spark = SparkSession.builder.appName('PySpark-App').getOrCreate()
    df = load_and_get_table_df(spark, "emp1", "example_model")
    df.show()
