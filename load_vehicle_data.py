from pyspark.sql import SparkSession
from airflow.models.connection import Connection

conn = Connection(
    conn_id="jdbc-conn-id",
    description="Establish connection between Spark & MySQL"
)

spark = SparkSession.builder.appName('vehicle_makes').getOrCreate()
makes = spark.read.option('header', 'true').csv(
    'hdfs://spark-driver.desk.home:9000/user/spark_driver_user/vehicle_makes.csv')

print(conn)
