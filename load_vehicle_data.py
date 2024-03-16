from pyspark.sql import SparkSession
from airflow.models.connection import Connection

spark = SparkSession.builder.appName('vehicle_makes').getOrCreate()
makes = spark.read.option('header', 'true').csv(
    'hdfs://spark-driver.desk.home:9000/user/spark_driver_user/vehicle_makes.csv')

makes.write.jdbc('jdbc:mysql://mysql.desk.home:3306/vehicle','makes', mode='overwrite',properties={"user": "spark_user", "password": "msk","driver": "com.mysql.cj.jdbc.Driver"})