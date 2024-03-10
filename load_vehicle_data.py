from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('vehicle_makes').getOrCreate()
makes = spark.read.option('header','true').csv('/home/spark_driver_user/airflow/vehicle_makes.csv')
makes.show(truncate=False)
