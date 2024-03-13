from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('vehicle_makes').getOrCreate()
makes = spark.read.option('header','true').csv('hdfs://spark-driver.desk.home:9000/user/spark_driver_user/vehicle_makes.csv')
makes.saveAsTable('makes')
