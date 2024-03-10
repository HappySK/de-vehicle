from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('vehicle_makes').getOrCreate()
makes = spark.read.option('header','true').csv('vehicle_makes.csv')
makes.show(truncate=False)
