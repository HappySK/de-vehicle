from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('vehicle_makes').config('spark.sql.warehouse.dir','webhdfs://spark-driver.desk.home:9000/user/spark_driver_user/vehicle/').getOrCreate()
makes = spark.read.option('header','true').csv('hdfs://spark-driver.desk.home:9000/user/spark_driver_user/vehicle_makes.csv')
makes.write.saveAsTable('makes')
