import requests, os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('get_vehicle_data').getOrCreate()

base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/GetAllMakes?format=csv')
with open('vehicle_makes.csv', 'w') as f:
    f.write(res.text)
print('Write Completed')

makes_df = spark.read.option('header','true').csv('/home/airflow_user/vehicle_makes.csv')
makes_df.write.csv('hdfs://spark-driver.desk.home:9000/vehicle_csv')
