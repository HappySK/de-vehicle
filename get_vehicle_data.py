import requests, os
from pyspark.sql import SparkSession
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

spark = SparkSession.builder.appName('get_vehicle_data').getOrCreate()

base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/GetAllMakes?format=csv')
with open('vehicle_makes.csv', 'w') as f:
    f.write(res.text)
print('Write Completed')

conn = WebHDFSHook('webhdfs-connection')

conn.load_file('vehicle_makes.csv','hdfs://spark-driver.desk.home:50070/user/spark_driver_user/vehicle_makes.csv',True,1)

print('Load to HDFS completed')
