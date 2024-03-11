import requests, os
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/GetAllMakes?format=csv')
with open('vehicle_makes.csv', 'w') as f:
    f.write(res.text)
print('Write Completed')

conn = WebHDFSHook('webhdfs-connection')

conn.load_file('vehicle_makes.csv','vehicle_makes.csv',True,1)

print('Load to HDFS completed')
