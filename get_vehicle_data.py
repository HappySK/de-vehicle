import requests, sys
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

api_action = sys.argv[1]
base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/{api_action}?format=csv')
with open(f'{api_action}.csv', 'w') as f:
    f.write(res.text)
print(f'Write Completed for {api_action}')

conn = WebHDFSHook('webhdfs-connection')

conn.load_file('vehicle_makes.csv', 'vehicle_makes.csv', True, 1)

print(f'Load to HDFS completed for {api_action}')
