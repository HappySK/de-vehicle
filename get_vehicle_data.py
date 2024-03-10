import requests

base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/GetAllMakes?format=csv')
with open('hdfs://spark-driver.desk.home:9000/vehicle_makes.csv', 'w') as f:
    f.write(res.text)
print('Write Completed')
