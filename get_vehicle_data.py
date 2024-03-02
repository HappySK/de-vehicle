import requests

base_url = 'https://vpic.nhtsa.dot.gov/api'
res = requests.get(f'{base_url}/vehicles/GetAllMakes?format=csv')
with open('vehicle_makes.csv', 'w') as f:
    f.write(res.text)
print('Write Completed')
