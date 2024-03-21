import sys

from pyspark.sql import SparkSession
from airflow.models import Variable

mysql_pwd = Variable.get("foo")

api_action = sys.argv[1]
table_name = api_action.replace('Get', '')

spark = SparkSession.builder.appName(f'{api_action}').getOrCreate()
makes = spark.read.option('header', 'true').csv(
    f'hdfs://spark-driver.desk.home:9000/user/spark_driver_user/{api_action}.csv')

print(f'Data Frame read is completed for {api_action}')

makes.write.jdbc('jdbc:mysql://mysql.desk.home:3306/vehicle?allowPublicKeyRetrieval=true&useSSL=false', table_name,
                 mode='overwrite',
                 properties={"user": "spark_user", "password": f"{mysql_pwd}", "driver": "com.mysql.cj.jdbc.Driver"})

print(f'The Data is loaded into MySQL for {table_name}')
