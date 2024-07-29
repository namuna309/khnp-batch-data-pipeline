import datetime

import pyspark
from dotenv import load_dotenv
import os
from urllib.parse import unquote
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

load_dotenv()

AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))
AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))

conf = pyspark.SparkConf()
conf.set('spark.driver.host', '127.0.0.1')
conf.set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)# Caused by: org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: No AWS Credentials provided by TemporaryAWSCredentialsProvider SimpleAWSCredentialsProvider EnvironmentVariableCredentialsProvider IAMInstanceCredentialsProvider : com.amazonaws.SdkClientException: Unable to load AWS credentials from environment variables (AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY))
conf.set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
conf.set("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.4') # java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found 해결을 위한 구문


spark = SparkSession.builder\
    .config(conf=conf)\
    .appName('s3_rw_example')\
    .getOrCreate()

plants = ['WS', 'KR', 'YK', 'UJ', 'SU']  # 발전소 리스트
kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']  # 데이터 종류 리스트

now = datetime.now().strftime('%Y-%m-%d') + '_16'


pwr_dfs = []
for plant in plants:
    input_path = f'khnp/{kinds[0]}/{plant}/{now}.csv'
    df = spark.read.csv(f's3a://{input_path}', header=True, encoding='cp949')
    pwr_dfs.append(df)


new_pwr_dfs = []

for i in range(len(pwr_dfs)):
    new_pwr_df = pwr_dfs[i].selectExpr(
        '_c0 as gen_id',
        'expl as name_kor',
        'name as name_eng',
        'time as time',
        'value as value'
    ).withColumn('plant', F.lit(plants[i]))
    new_pwr_df.show(truncate=False)

