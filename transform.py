from dotenv import load_dotenv
import os
from urllib.parse import unquote
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DoubleType, TimestampType
from pyspark.sql.functions import split, regexp_extract, monotonically_increasing_id, lit, concat, when, col
from datetime import datetime
from functools import reduce
import boto3

class SparkDataProcessor:
    def __init__(self):
        load_dotenv()  # 환경 변수 로드
        self.AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))  # AWS 액세스 키 로드
        self.AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))  # AWS 시크릿 키 로드
        self.S3_REGION = 'ap-northeast-2'  # S3 리전 설정
        self.spark = self._create_spark_session()  # Spark 세션 생성
        self.kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']  # 데이터 종류 리스트
        self.plants = {  # 플랜트 종류별 리스트
            'pwr': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'weather': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'air': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'radiorate': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'inoutwater': ['WS', 'KR', 'YK', 'UJ'],
            'wastewater': ['WS', 'KR', 'YK'],
        }
        self.data_schemas = self._define_schemas()  # 데이터 스키마 정의

    def _create_spark_session(self):
        conf = pyspark.SparkConf()
        conf.set('spark.driver.host', '127.0.0.1')  # Spark 드라이버 호스트 설정
        conf.set('spark.hadoop.fs.s3a.access.key', self.AWS_ACCESS_KEY)  # S3 액세스 키 설정
        conf.set('spark.hadoop.fs.s3a.secret.key', self.AWS_SECRET_KEY)  # S3 시크릿 키 설정
        conf.set("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.4')  # S3 패키지 설정

        # Spark 세션 생성 및 반환
        return SparkSession.builder.config(conf=conf).appName('s3_rw_example').getOrCreate()

    def _define_schemas(self):
        # 각 데이터 종류별로 스키마 정의 및 반환
        return {
            'pwr': StructType([
                StructField('id', StringType(), True),
                StructField('name_kor', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('value', DoubleType(), True)
            ]),
            'weather': StructType([
                StructField('id', StringType(), True),
                StructField('type', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('value', DoubleType(), True),
            ]),
            'air': StructType([
                StructField('id', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('Grade', StringType(), True),
            ]),
            'radiorate': StructType([
                StructField('id', StringType(), True),
                StructField('position', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('value', DoubleType(), True),
            ]),
            'inoutwater': StructType([
                StructField('id', StringType(), True),
                StructField('type', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('value', DoubleType(), True),
            ]),
            'wastewater': StructType([
                StructField('id', StringType(), True),
                StructField('expl', StringType(), True),
                StructField('name_eng', StringType(), True),
                StructField('time', TimestampType(), True),
                StructField('value', DoubleType(), True),
            ])
        }

    def process_data(self):
        now = datetime.now().strftime('%Y-%m-%d_%H')  # 현재 시간 형식 지정
        for kind in self.kinds:
            dfs = []  # 데이터프레임 리스트 초기화
            for plant in self.plants[kind]:
                input_path = f'khnp/raw/{kind}/{plant}/{now}.csv'  # 입력 파일 경로 지정
                df = self.spark.read.csv(f's3a://{input_path}', header=True, encoding='cp949')  # CSV 파일 읽기
                df = self._rename_columns(df, kind)  # 컬럼 이름 변경
                df = self._process_columns(df, kind)  # 컬럼 처리
                df = df.withColumn('plant', lit(plant))  # 'plant' 컬럼 추가
                dfs.append(df)  # 데이터프레임 리스트에 추가
            union_df = reduce(lambda df1, df2: df1.union(df2), dfs)  # 데이터프레임 합치기
            union_df = union_df.coalesce(1).withColumn("id", monotonically_increasing_id())  # 단일 파티션으로 병합하고 ID 추가
            union_df.show()  # 결과 출력
            s3_client = boto3.client(  # S3 클라이언트 생성
                service_name='s3',
                aws_access_key_id=self.AWS_ACCESS_KEY,
                aws_secret_access_key=self.AWS_SECRET_KEY,
                region_name=self.S3_REGION
            )
            output_path = f'khnp/transform/{kind}/{now}/'  # 출력 경로 지정
            union_df.write.option('header', 'true').parquet(f's3a://{output_path}')  # Parquet 파일로 저장

    def _rename_columns(self, df, kind):
        # 스키마에 맞게 데이터프레임의 컬럼 이름 변경
        schema = self.data_schemas[kind]
        for old_field, new_field in zip(df.schema.fields, schema.fields):
            df = df.withColumnRenamed(old_field.name, new_field.name)
        return df

    def _process_columns(self, df, kind):
        # 데이터 종류에 따라 컬럼 처리
        if kind == 'pwr':
            df_split = df.withColumn("name_kor_split", split(df["name_kor"], " "))  # 'name_kor' 컬럼 분할
            df = df_split.withColumn("plant_num", when(df_split["name_kor_split"].getItem(0).startswith("신"),
                                                        concat(lit("신"), regexp_extract(df_split["name_kor_split"].getItem(0), r'\d+', 0))) \
                                                    .otherwise(regexp_extract(df_split["name_kor_split"].getItem(0), r'\d+', 0))) \
                .withColumn("component", df_split["name_kor_split"].getItem(1)) \
                .withColumn("measure", df_split["name_kor_split"].getItem(2)) \
                .drop("name_kor_split").drop('name_kor')
        elif kind == 'radiorate':
            df = df.withColumn('location', regexp_extract('position', r"ERMS-(.*)\(MS-\d+\)", 1)) \
                .drop('position')
        elif kind == 'inoutwater':
            df = df.withColumn('type_splited', split(df['type'], '-'))
            df = df.withColumn('location', df['type_splited'].getItem(0)) \
                .withColumn('parameter', df['type_splited'].getItem(1)) \
                .drop('type_splited').drop('type')
        elif kind == 'wastewater':
            df = df.withColumn("type", regexp_extract(df["expl"], r"-(.*?)(\(|$)", 1)) \
                .drop('expl')
        return df

if __name__ == "__main__":
    processor = SparkDataProcessor()  # SparkDataProcessor 클래스의 인스턴스 생성
    processor.process_data()  # 데이터 처리 시작
