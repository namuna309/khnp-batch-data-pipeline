from dotenv import load_dotenv
import os
import requests
from time import sleep
import pandas as pd 
from urllib.parse import unquote
from datetime import datetime
from io import StringIO
import boto3


# load .env
load_dotenv()
AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))
AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))
DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))

plants = ['WS', 'KR', 'YK', 'UJ', 'SU'] # WS : 월성, KR : 고리, YK : 한빛, UJ : 한울, SU : 새울
base_url = 'http://data.khnp.co.kr/environ/service/realtime/'
kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']

# 데이터 요청 함수
def request_data(kind, plant):
    params ={'serviceKey' : DATA_PORTAL_KEY_ENC, 'genName' : plant }

    for try_cnt in range(10):
        res = requests.get(base_url + kind, params=params)

        if res.status_code == 200:
            xml_text = res.text
            try:
                return pd.read_xml(StringIO(xml_text), xpath='.//item'), True   #Passing literal xml to 'read_xml' is deprecated and will be removed in a future version: str -> StringIO(str) 
            except:
                return 0, False
        else:
            try_cnt += 1
            print('요청 실패, 3초후 다시 재요청 시도')
            sleep(3)
    
    return 0, False
    
s3_client = boto3.client(
    service_name='s3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='ap-northeast-2'
    )

for kind in kinds:
    # 데이터 가져오기
    for plant in plants:
        df, is_success = request_data(kind, plant)
        
        if is_success:
            # 경로 및 파일평 설정
            now = datetime.now()
            file_path = f'{kind}'            

            # s3에 csv 파일 저장    
            file_name = '/' + now.strftime('%Y-%m-%d_%H') + f'_{kind}_{plant}.csv'
            s3_res = s3_client.put_object(
                Body=df.to_csv().encode('cp949'),
                Bucket='khnp',
                Key= file_path+file_name
            )
            