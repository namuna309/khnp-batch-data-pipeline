from dotenv import load_dotenv
import os
import requests
from time import sleep
import pandas as pd 
from urllib.parse import unquote
from datetime import datetime
from io import StringIO


# load .env
load_dotenv()

DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))
plants = ['WS', 'KR', 'YK', 'UJ', 'SU'] # WS : 월성, KR : 고리, YK : 한빛, UJ : 한울, SU : 새울
base_url = 'http://data.khnp.co.kr/environ/service/realtime/'
kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']

# 데이터 요청 함수
def request_data(kind, plant):
    params ={'serviceKey' : DATA_PORTAL_KEY_ENC, 'genName' : plant }

    for try_cnt in range(10):
        print(base_url + kind)
        res = requests.get(base_url + kind, params=params)

        if res.status_code == 200:
            xml_text = res.text
            return pd.read_xml(StringIO(xml_text), xpath='.//item'), True   #Passing literal xml to 'read_xml' is deprecated and will be removed in a future version: str -> StringIO(str) 
        else:
            try_cnt += 1
            print('요청 실패, 3초후 다시 재요청 시도')
            sleep(3)
    
    return 0, False
    

for kind in kinds:
    # 데이터 가져오기    
    power_df, is_success = request_data(kind, plants[0])

    if is_success:
        # csv파일로 저장
        now = datetime.now()
        csv_file_path = f'\csv\{kind}\\'
        csv_dir = os.getcwd() + csv_file_path
        
        # csv 저장할 경로가 있는지 확인 후 없으면 생성
        if not os.path.isdir(csv_dir):
            os.makedirs(csv_dir, exist_ok=True)

        # csv 파일 저장    
        file_name = now.strftime('%Y-%m-%d_%H') + f'_{kind}.csv'
        power_df.to_csv('.' + csv_file_path + file_name, encoding='cp949')