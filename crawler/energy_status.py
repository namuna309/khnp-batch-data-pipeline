from dotenv import load_dotenv
import os
import requests
from time import sleep
import pandas as pd 
from urllib.parse import unquote
from datetime import datetime

# load .env
load_dotenv()

DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))
plants = ['WS', 'KR', 'YK', 'UJ', 'SU'] # WS : 월성, KR : 고리, YK : 한빛, UJ : 한울, SU : 새울
url = 'http://data.khnp.co.kr/environ/service/realtime/pwr'

# 데이터 요청 함수
def request_data(plant):
    params ={'serviceKey' : DATA_PORTAL_KEY_ENC, 'genName' : plant }

    for try_cnt in range(10):
        res = requests.get(url, params=params)

        if res.status_code == 200:
            xml_text = res.text
            return pd.read_xml(xml_text, xpath='.//item'), True
        else:
            try_cnt += 1
            print('요청 실패, 3초후 다시 재요청 시도')
            sleep(3)
    
    return 0, False
    
    
# 데이터 가져오기    
energy_status_df, is_success = request_data(plants[0])

if is_success:
    # csv파일로 저장
    now = datetime.now()
    file_path = './csv/energy_status/'
    file_name = now.strftime('%Y-%m-%d_%H') + '_energy_statas.csv'
    energy_status_df.to_csv(file_path + file_name, encoding='cp949')