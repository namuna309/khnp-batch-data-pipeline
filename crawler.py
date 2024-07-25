from dotenv import load_dotenv
import os
import requests
from time import sleep
import pandas as pd
from urllib.parse import unquote
from datetime import datetime
from io import StringIO
import boto3

class DataCrawler:
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_bucket_name, s3_region):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.data_portal_key_enc = data_portal_key_enc
        self.s3_bucket_name = s3_bucket_name
        self.s3_client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=s3_region
        )

    def request_data(self, url, params):
        for try_cnt in range(10):
            res = requests.get(url, params=params)
            print(res.url)
            if res.status_code == 200:
                xml_text = res.text
                try:
                    return pd.read_xml(StringIO(xml_text), xpath='.//item'), True
                except Exception as e:
                    print(f"Error parsing XML: {e}")
                    return 0, False
            else:
                try_cnt += 1
                print('Request failed, retrying in 3 seconds')
                sleep(3)
        return 0, False

    def save_to_s3(self, df, file_path):
        now = datetime.now()
        file_name = '/' + now.strftime('%Y-%m-%d_%H') + f'_{file_path}.csv'
        file = df.to_csv(encoding='cp949')
        s3_res = self.s3_client.put_object(
            Body=file,
            Bucket=self.s3_bucket_name,
            Key=file_path + file_name
        )
        return s3_res


class KHNPCrawler(DataCrawler):
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_region):
        super().__init__(aws_access_key, aws_secret_key, data_portal_key_enc, 'khnp', s3_region)
        self.plants = ['WS', 'KR', 'YK', 'UJ', 'SU']
        self.kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']
        self.base_url = 'http://data.khnp.co.kr/environ/service/realtime/'

    def crawl_data(self):
        for kind in self.kinds:
            for plant in self.plants:
                params = {'serviceKey': self.data_portal_key_enc, 'genName': plant}
                df, is_success = self.request_data(self.base_url + kind, params)
                if is_success:
                    file_path = f'{kind}_{plant}'
                    self.save_to_s3(df, file_path)


class KPXCrawler(DataCrawler):
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_region):
        super().__init__(aws_access_key, aws_secret_key, data_portal_key_enc, 'kpx', s3_region)
        self.base_url = 'https://openapi.kpx.or.kr/openapi/sukub5mMaxDatetime/getSukub5mMaxDatetime/'

    def crawl_data(self):
        params = {'serviceKey': self.data_portal_key_enc}
        df, is_success = self.request_data(self.base_url, params)
        if is_success:
            file_path = 'sukub'
            self.save_to_s3(df, file_path)


if __name__ == "__main__":
    load_dotenv()
    AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))
    AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))
    DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))
    S3_REGION = 'ap-northeast-2'

    khnp_crawler = KHNPCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)
    kpx_crawler = KPXCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)

    khnp_crawler.crawl_data()
    # kpx_crawler.crawl_data()
