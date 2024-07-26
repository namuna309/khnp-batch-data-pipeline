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
            if res.status_code == 200:
                xml_text = res.text
                try:
                    xml_to_df = pd.read_xml(StringIO(xml_text), xpath='.//item')
                    return xml_to_df, None
                except Exception as e:
                    log_message = f"Error parsing XML: {e}"
                    return 0, log_message
            else:
                print('Request failed, retrying in 3 seconds')
                sleep(3)
        log_message = f"Failed to fetch data after 10 retries for URL: {url}"
        return 0, log_message

    def save_to_s3(self, df, file_path):
        now = datetime.now().strftime('%Y-%m-%d_%H')
        file_name = f'/{now}.csv'
        file = df.to_csv().encode('cp949')
        try:
            s3_res = self.s3_client.put_object(
                Body=file,
                Bucket=self.s3_bucket_name,
                Key=file_path + file_name
            )
            return None
        except Exception as e:
            log_message = f"Error saving data to S3 bucket {self.s3_bucket_name}: {e}"
            return log_message

    def log_result(self, message):
        log_file_path = 'log/cralwer.log'
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"{self.s3_bucket_name} [{now}] {message}\n"
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        with open(log_file_path, 'a') as log_file:
            log_file.write(log_message)


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
                df, log_message = self.request_data(self.base_url + kind, params)
                if log_message:
                    log_message = f'[Kind: {kind} GenName: {plant}]' + ' ' + '[Requesting data]' + ' ' + log_message
                    self.log_result(log_message)
                else:
                    file_path = f'{kind}/{plant}'
                    save_to_res = self.save_to_s3(df, file_path)
                    if save_to_res:
                        log_message = f'[Kind: {kind} GenName: {plant}]' + ' ' + '[Saving to S3]' + ' ' + log_message 
                        self.log_result(log_message)
                    


class KPXCrawler(DataCrawler):
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_region):
        super().__init__(aws_access_key, aws_secret_key, data_portal_key_enc, 'kpx', s3_region)
        self.base_url = 'https://openapi.kpx.or.kr/openapi/sukub5mMaxDatetime/getSukub5mMaxDatetime/'

    def crawl_data(self):
        params = {'serviceKey': self.data_portal_key_enc}
        df, log_message = self.request_data(self.base_url, params)
        if log_message:
            log_message = '[Requesting data]' + ' ' + log_message
            self.log_result(log_message) 
        else:
            file_path = 'sukub'
            save_to_res = self.save_to_s3(df, file_path)
            if save_to_res:
                log_message = '[Saving to S3]' + ' ' + log_message 
                self.log_result(log_message)


if __name__ == "__main__":
    load_dotenv()
    AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))
    AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))
    DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))
    S3_REGION = 'ap-northeast-2'

    khnp_crawler = KHNPCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)
    kpx_crawler = KPXCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)

    khnp_crawler.crawl_data()
    kpx_crawler.crawl_data()
