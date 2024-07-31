from dotenv import load_dotenv  # .env 파일에서 환경 변수를 로드하기 위한 라이브러리
import os
import requests  # HTTP 요청을 보내기 위한 라이브러리
from time import sleep  # 일정 시간 대기하기 위한 라이브러리
import pandas as pd  # 데이터 처리 및 분석을 위한 라이브러리
from urllib.parse import unquote  # URL 디코딩을 위한 라이브러리
from datetime import datetime, timedelta  # 날짜 및 시간 처리를 위한 라이브러리
from io import StringIO  # 문자열을 파일처럼 다루기 위한 라이브러리
import boto3  # AWS SDK for Python
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# 데이터 크롤링 및 S3 저장을 위한 기본 클래스
class DataCrawler:
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_bucket_name, s3_region):
        # AWS 및 데이터 포털 키 초기화
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.data_portal_key_enc = data_portal_key_enc
        self.s3_bucket_name = s3_bucket_name
        # S3 클라이언트 생성
        self.s3_client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=s3_region
        )
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'}

    # 데이터를 요청하는 메서드
    def request_data(self, url, params):
        max_retries = 10
        for try_cnt in range(max_retries):  # 최대 10번 시도
            try:
                res = requests.get(url, params=params, headers=self.headers)
                xml_text = res.text  # 응답 텍스트(XML 형식)
                break
            except Exception as e:
                print(e)
                print(f'Request failed, retrying in 3 seconds. Requesting count: {try_cnt + 1} for URL: {url} {params}')  # 요청 실패 시 재시도 메시지 출력
        else:    
            # 최대 재시도 횟수 초과 시 실패 메시지 생성
            log_message = f"Failed to fetch data after 10 retries for URL: {url}"
            return 0, log_message  # 실패 메시지 반환
    
        try:
            # XML 텍스트를 데이터프레임으로 변환
            xml_to_df = pd.read_xml(StringIO(xml_text), xpath='.//item')
            res.close()
            return xml_to_df, None  # 데이터프레임과 성공 메시지 반환
        except Exception as e:
            # XML 파싱 중 에러 발생 시 로그 메시지 생성
            log_message = f"Error parsing XML: {e}"
            return 0, log_message  # 에러 메시지 반환

    # 데이터를 S3에 저장하는 메서드
    def save_to_s3(self, df, file_path):
        now = datetime.now().strftime('%Y-%m-%d_%H')  # 현재 시간 형식 지정
        Key_path = file_path + f'/{now}.csv'  # 파일 이름 생성
        file = df.to_csv().encode('cp949')  # 데이터프레임을 CSV로 변환 후 인코딩
        
        for try_cnt in range(10):  # 최대 10번 시도
            try:
                # S3에 파일 업로드
                s3_res = self.s3_client.put_object(
                    Body=file,
                    Bucket=self.s3_bucket_name,
                    Key=Key_path
                )
                return None  # 성공 시 None 반환
            except Exception as e:
                # 업로드 실패 시 로그 메시지 생성
                log_message = f"Error saving data to S3 bucket {self.s3_bucket_name}: {e}"
                continue  # 재시도
        return log_message  # 실패 시 로그 메시지 반환
    
    # 가장 최근 파일을 복사하는 메서드
    def copy_latest_file(self, file_path):
        now = datetime.now().strftime('%Y-%m-%d_%H') # 현재 시간 형식 지정
        before = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d_%H')
        CopySource_path = file_path + f'/{before}.csv'
        Key_path = file_path + f'/{now}.csv'

        for try_cnt in range(10):  # 최대 10번 시도
            try:
                # S3에 파일 업로드
                s3_res = self.s3_client.copy_object(
                    Bucket=self.s3_bucket_name,
                    CopySource=CopySource_path,
                    Key=Key_path
                )
                return None  # 성공 시 None 반환
            except Exception as e:
                # 업로드 실패 시 로그 메시지 생성
                log_message = f"Error saving data to S3 bucket {self.s3_bucket_name}: {e}"
                continue  # 재시도
        return log_message  # 실패 시 로그 메시지 반환


    # 로그를 기록하는 메서드
    def log_result(self, message):
        log_file_path = 'log/crawler.log'  # 로그 파일 경로
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 현재 시간 형식 지정
        log_message = f"{self.s3_bucket_name} [{now}] {message}\n"  # 로그 메시지 생성
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)  # 디렉토리가 없으면 생성
        # 로그 파일에 메시지 추가
        with open(log_file_path, 'a') as log_file:
            log_file.write(log_message)

# 한국수력원자력 데이터를 크롤링하는 클래스
class KHNPCrawler(DataCrawler):
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_region):
        # 상위 클래스 초기화 및 S3 버킷 이름 지정
        super().__init__(aws_access_key, aws_secret_key, data_portal_key_enc, 'khnp', s3_region)
        self.kinds = ['pwr', 'weather', 'air', 'radiorate', 'inoutwater', 'wastewater']  # 데이터 종류 리스트
        self.plants = {
            'pwr': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'weather': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'air':  ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'radiorate': ['WS', 'KR', 'YK', 'UJ', 'SU'],
            'inoutwater': ['WS', 'KR', 'YK', 'UJ'],
            'wastewater': ['WS', 'KR', 'YK'],
        }  # 데이터 별 발전소 리스트
        self.base_url = 'http://data.khnp.co.kr/environ/service/realtime/'  # 기본 URL

    def fetch_and_process(self, kind, plant):
        params = {'serviceKey': self.data_portal_key_enc, 'genName': plant}  # 요청 파라미터 설정
        df, log_message = self.request_data(self.base_url + kind, params)  # 데이터 요청
        file_path = f'raw/{kind}/{plant}'  # S3 파일 경로 설정
        if log_message:  # 요청 실패 시
            # 로그 메시지에 추가 정보 포함
            log_message = f'[Kind: {kind} GenName: {plant}] [Requesting data] {log_message}'
            self.log_result(log_message)  # 로그 기록
            copy_res = self.copy_latest_file(file_path)
            self.log_result('[Copying data] Copying the most recent data')
            if copy_res:
                log_message = f'[Saving to S3] {log_message}'  # 로그 메시지에 추가 정보 포함
                self.log_result(log_message)  # 로그 기록
        else:
            save_to_res = self.save_to_s3(df, file_path)  # 데이터 저장
            if save_to_res:  # 저장 실패 시
                # 로그 메시지에 추가 정보 포함
                log_message = f'[Kind: {kind} GenName: {plant}] [Saving to S3] {log_message}'
                self.log_result(log_message)  # 로그 기록

    def crawl_data(self):
        with ThreadPoolExecutor() as executor:
            futures = []
            for kind in self.kinds:  # 각 데이터 종류에 대해
                for plant in self.plants[kind]:  # 각 발전소에 대해
                    futures.append(executor.submit(self.fetch_and_process, kind, plant))
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # 결과를 가져옴으로써 예외를 발생시킴
                except Exception as e:
                    print(f"Error: {e}")

# 한국전력거래소 데이터를 크롤링하는 클래스
class KPXCrawler(DataCrawler):
    def __init__(self, aws_access_key, aws_secret_key, data_portal_key_enc, s3_region):
        # 상위 클래스 초기화 및 S3 버킷 이름 지정
        super().__init__(aws_access_key, aws_secret_key, data_portal_key_enc, 'kpx', s3_region)
        self.base_url = 'https://openapi.kpx.or.kr/openapi/sukub5mMaxDatetime/getSukub5mMaxDatetime/'  # 기본 URL

    # 데이터를 크롤링하는 메서드
    def crawl_data(self):
        params = {'serviceKey': self.data_portal_key_enc}  # 요청 파라미터 설정
        df, log_message = self.request_data(self.base_url, params)  # 데이터 요청
        file_path = 'raw/sukub'  # S3 파일 경로 설정
        if log_message:  # 요청 실패 시
            log_message = f'[Requesting data] {log_message}'  # 로그 메시지에 추가 정보 포함
            self.log_result(log_message)  # 로그 기록
            
        else:
            save_to_res = self.save_to_s3(df, file_path)  # 데이터 저장
            if save_to_res:  # 저장 실패 시
                log_message = f'[Saving to S3] {log_message}'  # 로그 메시지에 추가 정보 포함
                self.log_result(log_message)  # 로그 기록

# 메인 실행 부분
if __name__ == "__main__":
    load_dotenv()  # 환경 변수 로드
    AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))  # AWS 접근 키 로드 및 디코딩
    AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))  # AWS 비밀 키 로드 및 디코딩
    DATA_PORTAL_KEY_ENC = unquote(os.environ.get('DATA_PORTAL_KEY_ENC'))  # 데이터 포털 키 로드 및 디코딩
    S3_REGION = 'ap-northeast-2'  # S3 리전 설정

    # 각 크롤러 인스턴스 생성 및 데이터 크롤링 실행
    khnp_crawler = KHNPCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)
    kpx_crawler = KPXCrawler(AWS_ACCESS_KEY, AWS_SECRET_KEY, DATA_PORTAL_KEY_ENC, S3_REGION)

    khnp_crawler.crawl_data()  # 한국수력원자력 데이터 크롤링 실행
    kpx_crawler.crawl_data()  # 한국전력거래소 데이터 크롤링 실행
