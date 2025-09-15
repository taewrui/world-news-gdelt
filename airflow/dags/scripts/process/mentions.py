import pandas as pd
import csv
import os
from dags.scripts.converters import *
from dags.scripts.utils import get_gdelt_timestamp
from dags.scripts.schemas import MENTIONS_SCHEMA, apply_dataframe_schema
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def process_mentions_files(**context):
    """mentions 파일명을 가진 CSV 파일들을 전처리하고 GCS에 업로드하는 함수"""
    
    # 타임스탬프 계산
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        return
    
    # /tmp/gdelt/extract에서 해당 타임스탬프의 mentions 파일들 찾기
    extract_dir = '/tmp/gdelt/extract'
    mentions_files = []
    
    # 모든 하위 디렉토리에서 특정 타임스탬프의 mentions가 포함된 CSV 파일 검색
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if ('mentions' in file.lower() and 
                timestamp in file and 
                file.endswith('.CSV')):
                mentions_files.append(os.path.join(root, file))
    
    if not mentions_files:
        print(f"No mentions CSV files found for timestamp {timestamp}")
        return

    # GCS Hook 초기화
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = 'gdelt-csv-egd'
    
    processed_files = []
    
    for file_path in mentions_files:
        print(f"Processing file: {file_path}")
        
        flattened_rows = []
        
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            # 1차 구분 쉼표, 2차 구분 탭
            reader = csv.reader(f, delimiter='\t')
            
            # 14번째 필드 - 기계 번역 정보
            for row in reader:
                new_row = row.copy()
                
                # new_row[1], new_row[2] 날짜시간 형식 변환: yyyymmddhhmmss -> yyyy-mm-dd hh:mm:ss
                if len(new_row) > 1 and new_row[1]:
                    new_row[1] = convert_datetime_format(new_row[1])
                if len(new_row) > 2 and new_row[2]:
                    new_row[2] = convert_datetime_format(new_row[2])
                
                if len(row) > 14 and row[14]:
                    converted = convert_to_json_array(row[14], ';')
                    new_row[14] = converted if converted is not None else ""
                
                new_row = new_row[:16]
                flattened_rows.append(new_row)

        columns = MENTIONS_SCHEMA['columns']
        
        df = pd.DataFrame(flattened_rows, columns=columns)
        
        # 스키마 적용을 통한 데이터 타입 설정
        df = apply_dataframe_schema(df, MENTIONS_SCHEMA)
        
        # 스키마 정보를 로그에 출력
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame schema:")
        print(df.dtypes)
        print(f"DataFrame info:")
        print(df.info(verbose=True))
        
        # GCS에 직접 업로드
        base_name = os.path.basename(file_path).replace('.CSV', '')
        
        # 실행 시간 기반 폴더 구조 생성
        execution_date = context.get('execution_date') or context.get('logical_date')
        if execution_date:
            from datetime import timedelta
            prev_time = execution_date - timedelta(minutes=15)
            folder_path = prev_time.strftime('gdelt/processed/%Y/%m/%d/%H/%M/')
        else:
            folder_path = 'gdelt/processed/'
        
        gcs_path = f"{folder_path}{base_name}_flat.csv"
        
        # DataFrame을 CSV 형식으로 저장
        temp_file = f"/tmp/{base_name}_flat.csv"
        df.to_csv(temp_file, index=False, encoding='utf-8')
        
        # GCSHook을 사용하여 업로드
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=gcs_path,
            filename=temp_file,
            mime_type='text/csv'
        )
        
        # 임시 파일은 PostgreSQL 로드를 위해 유지 (cleanup 태스크에서 정리)
        print(f"Processed file saved locally at: {temp_file}")
        print(f"Processed file uploaded to GCS: {gcs_path}")
        
        processed_files.append(gcs_path)
    
    return processed_files