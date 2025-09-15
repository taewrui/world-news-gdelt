import pandas as pd
import os
from sqlalchemy import create_engine
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.scripts.utils import get_gdelt_timestamp


def load_export_to_postgres(**context):
    """Export 데이터를 PostgreSQL에 로드하는 함수"""
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        raise ValueError("타임스탬프를 가져올 수 없습니다")
    
    # 로컬 /tmp에서 전처리된 파일들 찾기 - process 태스크가 생성하는 파일명과 일치
    local_files = [
        f"/tmp/{timestamp}.export_flat.csv",
        f"/tmp/{timestamp}.translation.export_flat.csv"
    ]
    
    # PostgreSQL Hook 초기화
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    loaded_files = []
    missing_files = []
    
    for local_file in local_files:
        try:
            # 파일 존재 여부 확인
            if not os.path.exists(local_file):
                missing_files.append(local_file)
                print(f"File {local_file} not found")
                continue
                
            # CSV 파일 읽기
            df = pd.read_csv(local_file)
            
            if df.empty:
                print(f"Warning: {local_file} is empty, skipping...")
                continue
            
            # PostgreSQL에 데이터 삽입
            engine = postgres_hook.get_sqlalchemy_engine()
            df.to_sql('raw_export', engine, if_exists='append', index=False)
            
            loaded_files.append(local_file)
            print(f"Successfully loaded {local_file} to PostgreSQL ({len(df)} records)")
            
        except Exception as e:
            print(f"Error loading {local_file}: {e}")
            raise Exception(f"PostgreSQL 로드 실패: {local_file} - {str(e)}")
    
    # 최소 1개 파일은 로드되어야 함
    if not loaded_files:
        error_msg = f"처리된 export 파일을 찾을 수 없습니다. 누락된 파일: {missing_files}"
        print(error_msg)
        raise FileNotFoundError(error_msg)


def load_mentions_to_postgres(**context):
    """Mentions 데이터를 PostgreSQL에 로드하는 함수"""
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        raise ValueError("타임스탬프를 가져올 수 없습니다")
    
    # 로컬 /tmp에서 전처리된 파일들 찾기 - process 태스크가 생성하는 파일명과 일치
    local_files = [
        f"/tmp/{timestamp}.mentions_flat.csv",
        f"/tmp/{timestamp}.translation.mentions_flat.csv"
    ]
    
    # PostgreSQL Hook 초기화
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    loaded_files = []
    missing_files = []
    
    for local_file in local_files:
        try:
            # 파일 존재 여부 확인
            if not os.path.exists(local_file):
                missing_files.append(local_file)
                print(f"File {local_file} not found")
                continue
                
            # CSV 파일 읽기
            df = pd.read_csv(local_file)
            
            if df.empty:
                print(f"Warning: {local_file} is empty, skipping...")
                continue
            
            # PostgreSQL에 데이터 삽입
            engine = postgres_hook.get_sqlalchemy_engine()
            df.to_sql('raw_mentions', engine, if_exists='append', index=False)
            
            loaded_files.append(local_file)
            print(f"Successfully loaded {local_file} to PostgreSQL ({len(df)} records)")
            
        except Exception as e:
            print(f"Error loading {local_file}: {e}")
            raise Exception(f"PostgreSQL 로드 실패: {local_file} - {str(e)}")
    
    # 최소 1개 파일은 로드되어야 함
    if not loaded_files:
        error_msg = f"처리된 mentions 파일을 찾을 수 없습니다. 누락된 파일: {missing_files}"
        print(error_msg)
        raise FileNotFoundError(error_msg)


def load_gkg_to_postgres(**context):
    """GKG 데이터를 PostgreSQL에 로드하는 함수"""
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        raise ValueError("타임스탬프를 가져올 수 없습니다")
    
    # 로컬 /tmp에서 전처리된 파일들 찾기 - process 태스크가 생성하는 파일명과 일치
    local_files = [
        f"/tmp/{timestamp}.gkg_flat.csv",
        f"/tmp/{timestamp}.translation.gkg_flat.csv"
    ]
    
    # PostgreSQL Hook 초기화
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    loaded_files = []
    missing_files = []
    
    for local_file in local_files:
        try:
            # 파일 존재 여부 확인
            if not os.path.exists(local_file):
                missing_files.append(local_file)
                print(f"File {local_file} not found")
                continue
                
            # CSV 파일 읽기
            df = pd.read_csv(local_file)
            
            if df.empty:
                print(f"Warning: {local_file} is empty, skipping...")
                continue
            
            # PostgreSQL에 데이터 삽입
            engine = postgres_hook.get_sqlalchemy_engine()
            df.to_sql('raw_gkg', engine, if_exists='append', index=False)
            
            loaded_files.append(local_file)
            print(f"Successfully loaded {local_file} to PostgreSQL ({len(df)} records)")
            
        except Exception as e:
            print(f"Error loading {local_file}: {e}")
            raise Exception(f"PostgreSQL 로드 실패: {local_file} - {str(e)}")
    
    # 최소 1개 파일은 로드되어야 함
    if not loaded_files:
        error_msg = f"처리된 gkg 파일을 찾을 수 없습니다. 누락된 파일: {missing_files}"
        print(error_msg)
        raise FileNotFoundError(error_msg)
