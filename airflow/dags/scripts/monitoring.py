"""
GDELT 파이프라인 모니터링 및 품질 체크 함수들
"""

import pandas as pd
import os
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DagRun, TaskInstance
from airflow import settings
from sqlalchemy.orm import sessionmaker
from dags.scripts.utils import get_gdelt_timestamp


def collect_data_quality_metrics(**context):
    """
    각 파일 타입별 데이터 품질 메트릭을 수집하고 PostgreSQL에 저장
    """
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date'] or context['logical_date']
    
    # Proxy 객체를 실제 datetime 객체로 변환
    if hasattr(execution_date, '_get_object'):
        execution_date = execution_date._get_object()
    elif hasattr(execution_date, '__wrapped__'):
        execution_date = execution_date.__wrapped__
    elif str(type(execution_date)) == "<class 'airflow.utils.context.Proxy'>":
        # Proxy 객체에서 실제 값 추출
        execution_date = execution_date.__dict__.get('_value', execution_date)
        if hasattr(execution_date, 'replace'):
            # DateTime 객체인 경우 Python datetime으로 변환
            try:
                from datetime import datetime as dt
                if hasattr(execution_date, 'year'):
                    execution_date = dt(
                        execution_date.year, execution_date.month, execution_date.day,
                        execution_date.hour, execution_date.minute, execution_date.second,
                        execution_date.microsecond, execution_date.tzinfo
                    )
            except:
                pass
    
    print(f"Converted execution_date: {execution_date} (type: {type(execution_date)})")
    
    # PostgreSQL Hook 초기화
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 타임스탬프 계산
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        print("Could not determine timestamp")
        return
    
    # 파일 타입별 메트릭 수집
    file_types = [
        'export', 'translation.export',
        'mentions', 'translation.mentions', 
        'gkg', 'translation.gkg'
    ]
    
    quality_metrics = []
    
    for file_type in file_types:
        try:
            # 원본 파일 경로 결정
            if 'translation' in file_type:
                base_type = file_type.replace('translation.', '')
                # 실제 unzip 경로와 일치: /tmp/gdelt/extract/translation_export, translation_mentions, translation_gkg
                extract_subdir = f"translation_{base_type}"
                file_extension = ".csv" if base_type == "gkg" else ".CSV"
                original_file = f"/tmp/gdelt/extract/{extract_subdir}/{timestamp}.translation.{base_type}{file_extension}"
                processed_file = f"/tmp/{timestamp}.translation.{base_type}_flat.csv"
            else:
                # 실제 unzip 경로와 일치: /tmp/gdelt/extract/export, mentions, gkg
                file_extension = ".csv" if file_type == "gkg" else ".CSV"
                original_file = f"/tmp/gdelt/extract/{file_type}/{timestamp}.{file_type}{file_extension}"
                processed_file = f"/tmp/{timestamp}.{file_type}_flat.csv"
            
            # 원본 레코드 수 계산
            original_records = 0
            if os.path.exists(original_file):
                try:
                    with open(original_file, 'r', encoding='utf-8-sig') as f:
                        original_records = sum(1 for line in f)
                except Exception as e:
                    print(f"Error reading original file {original_file}: {e}")
                    original_records = -1
            
            # 처리된 레코드 수 계산
            processed_records = 0
            if os.path.exists(processed_file):
                try:
                    df = pd.read_csv(processed_file)
                    processed_records = len(df)
                except Exception as e:
                    print(f"Error reading processed file {processed_file}: {e}")
                    processed_records = -1
            
            # PostgreSQL에서 적재된 레코드 수 확인
            loaded_records = 0
            table_mapping = {
                'export': 'raw_export',
                'translation.export': 'raw_export',
                'mentions': 'raw_mentions', 
                'translation.mentions': 'raw_mentions',
                'gkg': 'raw_gkg',
                'translation.gkg': 'raw_gkg'
            }
            
            table_name = table_mapping.get(file_type, 'unknown')
            if table_name != 'unknown':
                try:
                    # 해당 실행 시간대의 레코드 수 확인 (정확한 카운트를 위해 dateadded 기준)
                    count_query = f"""
                    SELECT COUNT(*) as cnt FROM {table_name} 
                    WHERE dateadded >= %s AND dateadded < %s + INTERVAL '15 minutes'
                    """
                    result = postgres_hook.get_first(count_query, parameters=[execution_date, execution_date])
                    loaded_records = result[0] if result else 0
                except Exception as e:
                    print(f"Error counting loaded records for {table_name}: {e}")
                    loaded_records = -1
            
            # 품질 체크 (원본 -> 처리 -> 적재 과정에서 데이터 손실 없는지)
            quality_check_passed = True
            error_message = None
            
            if original_records == -1 or processed_records == -1 or loaded_records == -1:
                quality_check_passed = False
                error_message = "파일 읽기 또는 DB 쿼리 오류"
            elif original_records == 0:
                quality_check_passed = False
                error_message = "원본 파일이 비어있음"
            elif processed_records < original_records * 0.95:  # 5% 이상 손실시 실패
                quality_check_passed = False
                error_message = f"처리 중 데이터 손실 과다: {original_records} -> {processed_records}"
            elif loaded_records < processed_records * 0.95:  # 5% 이상 손실시 실패
                quality_check_passed = False
                error_message = f"적재 중 데이터 손실 과다: {processed_records} -> {loaded_records}"
            
            quality_metrics.append({
                'dag_id': dag_id,
                'run_id': run_id,
                'execution_date': execution_date,
                'file_type': file_type,
                'original_records': original_records,
                'processed_records': processed_records,
                'loaded_records': loaded_records,
                'quality_check_passed': quality_check_passed,
                'error_message': error_message
            })
            
            print(f"{file_type}: 원본={original_records}, 처리={processed_records}, 적재={loaded_records}, 품질체크={'통과' if quality_check_passed else '실패'}")
            
        except Exception as e:
            print(f"Error processing quality metrics for {file_type}: {e}")
            quality_metrics.append({
                'dag_id': dag_id,
                'run_id': run_id,
                'execution_date': execution_date,
                'file_type': file_type,
                'original_records': -1,
                'processed_records': -1,
                'loaded_records': -1,
                'quality_check_passed': False,
                'error_message': str(e)
            })
    
    # PostgreSQL에 품질 메트릭 저장
    if quality_metrics:
        try:
            for metric in quality_metrics:
                insert_sql = """
                INSERT INTO data_quality_metrics 
                (dag_id, run_id, execution_date, file_type, original_records, processed_records, loaded_records, quality_check_passed, error_message)
                VALUES (%(dag_id)s, %(run_id)s, %(execution_date)s, %(file_type)s, %(original_records)s, %(processed_records)s, %(loaded_records)s, %(quality_check_passed)s, %(error_message)s)
                ON CONFLICT (dag_id, run_id, file_type) 
                DO UPDATE SET 
                    original_records = EXCLUDED.original_records,
                    processed_records = EXCLUDED.processed_records,
                    loaded_records = EXCLUDED.loaded_records,
                    quality_check_passed = EXCLUDED.quality_check_passed,
                    error_message = EXCLUDED.error_message
                """
                
                postgres_hook.run(insert_sql, parameters=metric)
            
            print(f"품질 메트릭 {len(quality_metrics)}개 저장 완료")
            
        except Exception as e:
            print(f"Error saving quality metrics: {e}")
    
    return quality_metrics


def collect_dag_execution_metrics(**context):
    """
    Airflow 메타 DB에서 DAG 실행 메트릭을 수집하고 PostgreSQL에 저장
    """
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    execution_date = context['execution_date'] or context['logical_date']
    
    # Proxy 객체를 실제 datetime 객체로 변환
    if hasattr(execution_date, '_get_object'):
        execution_date = execution_date._get_object()
    elif hasattr(execution_date, '__wrapped__'):
        execution_date = execution_date.__wrapped__
    elif str(type(execution_date)) == "<class 'airflow.utils.context.Proxy'>":
        # Proxy 객체에서 실제 값 추출
        execution_date = execution_date.__dict__.get('_value', execution_date)
        if hasattr(execution_date, 'replace'):
            # DateTime 객체인 경우 Python datetime으로 변환
            try:
                from datetime import datetime as dt
                if hasattr(execution_date, 'year'):
                    execution_date = dt(
                        execution_date.year, execution_date.month, execution_date.day,
                        execution_date.hour, execution_date.minute, execution_date.second,
                        execution_date.microsecond, execution_date.tzinfo
                    )
            except:
                pass
    
    # PostgreSQL Hook 초기화
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Airflow 메타 DB 세션 생성
        Session = sessionmaker(bind=settings.engine)
        session = Session()
        
        # DagRun 정보 조회
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.run_id == run_id
        ).first()
        
        if dag_run:
            # 실행 시간 계산
            duration_seconds = None
            if dag_run.start_date and dag_run.end_date:
                duration_seconds = int((dag_run.end_date - dag_run.start_date).total_seconds())
            elif dag_run.start_date:
                # 아직 실행 중인 경우 현재 시간까지의 duration
                duration_seconds = int((datetime.now() - dag_run.start_date).total_seconds())
            
            # 메트릭 저장
            execution_metric = {
                'dag_id': dag_id,
                'run_id': run_id,
                'execution_date': execution_date,
                'start_date': dag_run.start_date,
                'end_date': dag_run.end_date,
                'duration_seconds': duration_seconds,
                'state': dag_run.state
            }
            
            # PostgreSQL에 저장
            insert_sql = """
            INSERT INTO pipeline_execution_metrics 
            (dag_id, run_id, execution_date, start_date, end_date, duration_seconds, state)
            VALUES (%(dag_id)s, %(run_id)s, %(execution_date)s, %(start_date)s, %(end_date)s, %(duration_seconds)s, %(state)s)
            ON CONFLICT (dag_id, run_id) 
            DO UPDATE SET 
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                duration_seconds = EXCLUDED.duration_seconds,
                state = EXCLUDED.state
            """
            
            postgres_hook.run(insert_sql, parameters=execution_metric)
            
            print(f"DAG 실행 메트릭 저장 완료: {dag_id} ({run_id}) - 상태: {dag_run.state}, 실행시간: {duration_seconds}초")
            
            session.close()
            return execution_metric
            
        else:
            print(f"DagRun not found: {dag_id} ({run_id})")
            session.close()
            return None
            
    except Exception as e:
        print(f"Error collecting DAG execution metrics: {e}")
        return None
