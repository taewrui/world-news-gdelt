from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.scripts.daily_report import (
    generate_daily_quality_report,
    test_slack_connection
)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# 일일 리포트 DAG - 매일 오전 9시에 실행
daily_report_dag = DAG(
    'gdelt_daily_report',
    default_args=default_args,
    description='GDELT pipeline daily quality report',
    schedule='0 9 * * *',  # 매일 오전 9시
    start_date=datetime(2025, 6, 2),  # 메인 DAG 다음날부터 시작
    catchup=False,  # 과거 날짜 실행하지 않음
    max_active_runs=1,
    tags=['gdelt', 'report', 'monitoring'],
)

with daily_report_dag:
    
    # 일일 품질 리포트 생성 및 Slack 전송
    generate_daily_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_quality_report,
        provide_context=True,
    )

    # Slack 연결 테스트 (선택적으로 사용)
    test_slack = PythonOperator(
        task_id='test_slack',
        python_callable=test_slack_connection,
        provide_context=True,
    )

    # 의존성: 필요시 Slack 테스트 후 리포트 생성
    # test_slack >> generate_daily_report
    
    # 또는 바로 리포트 생성
    generate_daily_report
