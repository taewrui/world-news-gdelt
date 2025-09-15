from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# 기본 DAG 설정
default_args = {
    'owner': 'gdelt',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def test_postgres_connection(**context):
    """PostgreSQL 연결 테스트 함수"""
    try:
        # PostgresHook을 사용하여 연결 테스트
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 연결 테스트
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # 기본 연결 테스트 쿼리
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"PostgreSQL 버전: {version[0]}")
        
        # 데이터베이스 목록 조회
        cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = cursor.fetchall()
        print(f"사용 가능한 데이터베이스: {[db[0] for db in databases]}")
        
        # 현재 데이터베이스의 스키마 목록 조회
        cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cursor.fetchall()
        print(f"스키마 목록: {[schema[0] for schema in schemas]}")
        
        # 연결 정보 출력
        cursor.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
        conn_info = cursor.fetchone()
        print(f"연결 정보 - 데이터베이스: {conn_info[0]}, 사용자: {conn_info[1]}, 서버: {conn_info[2]}:{conn_info[3]}")
        
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL 연결 테스트 성공!")
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ PostgreSQL 연결 테스트 실패: {str(e)}")
        raise e

def test_table_operations(**context):
    """테이블 생성/삭제 테스트 함수"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_gdelt')
        
        # 테스트 테이블 생성
        create_sql = """
        CREATE TABLE IF NOT EXISTS test_connection (
            id SERIAL PRIMARY KEY,
            test_name VARCHAR(50),
            test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            test_result VARCHAR(20)
        );
        """
        postgres_hook.run(create_sql)
        print("✅ 테스트 테이블 생성 완료")
        
        # 테스트 데이터 삽입
        insert_sql = """
        INSERT INTO test_connection (test_name, test_result) 
        VALUES ('airflow_connection_test', 'SUCCESS');
        """
        postgres_hook.run(insert_sql)
        print("✅ 테스트 데이터 삽입 완료")
        
        # 데이터 조회
        select_sql = "SELECT * FROM test_connection ORDER BY id DESC LIMIT 5;"
        records = postgres_hook.get_records(select_sql)
        print(f"✅ 조회된 레코드: {records}")
        
        # 테스트 테이블 삭제
        drop_sql = "DROP TABLE IF EXISTS test_connection;"
        postgres_hook.run(drop_sql)
        print("✅ 테스트 테이블 삭제 완료")
        
        return "SUCCESS"
        
    except Exception as e:
        print(f"❌ 테이블 작업 테스트 실패: {str(e)}")
        raise e

# DAG 정의
dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='PostgreSQL 연결 및 기본 작업 테스트',
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    tags=['test', 'postgres', 'connection']
)

# Task 1: 기본 연결 테스트
test_connection_task = PythonOperator(
    task_id='test_connection',
    python_callable=test_postgres_connection,
    dag=dag
)

# Task 2: SQL을 통한 버전 확인
check_version_task = PostgresOperator(
    task_id='check_postgres_version',
    postgres_conn_id='postgres_gdelt',
    sql="SELECT 'PostgreSQL Version: ' || version() as info;",
    dag=dag
)

# Task 3: 현재 시간 확인
check_time_task = PostgresOperator(
    task_id='check_current_time',
    postgres_conn_id='postgres_gdelt',
    sql="SELECT 'Current Time: ' || NOW() as current_time;",
    dag=dag
)

# Task 4: 테이블 작업 테스트
test_table_ops_task = PythonOperator(
    task_id='test_table_operations',
    python_callable=test_table_operations,
    dag=dag
)

# Task 5: 스키마 정보 확인
check_schema_task = PostgresOperator(
    task_id='check_schema_info',
    postgres_conn_id='postgres_gdelt',
    sql="""
    SELECT 
        'Database: ' || current_database() as db_info,
        'User: ' || current_user as user_info,
        'Schema Count: ' || count(*) as schema_count
    FROM information_schema.schemata;
    """,
    dag=dag
)

# Task 의존성 설정
test_connection_task >> [check_version_task, check_time_task, check_schema_task]
[check_version_task, check_time_task, check_schema_task] >> test_table_ops_task
