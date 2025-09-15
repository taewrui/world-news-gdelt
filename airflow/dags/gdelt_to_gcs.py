from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta

from dags.scripts.utils import (
    download_gdelt_zip,
    unzip_file,
    cleanup_files
)

from dags.scripts.process.export import process_export_files
from dags.scripts.process.mentions import process_mentions_files
from dags.scripts.process.gkg import process_gkg_files
from dags.scripts.load_to_postgres import (
    load_export_to_postgres,
    load_mentions_to_postgres,
    load_gkg_to_postgres
)
from dags.scripts.monitoring import (
    collect_data_quality_metrics,
    collect_dag_execution_metrics
)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gdelt_pipeline',
    default_args=default_args,
    description='GDELT pipeline',
    schedule='*/15 * * * *',
    start_date=datetime(2025, 6, 1),
    catchup=True,
    max_active_runs=1,
    tags=['gdelt'],
)

with dag:
    download_export = PythonOperator(
        task_id='download_export',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'export',
            'is_translation': False,
        },
    )

    unzip_export = PythonOperator(
        task_id='unzip_export',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_export') }}",
            'extract_dir': '/tmp/gdelt/extract/export',
        },
    )

    upload_export_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_export_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_export') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.export.CSV') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    download_translation_export = PythonOperator(
        task_id='download_translation_export',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'export',
            'is_translation': True,
        },
    )

    unzip_translation_export = PythonOperator(
        task_id='unzip_translation_export',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_translation_export') }}",
            'extract_dir': '/tmp/gdelt/extract/translation_export',
        },
    )

    upload_translation_export_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_translation_export_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_translation_export') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.translation.export.CSV') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    download_mentions = PythonOperator(
        task_id='download_mentions',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'mentions',
            'is_translation': False,
        },
    )

    unzip_mentions = PythonOperator(
        task_id='unzip_mentions',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_mentions') }}",
            'extract_dir': '/tmp/gdelt/extract/mentions',
        },
    )

    upload_mentions_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_mentions_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_mentions') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.mentions.CSV') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    download_translation_mentions = PythonOperator(
        task_id='download_translation_mentions',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'mentions',
            'is_translation': True,
        },
    )

    unzip_translation_mentions = PythonOperator(
        task_id='unzip_translation_mentions',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_translation_mentions') }}",
            'extract_dir': '/tmp/gdelt/extract/translation_mentions',
        },
    )

    upload_translation_mentions_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_translation_mentions_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_translation_mentions') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.translation.mentions.CSV') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    download_gkg = PythonOperator(
        task_id='download_gkg',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'gkg',
            'is_translation': False,
        },
    )

    unzip_gkg = PythonOperator(
        task_id='unzip_gkg',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_gkg') }}",
            'extract_dir': '/tmp/gdelt/extract/gkg',
        },
    )

    upload_gkg_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_gkg_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_gkg') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.gkg.csv') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    download_translation_gkg = PythonOperator(
        task_id='download_translation_gkg',
        python_callable=download_gdelt_zip,
        op_kwargs={
            'download_dir': '/tmp/gdelt',
            'file_type': 'gkg',
            'is_translation': True,
        },
    )

    unzip_translation_gkg = PythonOperator(
        task_id='unzip_translation_gkg',
        python_callable=unzip_file,
        op_kwargs={
            'zip_path': "{{ ti.xcom_pull(task_ids='download_translation_gkg') }}",
            'extract_dir': '/tmp/gdelt/extract/translation_gkg',
        },
    )

    upload_translation_gkg_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_translation_gkg_to_gcs',
        src="{{ ti.xcom_pull(task_ids='unzip_translation_gkg') }}",
        dst="{{ (logical_date - macros.timedelta(minutes=15)).strftime('gdelt/raw/%Y/%m/%d/%H/%M/') }}{{ (logical_date - macros.timedelta(minutes=15)).strftime('%Y%m%d%H%M%S.translation.gkg.csv') }}",
        bucket='gdelt-csv-egd',
        gcp_conn_id='google_cloud_default',
        mime_type='text/csv',
    )

    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup_files,
    )

    process_export = PythonOperator(
        task_id='process_export',
        python_callable=process_export_files,
    )

    process_mentions = PythonOperator(
        task_id='process_mentions',
        python_callable=process_mentions_files,
    )

    process_gkg = PythonOperator(
        task_id='process_gkg',
        python_callable=process_gkg_files,
    )

    # PostgreSQL 로드 태스크들
    load_export_to_pg = PythonOperator(
        task_id='load_export_to_pg',
        python_callable=load_export_to_postgres,
    )

    load_mentions_to_pg = PythonOperator(
        task_id='load_mentions_to_pg',
        python_callable=load_mentions_to_postgres,
    )

    load_gkg_to_pg = PythonOperator(
        task_id='load_gkg_to_pg',
        python_callable=load_gkg_to_postgres,
    )

    # 모니터링 태스크들
    collect_quality_metrics = PythonOperator(
        task_id='collect_quality_metrics',
        python_callable=collect_data_quality_metrics,
    )

    collect_execution_metrics = PythonOperator(
        task_id='collect_execution_metrics',
        python_callable=collect_dag_execution_metrics,
    )

    
    # Export
    download_export >> unzip_export >> upload_export_to_gcs
    download_translation_export >> unzip_translation_export >> upload_translation_export_to_gcs
    
    # Mentions
    download_mentions >> unzip_mentions >> upload_mentions_to_gcs
    download_translation_mentions >> unzip_translation_mentions >> upload_translation_mentions_to_gcs
    
    # GKG
    download_gkg >> unzip_gkg >> upload_gkg_to_gcs
    download_translation_gkg >> unzip_translation_gkg >> upload_translation_gkg_to_gcs

    # 모든 업로드 완료 후 전처리
    upload_export_to_gcs >> process_export
    upload_translation_export_to_gcs >> process_export
    upload_mentions_to_gcs >> process_mentions
    upload_translation_mentions_to_gcs >> process_mentions
    upload_gkg_to_gcs >> process_gkg
    upload_translation_gkg_to_gcs >> process_gkg
    
    # 전처리 완료 후 PostgreSQL 로드
    process_export >> load_export_to_pg
    process_mentions >> load_mentions_to_pg
    process_gkg >> load_gkg_to_pg
    
    # 모든 PostgreSQL 로드 완료 후 품질 메트릭 수집
    [load_export_to_pg, load_mentions_to_pg, load_gkg_to_pg] >> collect_quality_metrics
    
    # 품질 메트릭 수집 후 실행 메트릭 수집
    collect_quality_metrics >> collect_execution_metrics
    
    # 실행 메트릭 수집 후 정리
    collect_execution_metrics >> cleanup
    