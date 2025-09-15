import requests
import zipfile
import os
import shutil
from datetime import timedelta


def download_gdelt_zip(download_dir, file_type='export', is_translation=False, **context):
    execution_date = context.get('execution_date') or context.get('logical_date')
    os.makedirs(download_dir, exist_ok=True)
    prev_time = execution_date - timedelta(minutes=15)
    timestamp = prev_time.strftime('%Y%m%d%H%M%S')
    
    # 번역 파일 접두어
    translation_prefix = 'translation.' if is_translation else ''
    
    # 파일 타입별 URL 구성
    if file_type == 'export':
        url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.{translation_prefix}export.CSV.zip"
        filename = f"{timestamp}.{translation_prefix}export.CSV.zip"
    elif file_type == 'mentions':
        url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.{translation_prefix}mentions.CSV.zip"
        filename = f"{timestamp}.{translation_prefix}mentions.CSV.zip"
    elif file_type == 'gkg':
        url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.{translation_prefix}gkg.csv.zip"
        filename = f"{timestamp}.{translation_prefix}gkg.csv.zip"
    else:
        raise ValueError(f"Unsupported file_type: {file_type}")
    
    local_zip_path = os.path.join(download_dir, filename)
    response = requests.get(url)
    response.raise_for_status()
    with open(local_zip_path, 'wb') as f:
        f.write(response.content)
    return local_zip_path


def unzip_file(zip_path, extract_dir, **context):
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    # 확장자가 .csv 또는 .CSV인 파일
    for fname in os.listdir(extract_dir):
        if fname.endswith('.CSV') or fname.endswith('.csv'):
            return os.path.join(extract_dir, fname)
    raise FileNotFoundError('No CSV file found after unzip')


def cleanup_files(**context): 
    """임시로 다운로드한 파일들과 처리된 파일들을 모두 삭제"""
    import glob
    
    # GDELT 추출 디렉토리 삭제
    shutil.rmtree('/tmp/gdelt', ignore_errors=True)
    
    # 처리된 flat 파일들 삭제
    flat_files = glob.glob('/tmp/*_flat.csv')
    for file_path in flat_files:
        try:
            os.remove(file_path)
            print(f"Removed processed file: {file_path}")
        except Exception as e:
            print(f"Error removing {file_path}: {e}")
    
    print("Cleanup completed")


def get_gdelt_timestamp(**context):
    """실행 날짜로부터 GDELT 타임스탬프를 계산하는 함수"""
    execution_date = context.get('execution_date') or context.get('logical_date')
    if execution_date is None:
        print("No execution_date found in context")
        return None
    prev_time = execution_date - timedelta(minutes=15)
    timestamp = prev_time.strftime('%Y%m%d%H%M%S')
    return timestamp
