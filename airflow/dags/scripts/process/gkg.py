import pandas as pd
import csv
import os
from dags.scripts.converters import *
from dags.scripts.utils import get_gdelt_timestamp
from dags.scripts.schemas import GKG_SCHEMA, apply_dataframe_schema
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def process_gkg_files(**context):
    """gkg 파일명을 가진 CSV 파일들을 전처리하고 GCS에 업로드하는 함수"""
    
    # 타임스탬프 계산
    timestamp = get_gdelt_timestamp(**context)
    if timestamp is None:
        return
    
    # /tmp/gdelt/extract에서 해당 타임스탬프의 gkg 파일들 찾기
    extract_dir = '/tmp/gdelt/extract'
    gkg_files = []
    
    # 모든 하위 디렉토리에서 특정 타임스탬프의 gkg가 포함된 CSV 파일 검색
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            if ('gkg' in file.lower() and 
                timestamp in file and 
                file.endswith('.csv')):
                gkg_files.append(os.path.join(root, file))
    
    if not gkg_files:
        print(f"No gkg CSV files found for timestamp {timestamp}")
        return
    
    # GCS Hook 초기화
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = 'gdelt-csv-egd'
    
    processed_files = []
    
    for file_path in gkg_files:
        print(f"Processing file: {file_path}")
        
        flattened_rows = []
        
        # 여러 인코딩을 시도하여 파일 읽기
        encodings = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    file_content = f.read()
                print(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            print(f"Could not read file {file_path} with any encoding")
            continue
        
        # 파일 내용을 줄별로 분리하고 CSV reader로 처리
        lines = file_content.strip().split('\n')
        
        for line in lines:
            # 탭으로 구분된 행을 파싱
            row = line.split('\t')
            new_row = row.copy()
            
            """
            - 삭제할 컬럼: 5, 7, 9, 11, 13
            - 배열로 변환할 컬럼: 19, 20, 21, 25
            - 가변 스키마(키:값 배열): 8, 12, 14, 17, 23
            - 고정 스키마: 6, 10, 15, 16, 22, 24
            """
            
            # 배열로 변환
            for i in [19, 20, 21, 25]:
                if len(new_row) > i and new_row[i]:
                    try:
                        import json
                        # 값이 이미 JSON 형태인지 확인
                        value = new_row[i].strip()
                        if value.startswith(('[', '{')):
                            # JSON 형태라면 유효성 검사
                            json.loads(value)
                            # 유효한 JSON이면 그대로 유지
                        else:
                            # JSON이 아니라면 배열로 변환
                            converted = convert_to_json_array(value)
                            new_row[i] = converted if converted is not None else '[]'
                    except json.JSONDecodeError:
                        # JSON 파싱 실패하면 배열로 변환 시도
                        try:
                            converted = convert_to_json_array(new_row[i])
                            new_row[i] = converted if converted is not None else '[]'
                        except Exception as conv_error:
                            print(f"Error converting column {i} to array: {conv_error}")
                            new_row[i] = '[]'
                    except Exception as e:
                        print(f"Error processing column {i} value '{new_row[i][:100]}...': {e}")
                        new_row[i] = '[]'
                else:
                    new_row[i] = '[]'
            
            # 딕셔너리로 변환(가변 스키마)
            for i in [8, 12, 14, 17, 23]:
                if len(new_row) > i and new_row[i]:
                    try:
                        d1, d2 = (',', ':') if i == 17 else (';', ',')
                        dict_result = edit_to_dict(new_row[i], d1, d2)
                        if dict_result:
                            import json
                            new_row[i] = json.dumps(dict_result, ensure_ascii=False)
                        else:
                            new_row[i] = "{}"
                    except Exception as e:
                        print(f"Error converting column {i} to dict: {e}")
                        new_row[i] = "{}"
                else:
                    new_row[i] = "{}"
            
            # v2 스키마 변환
            if len(new_row) > 6: new_row[6] = edit_v2_counts(new_row[6]) or ""
            if len(new_row) > 10: new_row[10] = edit_v2_locations(new_row[10]) or ""
            # edit_v1_tone은 이제 7개 값의 리스트를 반환하므로 별도 처리
            tone_values = edit_v1_tone(new_row[15]) if len(new_row) > 15 else [None] * 7
            if len(new_row) > 16: new_row[16] = edit_v2_enhanceddates(new_row[16]) or ""
            if len(new_row) > 22: new_row[22] = edit_v2_quotations(new_row[22]) or ""
            if len(new_row) > 24: new_row[24] = edit_v2_amounts(new_row[24]) or ""

            # 예전 버전 컬럼 삭제 (역순으로 삭제)
            for i in reversed(range(5, 14, 2)):  # 13, 11, 9, 7, 5 순서로 삭제
                if len(new_row) > i:
                    new_row.pop(i)
            
            # V1.5Tone 위치(인덱스 10)에 7개의 톤 값들을 개별 컬럼으로 삽입
            if len(new_row) > 10:
                # 톤 값들을 문자열로 변환 (None은 빈 문자열로)
                tone_strings = [str(v) if v is not None else "" for v in tone_values]
                new_row[10:11] = tone_strings  # 기존 톤 컬럼을 7개 컬럼으로 교체
            
            flattened_rows.append(new_row)

        columns = GKG_SCHEMA['columns']
        
        df = pd.DataFrame(flattened_rows, columns=columns)
        
        # 스키마 적용을 통한 데이터 타입 설정
        df = apply_dataframe_schema(df, GKG_SCHEMA)
        
        # JSON 컬럼들의 유효성 최종 검사
        json_dict_columns = ['v2enhancedthemes', 'v2enhancedpersons', 'v2enhancedorganizations', 
                            'v2gcam', 'v2_1allnames']
        json_array_columns = ['v2_1relatedimages', 'v2_1socialimageembeds', 
                             'v2_1socialvideoembeds', 'v2_1amounts']
        json_string_columns = ['v2_1sharingimage']  # 단일 URL을 JSON 문자열로 저장
        
        # JSON 딕셔너리 컬럼들 검증
        for col in json_dict_columns:
            if col in df.columns:
                def validate_json_dict(value):
                    if pd.isna(value) or value == "":
                        return "{}"
                    try:
                        import json
                        value_str = str(value).strip()
                        
                        # JSON 파싱 및 다시 직렬화하여 정규화
                        parsed = json.loads(value_str)
                        return json.dumps(parsed, ensure_ascii=False)
                    except:
                        print(f"Invalid JSON dict in column {col}: {str(value)[:100]}...")
                        return "{}"
                
                df[col] = df[col].apply(validate_json_dict)
        
        # JSON 배열 컬럼들 검증
        for col in json_array_columns:
            if col in df.columns:
                def validate_json_array(value):
                    if pd.isna(value) or value == "":
                        return "[]"
                    try:
                        import json
                        value_str = str(value).strip()
                        
                        # 백슬래시 이스케이프 문제 해결
                        if '\\' in value_str:
                            # 과도한 백슬래시 제거
                            value_str = value_str.replace('\\\\\\\\\\\\', '').replace('\\\\\\\\', '').replace('\\\\', '\\')
                        
                        # 이미 JSON 배열인지 확인
                        parsed = json.loads(value_str)
                        if isinstance(parsed, list):
                            # 유효한 JSON 배열이면 다시 직렬화하여 정규화
                            return json.dumps(parsed, ensure_ascii=False)
                        else:
                            # 배열이 아니라면 배열로 감싸기
                            return json.dumps([parsed], ensure_ascii=False)
                    except:
                        # JSON이 아니라면 문자열을 배열로 변환
                        try:
                            # URL이나 단순 문자열인 경우 배열로 감싸기
                            value_str = str(value).strip()
                            if value_str and value_str != "[]":
                                # 백슬래시 정리
                                clean_value = value_str.replace('\\\\\\\\\\\\', '').replace('\\\\\\\\', '').replace('\\\\', '')
                                if clean_value:
                                    return json.dumps([clean_value], ensure_ascii=False)
                            return "[]"
                        except:
                            print(f"Invalid JSON array in column {col}: {str(value)[:100]}...")
                            return "[]"
                
                df[col] = df[col].apply(validate_json_array)
        
        # JSON 문자열 컬럼들 검증 (단일 값을 JSON 문자열로 저장)
        for col in json_string_columns:
            if col in df.columns:
                def validate_json_string(value):
                    if pd.isna(value) or value == "":
                        return '""'  # 빈 JSON 문자열
                    try:
                        import json
                        value_str = str(value).strip()
                        # 단일 문자열을 JSON 문자열로 변환
                        return json.dumps(value_str, ensure_ascii=False)
                    except:
                        print(f"Invalid JSON string in column {col}: {str(value)[:100]}...")
                        return '""'
                
                df[col] = df[col].apply(validate_json_string)
        
        # 스키마 정보를 로그에 출력
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame schema:")
        print(df.dtypes)
        print(f"DataFrame info:")
        print(df.info(verbose=True))
        
        # GCS에 직접 업로드
        base_name = os.path.basename(file_path).replace('.csv', '')
        
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