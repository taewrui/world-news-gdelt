"""
GDELT 데이터 스키마 정의 파일
각 테이블의 컬럼과 데이터 타입 정의를 포함합니다.
"""

# GDELT Export 테이블 스키마
EXPORT_SCHEMA = {
    'columns': [
        "globaleventid", "day", "monthyear", "year", "fractiondate", "actor1code", "actor1name", 
        "actor1countrycode", "actor1knowngroupcode", "actor1ethniccode", "actor1religion1code", "actor1religion2code", 
        "actor1type1code", "actor1type2code", "actor1type3code", "actor2code", "actor2name", "actor2countrycode", 
        "actor2knowngroupcode", "actor2ethniccode", "actor2religion1code", "actor2religion2code", "actor2type1code", 
        "actor2type2code", "actor2type3code", "isrootevent", "eventcode", "eventbasecode", "eventrootcode", "quadclass", 
        "goldsteinscale", "nummentions", "numsources", "numarticles", "avgtone", "actor1geo_type", "actor1geo_info",
        "actor2geo_type", "actor2geo_info", "actiongeo_type", "actiongeo_info", "dateadded", "sourceurl"
    ],
    'numeric_columns': [
        'globaleventid', 'monthyear', 'year', 'isrootevent', 'eventcode', 
        'eventbasecode', 'eventrootcode', 'quadclass', 'nummentions', 
        'numsources', 'numarticles', 'actor1geo_type', 'actor2geo_type', 'actiongeo_type'
    ],
    'float_columns': [
        'fractiondate', 'goldsteinscale', 'avgtone',
    ],
    'json_columns': [
        'actor1geo_info', 'actor2geo_info', 'actiongeo_info'
    ],
    'datetime_columns': [
        'day', 'dateadded'
    ],
    'string_columns': [
        'actor1code', 'actor1name', 'actor1countrycode', 'actor1knowngroupcode', 
        'actor1ethniccode', 'actor1religion1code', 'actor1religion2code', 'actor1type1code', 
        'actor1type2code', 'actor1type3code', 'actor2code', 'actor2name', 'actor2countrycode', 
        'actor2knowngroupcode', 'actor2ethniccode', 'actor2religion1code', 'actor2religion2code', 
        'actor2type1code', 'actor2type2code', 'actor2type3code', 'actor1geo_fullname',
        'actor2geo_fullname', 'actiongeo_fullname', 'sourceurl'
    ]
}

# GDELT Mentions 테이블 스키마
MENTIONS_SCHEMA = {
    'columns': [
        "globaleventid", "eventtimedate", "mentiontimedate", "mentiontype", "mentionsourcename", "mentionidentifier",
        "sentenceid", "actor1charoffset", "actor2charoffset", "actioncharoffset", "inrawtext", "confidence",
        "mentiondoclen", "mentiondoctone", "mentiondoctranslationinfo", "extras"
    ],
    'numeric_columns': [
        'globaleventid', 'mentiontype', 'sentenceid', 'actor1charoffset', 
        'actor2charoffset', 'actioncharoffset', 'inrawtext', 'confidence', 'mentiondoclen'
    ],
    'float_columns': [
        'mentiondoctone'
    ],
    'datetime_columns': [
        'eventtimedate', 'mentiontimedate'
    ],
    'json_columns': [
        'mentiondoctranslationinfo'
    ],
    'string_columns': [
        'mentionsourcename', 'mentionidentifier', 'extras'
    ]
}

# GDELT GKG 테이블 스키마
GKG_SCHEMA = {
    'columns': [
        "gkgrecordid", "v2_1date", "v2sourcecollectionidentifier", "v2sourcecommonname",
        "v2documentidentifier", "v2_1counts", "v2enhancedthemes", "v2enhancedlocations", 
        "v2enhancedpersons", "v2enhancedorganizations", 
        "v1tone_tone", "v1tone_positivescore", "v1tone_negativescore", "v1tone_polarity",
        "v1tone_activityreferencedensity", "v1tone_selfgroupreferencedensity", "v1tone_wordcount",
        "v2_1enhanceddates", "v2gcam", "v2_1sharingimage", "v2_1relatedimages", "v2_1socialimageembeds", 
        "v2_1socialvideoembeds", "v2_1quotations", "v2_1allnames", "v2_1amounts", 
        "v2_1transationinfo", "v2extrasxml"
    ],
    'numeric_columns': [
        'v2sourcecollectionidentifier', 'v1tone_wordcount'
    ],
    'float_columns': [
        'v1tone_tone', 'v1tone_positivescore', 'v1tone_negativescore', 
        'v1tone_polarity', 'v1tone_activityreferencedensity', 'v1tone_selfgroupreferencedensity'
    ],
    'datetime_columns': [
        'v2_1date'
    ],
    'json_columns': [
        'v2_1counts', 'v2enhancedthemes', 'v2enhancedlocations', 'v2enhancedpersons', 
        'v2enhancedorganizations', 'v2_1enhanceddates', 'v2gcam', 'v2_1sharingimage', 
        'v2_1relatedimages', 'v2_1socialimageembeds', 'v2_1socialvideoembeds', 
        'v2_1quotations', 'v2_1allnames', 'v2_1amounts', 'v2_1transationinfo'
    ],
    'string_columns': [
        'gkgrecordid', 'v2sourcecommonname', 'v2documentidentifier', 'v2extrasxml'
    ]
}


def apply_dataframe_schema(df, schema):
    """
    DataFrame에 스키마를 적용하는 유틸리티 함수
    
    Args:
        df: pandas DataFrame
        schema: 스키마 딕셔너리 (EXPORT_SCHEMA, MENTIONS_SCHEMA, GKG_SCHEMA 중 하나)
    
    Returns:
        타입이 적용된 pandas DataFrame
    """
    import pandas as pd
    
    # 숫자형 컬럼들을 적절한 타입으로 변환 (빈 값은 NULL로 유지)
    for col in schema.get('numeric_columns', []):
        if col in df.columns:
            # 빈 문자열을 NaN으로 변환 후 nullable integer 타입 사용
            df[col] = df[col].replace('', pd.NA)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype('Int64')  # nullable integer
    
    # 실수형 컬럼들 (빈 값은 NULL로 유지)
    for col in schema.get('float_columns', []):
        if col in df.columns:
            df[col] = df[col].replace('', pd.NA)
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # float64는 기본적으로 nullable
    
    # JSON 컬럼들
    for col in schema.get('json_columns', []):
        if col in df.columns:
            # 빈 문자열은 null로, 유효한 JSON은 그대로 유지
            df[col] = df[col].apply(lambda x: None if x == '' else x)
    
    # 날짜/시간 컬럼들
    for col in schema.get('datetime_columns', []):
        if col in df.columns:
            if col in ['day']:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce')
            elif col in ['dateadded']:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            elif col in ['eventtimedate', 'mentiontimedate']:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            elif col in ['v2_1date']:
                # GDELT GKG 날짜는 YYYYMMDDHHMMSS 형식이므로 변환 필요
                from dags.scripts.converters import convert_datetime_format
                df[col] = df[col].apply(lambda x: convert_datetime_format(str(x)) if x and str(x) != '' else None)
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            else:
                df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 문자열 컬럼들 (빈 문자열은 그대로 유지)
    for col in schema.get('string_columns', []):
        if col in df.columns:
            df[col] = df[col].astype('str')
    
    return df
