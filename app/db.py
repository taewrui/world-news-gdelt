import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# 각 테이블의 주요 컬럼 정의
TABLE_CONFIGS = {
    "raw_export": {
        "columns": ["day", "actor1name", "actor2name", "eventcode", "goldsteinscale"],
        "display_names": ["Date", "Actor 1", "Actor 2", "Event Code", "Goldstein Scale"]
    },
    "raw_gkg": {
        "columns": ["v2_1date", "v2sourcecommonname", "v1tone_tone"],
        "display_names": ["Date", "Source", "Tone"]
    },
    "raw_mentions": {
        "columns": ["eventtimedate", "mentionsourcename", "confidence"],
        "display_names": ["Event Time", "Mention Source", "Confidence"]
    }
}


def get_rows(table_name="raw_gkg"):
    """지정된 테이블에서 데이터를 조회합니다."""
    # 테스트용 모의 데이터 (DATABASE_URL이 없거나 연결 실패시 사용)
    if not DATABASE_URL or "username:password" in DATABASE_URL:
        mock_data = {
            "raw_export": [
                (1247198414, "2024-06-01", "USA", "CHN", 120, 2.5),
                (1247198415, "2024-06-01", "GBR", "FRA", 110, 1.8),
                (1247198416, "2024-06-01", "JPN", "KOR", 130, -1.2),
            ],
            "raw_gkg": [
                ("20250601093000-0", "2025-06-01 09:30:00", "haaretz.com", -1.5),
                ("20250601093000-1", "2025-06-01 09:30:00", "cnn.com", 2.3),
                ("20250601093000-2", "2025-06-01 09:30:00", "bbc.com", 0.8),
            ],
            "raw_mentions": [
                (1247191502, "2025-06-01 06:30:00", "Reuters", 95),
                (1247191503, "2025-06-01 06:30:00", "AP", 88),
                (1247191504, "2025-06-01 06:30:00", "Bloomberg", 92),
            ]
        }
        return mock_data.get(table_name, [])
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        if table_name not in TABLE_CONFIGS:
            raise ValueError(f"Unknown table: {table_name}")
        
        columns = TABLE_CONFIGS[table_name]["columns"]
        query = f"SELECT {', '.join(columns)} FROM {table_name} LIMIT 50;"
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"Database connection failed: {e}")
        # 연결 실패시 모의 데이터 반환
        mock_data = {
            "raw_export": [
                (1247198414, "2024-06-01", "USA", "CHN", 120, 2.5),
                (1247198415, "2024-06-01", "GBR", "FRA", 110, 1.8),
            ],
            "raw_gkg": [
                ("20250601093000-0", "2025-06-01 09:30:00", "haaretz.com", -1.5),
                ("20250601093000-1", "2025-06-01 09:30:00", "cnn.com", 2.3),
            ],
            "raw_mentions": [
                (1247191502, "2025-06-01 06:30:00", "Reuters", 95),
                (1247191503, "2025-06-01 06:30:00", "AP", 88),
            ]
        }
        return mock_data.get(table_name, [])


def get_table_config(table_name):
    """테이블 설정 정보를 반환합니다."""
    return TABLE_CONFIGS.get(table_name, {"columns": [], "display_names": []})
