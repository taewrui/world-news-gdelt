from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from db import get_rows, get_table_config, get_daily_stats, get_country_stats
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/api/rows")
def api_rows(table: str = Query("raw_gkg", description="Table name: raw_export, raw_gkg, or raw_mentions")):
    """API 엔드포인트: 지정된 테이블의 데이터를 JSON으로 반환"""
    rows = get_rows(table)
    config = get_table_config(table)
    return {
        "table": table,
        "columns": config["display_names"],
        "rows": rows
    }

@app.get("/api/stats/daily")
def api_daily_stats():
    """API 엔드포인트: 일별 이벤트 빈도 통계"""
    stats = get_daily_stats()
    return {
        "title": "일별 이벤트 빈도",
        "data": [{"date": row[0], "count": row[1]} for row in stats]
    }

@app.get("/api/stats/country")
def api_country_stats():
    """API 엔드포인트: 국가별 이벤트 빈도 통계"""
    stats = get_country_stats()
    return {
        "title": "국가별 이벤트 빈도",
        "data": [{"country": row[0], "count": row[1]} for row in stats]
    }

@app.get("/stats", response_class=HTMLResponse)
def stats_page(request: Request):
    """통계 페이지: 일별 및 국가별 통계를 표시"""
    daily_stats = get_daily_stats()
    country_stats = get_country_stats()
    
    return templates.TemplateResponse("stats.html", {
        "request": request,
        "daily_stats": daily_stats,
        "country_stats": country_stats
    })

@app.get("/", response_class=HTMLResponse)
def read_root(request: Request, table: str = Query("raw_gkg", description="Table name")):
    """메인 페이지: 지정된 테이블의 데이터를 웹페이지로 표시"""
    rows = get_rows(table)
    config = get_table_config(table)
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "rows": rows,
        "table_name": table,
        "display_names": config["display_names"],
        "available_tables": ["raw_export", "raw_gkg", "raw_mentions"]
    })
