from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from db import get_rows, get_table_config
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
