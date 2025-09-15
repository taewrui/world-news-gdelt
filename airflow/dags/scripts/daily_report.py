"""
GDELT 파이프라인 일일 리포트 생성 및 Slack 전송
"""

import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def generate_daily_quality_report(**context):
    """
    지난 24시간의 GDELT 파이프라인 품질 리포트를 생성하고 Slack으로 전송
    """
    execution_date = context['execution_date'] or context['logical_date']
    report_date = execution_date.date()
    
    # 어제 날짜 범위 설정
    start_date = report_date - timedelta(days=1)
    end_date = report_date
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # 1. 일일 실행 통계 수집
        execution_stats_query = """
        SELECT 
            COUNT(*) as total_runs,
            COUNT(CASE WHEN state = 'success' THEN 1 END) as successful_runs,
            COUNT(CASE WHEN state = 'failed' THEN 1 END) as failed_runs,
            AVG(duration_seconds) as avg_duration,
            MIN(duration_seconds) as min_duration,
            MAX(duration_seconds) as max_duration
        FROM pipeline_execution_metrics 
        WHERE dag_id = 'gdelt_pipeline' 
        AND DATE(execution_date) = %s
        """
        
        execution_stats = postgres_hook.get_first(execution_stats_query, parameters=[start_date])
        
        # 2. 파일별 품질 통계 수집
        quality_stats_query = """
        SELECT 
            file_type,
            COUNT(*) as total_checks,
            COUNT(CASE WHEN quality_check_passed = true THEN 1 END) as passed_checks,
            AVG(original_records) as avg_original_records,
            AVG(processed_records) as avg_processed_records,
            AVG(loaded_records) as avg_loaded_records,
            AVG(CASE WHEN original_records > 0 THEN 
                (loaded_records::float / original_records::float) * 100 
                ELSE 0 END) as avg_retention_rate
        FROM data_quality_metrics 
        WHERE dag_id = 'gdelt_pipeline' 
        AND DATE(execution_date) = %s
        GROUP BY file_type
        ORDER BY file_type
        """
        
        quality_stats = postgres_hook.get_records(quality_stats_query, parameters=[start_date])
        
        # 3. 데이터 레코드 적재 통계 수집
        data_loading_stats_query = """
        SELECT 
            SUM(original_records) as total_original_records,
            SUM(processed_records) as total_processed_records,
            SUM(loaded_records) as total_loaded_records,
            SUM(CASE WHEN loaded_records > 0 THEN 1 ELSE 0 END) as successful_loads,
            COUNT(*) as total_loads,
            AVG(CASE WHEN original_records > 0 THEN 
                (loaded_records::float / original_records::float) * 100 
                ELSE 0 END) as overall_retention_rate
        FROM data_quality_metrics 
        WHERE dag_id = 'gdelt_pipeline' 
        AND DATE(execution_date) = %s
        """
        
        data_loading_stats = postgres_hook.get_first(data_loading_stats_query, parameters=[start_date])
        
        # 4. 실패한 품질 체크 조회
        failed_checks_query = """
        SELECT file_type, error_message, COUNT(*) as failure_count
        FROM data_quality_metrics 
        WHERE dag_id = 'gdelt_pipeline' 
        AND DATE(execution_date) = %s
        AND quality_check_passed = false
        GROUP BY file_type, error_message
        ORDER BY failure_count DESC
        """
        
        failed_checks = postgres_hook.get_records(failed_checks_query, parameters=[start_date])
        
        # 5. 시간별 실행 패턴 (96회 실행 예상, 15분마다)
        hourly_pattern_query = """
        SELECT 
            EXTRACT(HOUR FROM execution_date) as hour,
            COUNT(*) as runs_count,
            COUNT(CASE WHEN state = 'success' THEN 1 END) as success_count
        FROM pipeline_execution_metrics 
        WHERE dag_id = 'gdelt_pipeline' 
        AND DATE(execution_date) = %s
        GROUP BY EXTRACT(HOUR FROM execution_date)
        ORDER BY hour
        """
        
        hourly_pattern = postgres_hook.get_records(hourly_pattern_query, parameters=[start_date])
        
        # 6. 리포트 생성
        report = generate_slack_report(start_date, execution_stats, quality_stats, data_loading_stats, failed_checks, hourly_pattern)
        
        # 7. Slack 전송
        send_slack_report(report)
        
        print(f"일일 품질 리포트 생성 및 전송 완료: {start_date}")
        return report
        
    except Exception as e:
        error_message = f"일일 리포트 생성 중 오류 발생: {str(e)}"
        print(error_message)
        
        # 오류 발생시에도 Slack으로 알림
        error_report = {
            "text": "GDELT 일일 리포트 생성 실패",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"**GDELT 파이프라인 일일 리포트 생성 실패**\n날짜: {start_date}\n오류: {error_message}"
                    }
                }
            ]
        }
        send_slack_report(error_report)
        raise


def generate_slack_report(report_date, execution_stats, quality_stats, data_loading_stats, failed_checks, hourly_pattern):
    """
    Slack 형식의 리포트 생성
    """
    total_runs, successful_runs, failed_runs, avg_duration, min_duration, max_duration = execution_stats or (0, 0, 0, 0, 0, 0)
    
    # 데이터 적재 통계
    total_original, total_processed, total_loaded, successful_loads, total_loads, retention_rate = data_loading_stats or (0, 0, 0, 0, 0, 0)
    failed_loads = total_loads - successful_loads if total_loads and successful_loads else 0
    load_success_rate = (successful_loads / total_loads * 100) if total_loads > 0 else 0
    
    # 성공률 계산
    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
    
    # 이모지 설정
    status_emoji = "" if success_rate >= 95 else "" if success_rate >= 80 else ""
    
    # 전체 품질 점수 계산
    total_quality_checks = sum(stat[1] for stat in quality_stats) if quality_stats else 0
    passed_quality_checks = sum(stat[2] for stat in quality_stats) if quality_stats else 0
    quality_rate = (passed_quality_checks / total_quality_checks * 100) if total_quality_checks > 0 else 0
    
    # Slack 블록 구성
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "GDELT 파이프라인 일일 리포트"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*리포트 날짜:* {report_date}\n*생성 시간:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*실행 통계*\n• 총 실행 횟수: {total_runs}회\n• 성공: {successful_runs}회 ({success_rate:.1f}%)\n• 실패: {failed_runs}회"
            }
        }
    ]
    
    # 실행 시간 통계 추가
    if avg_duration:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*실행 시간*\n• 평균: {int(avg_duration/60)}분 {int(avg_duration%60)}초\n• 최단: {int(min_duration/60)}분 {int(min_duration%60)}초\n• 최장: {int(max_duration/60)}분 {int(max_duration%60)}초"
            }
        })
    
    # 데이터 적재 통계 추가
    if total_loads > 0:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*데이터 적재 통계*\n• 총 적재 시도: {total_loads:,}회\n• 성공: {successful_loads:,}회 ({load_success_rate:.1f}%)\n• 실패: {failed_loads:,}회\n• 전체 레코드 보존율: {retention_rate:.1f}%"
            }
        })
        
        # 레코드 수량 상세 정보
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*레코드 처리 현황*\n• 원본 레코드: {total_original:,}건\n• 처리된 레코드: {total_processed:,}건\n• 최종 적재: {total_loaded:,}건"
            }
        })
    
    # 데이터 품질 통계
    blocks.append({
        "type": "divider"
    })
    
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*데이터 품질 검사*\n• 전체 품질 점수: {quality_rate:.1f}%\n• 총 검사: {total_quality_checks}회\n• 통과: {passed_quality_checks}회"
        }
    })
    
    # 파일별 상세 통계
    if quality_stats:
        quality_details = "*파일별 상세 통계*\n"
        for stat in quality_stats:
            file_type, total_checks, passed_checks, avg_orig, avg_proc, avg_loaded, retention_rate = stat
            quality_details += f"• {file_type}: {passed_checks}/{total_checks} 통과 ({retention_rate:.1f}% 보존율)\n"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": quality_details
            }
        })
    
    # 실패한 검사가 있는 경우 추가
    if failed_checks:
        blocks.append({
            "type": "divider"
        })
        
        failure_details = "*실패한 품질 검사*\n"
        for check in failed_checks[:5]:  # 최대 5개만 표시
            file_type, error_msg, count = check
            failure_details += f"• {file_type}: {error_msg} ({count}회)\n"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": failure_details
            }
        })
    
    # 시간별 실행 패턴 (문제가 있는 시간대만 표시)
    problematic_hours = [h for h in hourly_pattern if h[1] != h[2]]  # 실행 횟수와 성공 횟수가 다른 시간
    if problematic_hours:
        blocks.append({
            "type": "divider"
        })
        
        pattern_details = "*문제 발생 시간대*\n"
        for hour_data in problematic_hours[:5]:
            hour, total, success = hour_data
            pattern_details += f"• {int(hour):02d}시: {success}/{total} 성공\n"
        
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": pattern_details
            }
        })
    
    return {
        "text": f"GDELT 파이프라인 일일 리포트 - {report_date}",
        "blocks": blocks
    }


def send_slack_report(report):
    """
    Slack으로 리포트 전송
    """
    try:
        # Slack Webhook Hook 사용 (Airflow Connection에서 slack_default 설정 필요)
        slack_hook = SlackWebhookHook(
            http_conn_id='slack_default',
            message=report.get('text', 'GDELT 일일 리포트'),
            channel='#gdelt-daily-alert',  # 채널명 설정
            username='gdelt-daily-alert',
            icon_emoji=':chart_with_upwards_trend:'
        )
        
        # blocks가 있는 경우 추가
        if 'blocks' in report:
            slack_hook.send_dict({
                'text': report['text'],
                'blocks': report['blocks'],
                'channel': '#gdelt-daily-alert',
                'username': 'gdelt-daily-alert',
                'icon_emoji': ':chart_with_upwards_trend:'
            })
        else:
            slack_hook.execute()
        
        print("Slack 리포트 전송 완료")
        
    except Exception as e:
        print(f"Slack 전송 실패: {str(e)}")
        # Slack 전송 실패해도 DAG는 계속 진행
        pass


def test_slack_connection(**context):
    """
    Slack 연결 테스트 함수
    """
    test_message = {
        "text": "GDELT 파이프라인 Slack 연결 테스트",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "**Slack 연결이 정상적으로 작동합니다!**\n테스트 시간: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            }
        ]
    }
    
    send_slack_report(test_message)
    print("Slack 연결 테스트 완료")
