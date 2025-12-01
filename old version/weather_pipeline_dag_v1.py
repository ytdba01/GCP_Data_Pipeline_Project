from __future__ import annotations
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ====== DAG 設定 ======
# 定義排程開始時間和排程間隔 (例如，每天執行一次)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gcp_data_weather_pipeline',
    default_args=default_args,
    description='End-to-end ELT pipeline for weather data using GCS and BigQuery.',
    # 設定為每天執行一次 (使用標準 cron 表示法)
    schedule='0 6 * * *', # 每天早上 6:00 UTC 執行
    start_date=datetime(2025, 11, 28),
    catchup=False,
    tags=['data-engineering', 'gcp'],
) as dag:
    
    # Airflow 任務 1: 資料提取與載入到 GCS (E & L - GCS)
    # 我們使用 BashOperator 呼叫本地的 extract.py 腳本
    extract_to_gcs = BashOperator(
        task_id='extract_data_to_gcs',
        # 注意：我們假設 extract.py 和 load_bq.py 已經被放置在 DAG 儲存桶中的 'dags' 資料夾之外的某個地方
        # 這裡我們將模擬直接執行 extract.py 腳本
        # 在實際的 Composer 環境中，您需要確保腳本可以被呼叫到 (通常通過 git-sync 或其他方式)
        # 為了測試，我們將在下一步簡化這個呼叫
        bash_command='python extract.py',
    )
    
    # Airflow 任務 2: BigQuery 載入與轉換 (L - BQ & T)
    # 我們使用 BashOperator 呼叫本地的 execute_sql.py 腳本
    transform_data = BashOperator(
        task_id='load_and_transform_to_bq',
        bash_command='python execute_sql.py',
    )

    # 建立任務依賴關係
    # 提取完成後才能進行轉換
    extract_to_gcs >> transform_data