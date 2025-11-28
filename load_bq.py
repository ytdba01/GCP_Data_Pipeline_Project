import os
from google.cloud import bigquery
from datetime import datetime

# ====== 設定區塊 (與 extract.py 共享設定) ======

# 您的 Project ID
PROJECT_ID = 'lofty-ivy-473007-k5'

# BigQuery 設定
DATASET_ID = 'weather_data_lake'
# 原始資料暫存表 (Staging Table)，每次載入都創建一個新表
STAGING_TABLE_ID = 'raw_forecast_staging' 

# Cloud Storage 設定
GCS_BUCKET_NAME = 'de_project_weather_data_sj'

# GCS 資料來源路徑 (使用萬用字元 * 匹配所有上傳的 JSON 檔案)
# GCS_URI = f'gs://{GCS_BUCKET_NAME}/raw_data/*.json'

# GCS 資料來源路徑 (只載入單一、最新的檔案)
GCS_URI = f'gs://{GCS_BUCKET_NAME}/raw_data/latest_weather_data.json'

# ====== 載入函式 ======

def load_data_from_gcs():
    """
    從 Cloud Storage 載入所有 JSON 檔案到 BigQuery 暫存資料表。
    """
    print("--- 1. 初始化 BigQuery 客戶端 ---")
    # 客戶端會自動使用 GOOGLE_APPLICATION_CREDENTIALS 找到您的服務帳戶金鑰
    client = bigquery.Client(project=PROJECT_ID)

    # 建立一個載入任務配置
    job_config = bigquery.LoadJobConfig(
        # 關鍵修正：將格式從 NEWLINE_DELIMITED_JSON 更改為標準 JSON
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        # 讓 BigQuery 自動推論 Schema
        autodetect=True, 
        # 確保目標表 (STAGING_TABLE_ID) 每次載入前都被覆蓋
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    # 執行載入任務
    print(f"--- 2. 開始從 {GCS_URI} 載入資料至 {DATASET_ID}.{STAGING_TABLE_ID} ---")
    
    # 這裡我們載入 GCS 路徑下的所有 JSON 檔案
    load_job = client.load_table_from_uri(
        GCS_URI,
        client.dataset(DATASET_ID).table(STAGING_TABLE_ID),
        job_config=job_config
    )  # API Call
    
    # 等待任務完成
    load_job.result()  
    
    print(f"載入任務完成！已將 {load_job.output_rows} 列資料載入 BigQuery。")
    print(f"表結構推論完成：{client.get_table(client.dataset(DATASET_ID).table(STAGING_TABLE_ID)).schema}")

# 執行主要函式
if __name__ == '__main__':
    # 驗證 GOOGLE_APPLICATION_CREDENTIALS 變數是否存在
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        print("錯誤：GOOGLE_APPLICATION_CREDENTIALS 環境變數未設定。請先執行 'set GOOGLE_APPLICATION_CREDENTIALS=...'")
    else:
        try:
            load_data_from_gcs()
        except Exception as e:
            print(f"載入作業執行失敗：{e}")