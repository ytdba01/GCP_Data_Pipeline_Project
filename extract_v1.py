import requests
import json
from datetime import datetime
# Google Cloud SDK 將自動使用 GOOGLE_APPLICATION_CREDENTIALS 變數
from google.cloud import storage 

# ====== 設定區塊 ======

# 紐約市經緯度
LATITUDE = 40.71
LONGITUDE = -74.01

# 您的儲存桶名稱
GCS_BUCKET_NAME = 'de_project_weather_data_sj'

# API 請求參數
API_URL = 'https://api.open-meteo.com/v1/forecast'
PARAMS = {
    'latitude': LATITUDE,
    'longitude': LONGITUDE,
    'daily': ['temperature_2m_max', 'temperature_2m_min', 'weather_code'],
    'timezone': 'America/New_York',
    'forecast_days': 7 
}

# ====== 主要函式 ======

def run_extraction():
    """
    執行資料提取與上傳的主流程：
    1. 呼叫 Open-Meteo API 提取資料。
    2. 將原始 JSON 資料儲存為 GCS 上的檔案。
    """
    print("--- 1. 開始呼叫 Open-Meteo API ---")
    try:
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status() 
        data = response.json()
        print("API 呼叫成功，已接收資料。")

    except requests.exceptions.RequestException as e:
        print(f"API 呼叫失敗或 HTTP 錯誤: {e}")
        return 

    print("--- 2. 開始上傳資料至 Cloud Storage ---")
    
    # 建立 GCS 檔案名稱
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    GCS_FILE_NAME = f"raw_data/{now}_weather_data.json"
    
    # 建立 GCS 客戶端並上傳
    try:
        # storage.Client() 會自動尋找 GOOGLE_APPLICATION_CREDENTIALS
        storage_client = storage.Client() 
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_FILE_NAME)
        
        blob.upload_from_string(
            data=json.dumps(data, indent=4),
            content_type='application/json'
        )
        
        print(f"資料成功上傳到 GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}")
        
    except Exception as e:
        # 如果失敗，將顯示錯誤訊息，例如權限不足
        print(f"上傳至 GCS 失敗: {e}")
        
# 執行主要函式
if __name__ == '__main__':
    run_extraction() 
    