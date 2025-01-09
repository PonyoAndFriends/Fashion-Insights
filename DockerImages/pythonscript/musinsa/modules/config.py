import os
import pendulum

class Musinsa_Config:
    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "origin": "https://www.musinsa.com",
        "referer": "https://www.musinsa.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"'
    }
    
    LOCAL_TZ = pendulum.timezone("Asia/Seoul")
    
    TODAY_DATE = pendulum.now(tz=LOCAL_TZ).to_date_string()
    
class AWS_Config:
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
    
    AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
    
    REGION = 'ap-northeast-2'
    
    AWS_STORAGE_OPTIONS = {
        "key" : AWS_ACCESS_KEY_ID,
        "secret" : AWS_SECRET_ACCESS_KEY
    }
