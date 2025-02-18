from airflow.models import Variable

S3_BUCKET = Variable("s3_bucket")  # "Team3-2"

DEFAULT_S3_DICT = {
    "aws_access_key_id": Variable.get("aws_access_key_id"),
    "aws_secret_access_key": Variable.get("aws_secret_access_key"),
    "aws_region": Variable.get("aws_region"),
    "s3_bucket_name": Variable.get("s3_bucket"),
    "data_file": None,
    "file_path": None,
    "content_type": "application/json",
}

MUSINSA_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

OTHERAPI_DEFAULT_ARGS = {
    "owner": "gjstjd9509@gmail.com",
    "email": ["gjstjd9509@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

FILE_EXT = {
    "application/json": "json",
    "text/html": "html",
    "plain/text": "txt",
}

NAVER_HEADER = {
    "X-Naver-Client-Id": Variable.get("x-naver-client-id"),
    "X-Naver-Client-Secret": Variable.get("x-naver-client-secret"),
    "Content-Type": "application/json",
}

OTHERAPI_DEFAULT_PYTHON_SCRIPT_PATH = r"/app/python_scripts/otherapis"

NAVER_HAEDER_2 = {
    "X-Naver-Client-Id": Variable.get("naver_id_2"),
    "X-Naver-Client-Secret": Variable.get("naver_secret_2"),
    "Content-Type": "application/json",
}
