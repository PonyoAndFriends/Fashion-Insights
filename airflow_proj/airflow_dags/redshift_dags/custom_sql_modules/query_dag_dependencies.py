from datetime import datetime, timedelta
from airflow.models import Variable

NOW = datetime.now()
NOW_STRING = (NOW + timedelta(hours=9)).strftime("%Y-%m-%d")

SILVER_LOAD_DEFAULT_ARGS = {
    "owner": "gjstjd9509@gmail.com",
    "email": ["gjstjd9509@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
}

DEFAULT_SILVER_SHCEMA = "retail_silver_layer"
DEFAULT_GOLD_SHCEMA = "retail_gold_layer"
DEFULAT_SILVER_BUCKET_URL = rf"s3://{Variable.get('s3_bucket')}/silver/"
PLATFORMS = ["musinsa", "29cm", "ably"]
