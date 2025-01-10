# Ably 모듈에서 사용하는 기본 설정값 및 의존성 모듈을 정의한 파일

# Airflow DAG 기본 설정값
ABLYAPI_DEFAULT_ARGS = {
    'owner': 'jeongseoha1203@gmail.com',  # seoha 이메일
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,  
    'retries': 5,
}

# S3 관련 기본 설정값
DEFAULT_S3_DICT = {
    'bucket_name': 'ablyrawdata',
    'region': 'us-east-1',
}

# Ably API 헤더
ABLY_HEADER = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'origin': 'https://m.a-bly.com',
            'referer': 'https://m.a-bly.com/',
            'user-agent': 'Mozilla/5.0',
            'x-anonymous-token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhbm9ueW1vdXNfaWQiOiIzMzc3ODI3NDAiLCJpYXQiOjE3MzQ0MDE2MTR9.LZgtaLkf3KFnAcG4b5bcOa_taHF9FL6odYx5vd_o7w4',
            'x-app-version': '0.1.0',
            'x-device-id': '42e0f21c-3460-40f5-bfa5-d1595286ed3e',
            'x-device-type': 'MobileWeb',
            'x-web-type': 'Web'
}

ABLY_NEXT_TOKEN = "eyJsIjogMSwgInAiOiB7ImRlcGFydG1lbnRfdHlwZSI6ICJDQVRFR09SWSIsICJjYXRlZ29yeV9zbm8iOiA3LCAic3ViX2NhdGVnb3J5X3NubyI6IDI5NiwgInByZXZpb3VzX3NjcmVlbl9uYW1lIjogIlNVQl9DQVRFR09SWV9ERVBBUlRNRU5UIiwgImZpbHRlcl9jb21wb25lbnQiOiA2M30sICJjYXRlZ29yeV9zbm8iOiA3fQ=="

ABLY_CATEGORY = {
                7: {
                    "category_name": "아우터",
                    "sub_category": {
                    16: "가디건",
                    293: "자켓",
                    294: "집업/점퍼",
                    296: "코트",
                    297: "패딩",
                    496: "야상",
                    497: "바람막이",
                    577: "플리스"
                    }
                },
                8: {
                    "category_name": "상의",
                    "sub_category": {
                    18: "반소매티셔츠",
                    21: "민소매",
                    298: "블라우스",
                    299: "니트",
                    300: "맨투맨",
                    357: "조끼",
                    498: "긴소매티셔츠",
                    499: "셔츠",
                    500: "후드"
                    }
                },
                10: {
                    "category_name": "원피스",
                    "sub_category": {
                    206: "미니원피스",
                    207: "롱원피스"
                    }
                },
                174: {
                    "category_name": "팬츠",
                    "sub_category": {
                    176: "롱팬츠",
                    177: "숏팬츠",
                    178: "슬랙스",
                    501: "데님"
                    }
                },
                203: {
                    "category_name": "스커트",
                    "sub_category": {
                    204: "미니 스커트",
                    205: "미디/롱스커트"
                    }
                },
                481: {
                    "category_name": "신발",
                    "sub_category": {
                    488: "플랫/로퍼",
                    489: "힐",
                    490: "스니커즈",
                    491: "샌들",
                    492: "슬리퍼/쪼리",
                    493: "워커/부츠",
                    494: "블로퍼/뮬"
                    }
                },
                517: {
                    "category_name": "트레이닝",
                    "sub_category": {
                    518: "트레이닝 세트",
                    520: "트레이닝 상의",
                    521: "트레이닝 하의",
                    519: "레깅스"
                    }
                }
                }
