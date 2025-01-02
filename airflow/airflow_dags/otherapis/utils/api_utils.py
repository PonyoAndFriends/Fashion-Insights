def fetch_non_paged_data(base_url, key_url, params=None, headers=None):
    """
    Pagination이 없는 데이터를 API로부터 호출하는 함수

    Args:
        base_url (str): API 기본 URL
        key_url (str): API 키 URL
        params (dict, optional): 요청 파라미터. 기본값은 None.
        headers (dict, optional): 요청 헤더. 기본값은 None.

    Returns:
        requests.Response: API 응답 객체
    """
    import requests
    import logging
    logging.info(f"Fetching non-paged data from {base_url + key_url}...")

    try:
        response = requests.get(base_url + key_url, params=params, headers=headers)
        response.raise_for_status()
        logging.info("Successfully fetched non-paged data.")
        return response
    except requests.RequestException as e:
        logging.error(f"Error fetching non-paged data: {e}")
        raise


def fetch_ids_to_search(first_url, params=None, headers=None):
    import requests
    import logging
    """
    API로부터 ID 리스트를 가져오는 함수

    Args:
        first_url (str): API URL
        params (dict, optional): 요청 파라미터. 기본값은 None.
        headers (dict, optional): 요청 헤더. 기본값은 None.

    Returns:
        List[int]: ID 리스트
    """
    logging.info(f"Fetching IDs to search from {first_url}...")

    try:
        response = requests.get(first_url, params=params, headers=headers)
        response.raise_for_status()
        data_list = response.json()

        ids = [data['id'] for data in data_list if 'id' in data]
        logging.info(f"Successfully fetched {len(ids)} IDs.")
        return ids
    except requests.RequestException as e:
        logging.error(f"Error fetching IDs: {e}")
        raise
    except KeyError as e:
        logging.error(f"Invalid response format: {e}")
        raise


def calculate_page_range(total_count, total_pages, page_size, parallel_task_num):
    import math
    import logging
    """
    병렬 태스크 개수와 페이지 크기를 기반으로 페이지 범위를 계산하는 함수

    Args:
        total_count (int): 총 데이터 개수
        total_pages (int, optional): 총 페이지 수 (None이면 계산됨)
        page_size (int): 한 페이지의 데이터 크기
        parallel_task_num (int): 병렬 태스크 개수

    Returns:
        List[Tuple[int, int]]: 각 태스크에 할당할 페이지 범위 리스트
    """
    # 로깅 설정
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if total_pages is None:
        total_pages = math.ceil(total_count / page_size)

    pages_per_task = math.ceil(total_pages / parallel_task_num)
    page_ranges = [
        (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
        for i in range(parallel_task_num)
    ]

    logging.info(f"Calculated page ranges: {page_ranges}")
    return page_ranges


def fetch_paged_data(base_url, key_url, params, page_range, page_size, aws_config_dict, file_topic, content_type, headers=None):
    import requests
    import json
    import time
    import logging
    from datetime import datetime
    from s3_utils import load_data_to_s3
    """
    특정 page range의 데이터를 호출하여 AWS S3에 저장하는 함수

    Args:
        base_url (str): API 기본 URL
        key_url (str): API 키 URL
        params (dict): API 요청 파라미터
        page_range (tuple): (start_page, end_page)
        page_size (int): 페이지 크기
        aws_config_dict (dict): AWS 구성 정보
        file_topic (str): 파일 주제 이름
        content_type (str): 데이터 형식 (application/json, text/html 등)
        headers (dict, optional): 요청 헤더. 기본값은 None.

    Returns:
        None
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    start_page, end_page = page_range
    url = base_url + key_url
    logging.info(f"Fetching pages {start_page} to {end_page} from {url}")

    # 날짜 문자열 생성
    date_string = datetime.now().strftime("%Y-%m-%d")

    # AWS 구성 값 검증
    required_keys = ['aws_access_key_id', 'aws_secret_access_key', 'aws_region', 's3_bucket_name']
    for key in required_keys:
        if key not in aws_config_dict:
            raise ValueError(f"Missing required AWS config key: {key}")

    aws_access_key_id = aws_config_dict['aws_access_key_id']
    aws_secret_access_key = aws_config_dict['aws_secret_access_key']
    aws_region = aws_config_dict['aws_region']
    s3_bucket_name = aws_config_dict['s3_bucket_name']

    for page in range(start_page, end_page + 1):
        params['page'] = page

        # 페이지 크기 설정
        if 'size' in params:
            params['size'] = page_size
        elif 'perPage' in params:
            params['perPage'] = page_size

        # API 호출
        try:
            time.sleep(0.2)
            request_headers = headers or {}
            response = requests.get(url, headers=request_headers, params=params)
            response.raise_for_status()

            # 응답 데이터 처리
            if content_type == 'application/json':
                data = response.json()
                file_extension = 'json'
                serialized_data = json.dumps(data, ensure_ascii=False)
            elif content_type == 'text/html':
                serialized_data = response.text
                file_extension = 'html'
            else:
                serialized_data = response.text
                file_extension = 'txt'

            # S3 파일 경로 설정
            file_path = f"/{file_topic}_raw_data/{date_string}/{file_topic}_{page}.{file_extension}"

            # S3 업로드
            load_data_to_s3(
                aws_access_key_id,
                aws_secret_access_key,
                aws_region,
                s3_bucket_name,
                serialized_data,
                file_path,
                content_type
            )

            logging.info(f"Successfully fetched and uploaded page {page} to S3.")
        except requests.RequestException as e:
            logging.error(f"Error fetching page {page}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error for page {page}: {e}")

    logging.info(f"Completed fetching pages {start_page} to {end_page}.")
