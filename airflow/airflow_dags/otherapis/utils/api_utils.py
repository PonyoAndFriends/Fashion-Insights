def fetch_non_paged_data(base_url, key_url, params, headers=None):
    """Pagination 되지 않은 데이터를 api로부터 호출하는 함수"""
    import requests
    url = base_url + key_url

    print(f"Fetching non_paged data from {url}...")

    options = {}
    if headers:
        options['headers'] = headers
    if params:
        options['params'] = params
    
    response = requests.get(url, **options)
    response.raise_for_status()
    return response


def fetch_ids_to_search(first_url, params, headers, ):
    import requests
    res = requests.get(first_url, params=params, headers=headers)
    res.raise_for_status()

    ids = []
    res = list(res.json())
    for data in res:
        ids.append(data['id'])
    
    return ids


def calculate_page_range(total_count, total_pages, page_size, parallel_task_num):
        """병렬 실행하고 싶은 태스크의 개수와 사이즈를 받아 적절하게 페이지의 범위를 나누어 리턴하는 함수"""
        import math
        total_pages = math.ceil(total_count / page_size)
        pages_per_task = math.ceil(total_pages / parallel_task_num)
        
        # 페이지 범위 계산
        page_ranges = [
            (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
            for i in range(parallel_task_num)
        ]
        
        print(f"Page ranges: {page_ranges}")
        return page_ranges


def fetch_paged_data(base_url, key_url, params, page_range, page_size, aws_config_dict, file_topic, content_type, headers=None):
    """특정 page range의 데이터를 호출해 들고오는 함수"""
    import requests
    import json
    import time
    from s3_utils import load_data_to_s3

    start_page, end_page = page_range
    url = base_url + key_url
    print(f"Fetching pages {start_page} to {end_page}...")

    now = time.now()
    date_string = now.strftime("%Y-%m-%d")
    aws_access_key_id = aws_config_dict['aws_access_key_id']
    aws_secret_access_key = aws_config_dict['aws_secret_access_key']
    aws_region = aws_config_dict['aws_region']
    s3_bucket_name = aws_config_dict['s3_bucket_name']

    for page in range(start_page, end_page + 1):
        params['page'] = page
        if 'size' in params:
            params['size'] = page_size
        elif 'perPage' in params:
            params['perPage'] = page_size
        time.sleep(0.2)
        if not headers:
            response = requests.get(url, params=params)
        else:
            response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        if content_type == 'application/json':
            response = json.dumps(response, ensure_ascii=False)
            file_path = f"/{file_topic}_raw_data/{date_string}/{file_topic}_{page}.json"
        elif content_type == 'text/html':
            response = response.text
            file_path = f"/{file_topic}_raw_data/{date_string}/{file_topic}_{page}.html"
        else:
            response = response.text
            file_path = f"/{file_topic}_raw_data/{date_string}/{file_topic}_{page}.txt"

        load_data_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, s3_bucket_name,  response, file_path, content_type)
    
    print(f"Fetched {page_size} datas for total {end_page - start_page + 1} pages from {file_topic} apis.")
