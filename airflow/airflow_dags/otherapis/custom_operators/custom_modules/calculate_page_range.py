import math
import logging

def calculate_page_range(total_count, page_size, parallel_task_num, *args, **kwargs):
    """
    데이터를 가져올 때 여러 task를 병렬로 실행하기 위해 페이지를 각 태스크에게 분배할 범위를 결정하는 함수

    :param total_count: 총 가져올 데이터의 개수
    :param page_size: 한 페이지에 담길 데이터의 수를 결정
    :param parallel_task_num: 병렬로 실행할 task의 개수
    """
    total_pages = math.ceil(total_count / page_size)
    pages_per_task = math.ceil(total_pages / parallel_task_num)
    logging.info(f"Calculating page ranges for total {total_count} records, {total_pages} pages for {parallel_task_num} tasks")

    page_ranges = [
        (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
        for i in range(parallel_task_num)
    ]

    logging.info(f"Calculated page ranges: {page_ranges}")
    return page_ranges