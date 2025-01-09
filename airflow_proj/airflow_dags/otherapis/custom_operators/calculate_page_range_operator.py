from airflow.models import BaseOperator
import math


class CalculatePageRangeOperator(BaseOperator):
    """
    page range(튜플의 리스트)를 계산하여 반환하는 커스텀 오퍼레이터

    :param total_count: 총 가져올 데이터의 수
    :param page_size: 페이지 당 가져올 데이터의 수
    :param parallel_process_num: 병렬 실행할 프로세스(혹은 태스크)의 수
    """

    def __init__(self, total_count, page_size, parallel_process_num, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_count = total_count
        self.page_size = page_size
        self.parallel_process_num = parallel_process_num

    def execute(self, context):
        # 전체 페이지 수, 태스크 당 나눠줄 페이지 수 계산
        total_pages = math.ceil(self.total_count / self.page_size)
        pages_per_task = math.ceil(total_pages / self.parallel_process_num)

        # 페이지 범위를 담은 튜플의 리스트를 반환
        page_ranges = [
            (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
            for i in range(self.parallel_process_num)
        ]

        self.log.info(f"Calculated page ranges: {page_ranges}")
        return page_ranges
