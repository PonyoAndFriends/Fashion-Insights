from airflow.operators.python import PythonOperator
import math


class CalculatePageRangeOperator(PythonOperator):
    """
    page range(튜플의 리스트)를 계산하여 반환하는 커스텀 오퍼레이터

    :param total_count: 총 가져올 데이터의 수
    :param page_size: 페이지 당 가져올 데이터의 수
    :param parallel_task_num: 병렬 실행할 태스크의 수
    """

    def __init__(self, total_count, page_size, parallel_task_num, *args, **kwargs):
        super().__init__(python_callable=self._calculate_page_range, *args, **kwargs)
        self.total_count = total_count
        self.page_size = page_size
        self.parallel_task_num = parallel_task_num

    def _calculate_page_range(self):
        # 전체 페이지 수, 태스크 당 나눠줄 페이지 수 계산
        total_pages = math.ceil(self.total_count / self.page_size)
        pages_per_task = math.ceil(total_pages / self.parallel_task_num)

        # 페이지 범위를 담은 튜플의 리스트를 반환
        page_ranges = [
            (i * pages_per_task + 1, min((i + 1) * pages_per_task, total_pages))
            for i in range(self.parallel_task_num)
        ]

        self.log.info(f"Calculated page ranges: {page_ranges}")
        return page_ranges
