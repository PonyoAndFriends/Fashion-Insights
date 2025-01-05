import threading


def execute_in_threads(target_function, args_list, max_threads):
    """
    멀티스레드로 함수를 실행하는 유틸리티 함수.

    :param target_function: 실행할 함수
    :param args_list: 각 스레드에서 실행할 함수의 인자 리스트 (각 인자는 튜플)
    :param max_threads: 동시에 실행할 최대 스레드 수
    """
    threads = []
    active_threads = []

    # 스레드 생성 및 시작
    for args in args_list:
        thread = threading.Thread(target=target_function, args=args)
        threads.append(thread)

        # 활성 스레드 관리
        while len(active_threads) >= max_threads:
            for t in active_threads:
                if not t.is_alive():
                    active_threads.remove(t)

        thread.start()
        active_threads.append(thread)

    # 모든 스레드 완료 대기
    for thread in threads:
        thread.join()
