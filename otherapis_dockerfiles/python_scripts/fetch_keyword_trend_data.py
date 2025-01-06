import argparse
import json
import requests
import logging
from datetime import datetime, timedelta
from script_modules import run_func_multi_thread, s3_upload

logger = logging.getLogger(__name__)


def split_keywords_into_batches(keywords, batch_size=5):
    """
    API에서 호출가능 한도에 따라 키워드 리스트에서 튜플의 마지막 원소(리스트)를 기준으로 batch_size 크기로 분할.

    :param keywords: 튜플의 리스트 (마지막 원소가 리스트)
    :param batch_size: 각 배치의 최대 크기
    :return: 분할된 튜플 리스트
    """
    logger.info("Splitting keywords into batches.")
    result = []
    try:
        for keyword_tuple in keywords:
            *prefix, keyword_list = keyword_tuple  # 튜플에서 마지막 리스트 분리
            for i in range(0, len(keyword_list), batch_size):
                # prefix에 분할된 리스트 추가하여 새로운 튜플 생성
                result.append((*prefix, keyword_list[i : i + batch_size]))
    except Exception as e:
        logger.error(f"Error splitting keywords: {e}", exc_info=True)
        raise
    return result


def fetch_fashion_keyword_data_and_load_to_s3(url, headers, keyword_batch, s3_dict):
    """
    API 호출과 결과를 S3에 업로드하는 함수 (requests 사용).
    """
    now = datetime.now()
    one_week_ago = now - timedelta(days=7)
    now_string = now.strftime("%Y-%m-%d")
    one_week_ago_string = one_week_ago.strftime("%Y-%m-%d")

    for i, (gender, second_category, third_category, keywords) in enumerate(
        keyword_batch
    ):
        logger.info(
            f"Processing batch {i + 1}/{len(keyword_batch)} for gender={gender}, category={second_category}-{third_category}."
        )
        logger.info(f"Current keywords: {keywords}")
        body = {
            "startDate": one_week_ago_string,
            "endDate": now_string,
            "timeUnit": "date",
            "category": "50000000",
            "keyword": [
                {
                    "name": f"{gender}_{second_category}_{third_category}_{keyword}_trend",
                    "param": [keyword],
                }
                for keyword in keywords
            ],
            "gender": "f" if gender == "여성" else "m",
        }

        logger.debug(f"Request body: {body}")
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()
        logger.info(f"API call successful for batch {i + 1}.")

        s3_dict["data_file"] = response.json()

        # 데이터를 구분하여 덮어쓰지 않고 저장하기 위해 파일 이름 끝에 숫자를 추가
        s3_dict["file_path"] = (
            f"/{now_string}/keyword_trend_raw_data/{gender}_{second_category}_{third_category}_trend_{i}.json"
        )
        s3_dict["content_type"] = "application/json"
        s3_upload.load_data_to_s3(s3_dict)
        logger.info(f"Batch {i + 1} uploaded to S3 successfully.")


def main():
    logger.info("Script started.")
    parser = argparse.ArgumentParser(
        description="Process keywords and upload API results to S3"
    )
    parser.add_argument("--url", required=True, help="JSON string of keywords")
    parser.add_argument("--headers", required=True, help="S3 client config as JSON")
    parser.add_argument(
        "--category_list", required=True, help="JSON string of keywords"
    )
    parser.add_argument(
        "--max_threads", type=int, default=15, help="Maximum number of threads"
    )
    parser.add_argument("--s3_dict", required=True, help="S3 client config as JSON")

    try:
        args = parser.parse_args()

        url = args.url
        headers = json.loads(args.headers)
        category_list = json.loads(args.category_list)
        max_threads = args.max_threads
        s3_dict = json.loads(args.s3_dict)

        logger.info(f"Parsed arguments: URL={url}, Max threads={max_threads}")
        logger.debug(f"Category list: {category_list}")
        logger.debug(f"S3 configuration: {s3_dict}")

        # 키워드 분할
        splited_keywords = split_keywords_into_batches(category_list, 5)
        logger.info(f"Split keywords into {len(splited_keywords)} batches.")

        # 멀티스레드 실행
        args = [(url, headers, keywords, s3_dict) for keywords in splited_keywords]
        run_func_multi_thread.execute_in_threads(
            fetch_fashion_keyword_data_and_load_to_s3, args, max_threads
        )
        logger.info("All batches processed successfully. Script completed.")
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
