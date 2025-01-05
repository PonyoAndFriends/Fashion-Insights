import argparse
import json
from script_modules import fetch_one_page_range_data
from script_modules import run_func_multi_thread


def process_page_ranges(
    url,
    file_topic,
    page_ranges,
    s3_dict,
    pagination_keyword,
    params=None,
    headers=None,
):
    """
    멀티스레드로 page range의 정보를 가져오기 위한 함수
    """
    args_list = [
        (
            (
                url,
                start_page,
                end_page,
                file_topic,
                s3_dict,
                pagination_keyword,
                params,
                headers,
            ),
        )
        for start_page, end_page in page_ranges
    ]

    run_func_multi_thread.execute_in_threads(fetch_one_page_range_data.fetch_page_range_data, args_list, 10)


def main():
    parser = argparse.ArgumentParser(description="Fetch data and upload to S3")
    parser.add_argument("--url", required=True, help="API URL")
    parser.add_argument("--page_ranges", required=True, help="Page ranges as JSON")
    parser.add_argument("--file_topic", required=True, help="S3 file topic")
    parser.add_argument(
        "--s3_dict", required=True, help="Configurations for s3 as JSON"
    )
    parser.add_argument(
        "--pagination_keyword",
        required=True,
        help="Name of key in query params for pagination",
    )
    parser.add_argument("--params", required=False, help="API query params as JSON")
    parser.add_argument("--headers", required=False, help="API Headers as JSON")

    args = parser.parse_args()

    url = args.url
    page_ranges = json.loads(args.page_ranges)
    file_topic = args.file_topic
    s3_dict = json.loads(args.s3_dict)
    pagination_keyword = args.pagination_keyword
    headers = json.loads(args.headers) if args.headers else None
    params = json.loads(args.params) if args.params else None

    process_page_ranges(
        url,
        file_topic,
        page_ranges,
        s3_dict,
        pagination_keyword,
        params,
        headers,
    )


if __name__ == "__main__":
    main()
