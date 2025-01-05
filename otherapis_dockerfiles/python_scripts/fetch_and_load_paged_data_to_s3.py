import argparse, logging, json
from script_modules import fetch_one_page_range_data, run_func_multi_thread

logger = logging.getLogger(__name__)

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
    logger.info(f"Starting fetching data from URL: {url}, Data Topic: {file_topic}")
    logger.debug(f"Page ranges: {page_ranges}")
    logger.debug(f"S3 configuration: {s3_dict}")

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

    try:
        run_func_multi_thread.execute_in_threads(
            fetch_one_page_range_data.fetch_page_range_data, args_list, 10
        )
        logger.info("All page ranges processed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during multi-threaded execution: {e}", exc_info=True)
        raise



def main():
    logger.info("Script started.")
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
    logger.info(f"Arguments parsed: URL={url}, File topic={file_topic}, Page ranges={page_ranges}")

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
