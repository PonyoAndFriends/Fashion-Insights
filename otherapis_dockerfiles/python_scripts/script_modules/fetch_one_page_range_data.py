from datetime import datetime
import requests, json, sys, logging
import s3_upload

logger = logging.getLogger(__name__)


def fetch_page_range_data(
    url,
    start_page,
    end_page,
    file_topic,
    s3_dict,
    pagination_keyword,
    params,
    headers,
):
    """
    페이지 레인지를 받아 api를 페이지 마다 요청하여 가져옴.
    """
    for page in range(start_page, end_page + 1):
        try:
            # 페이지 변경
            params[pagination_keyword] = page

            # api 호출
            logger.info(f"Fetching data for page {page} from URL: {url}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # s3_dict의 빈 값, data_file, file_path을 원하는 설정으로 채움.
            now = datetime.now()
            now_string = now.strftime("%Y-%m-%d")
            file_ext = {
                "application/json": "json",
                "text/html": "html",
                "plain/text": "txt",
            }

            if s3_dict["content_type"] == "application/json":
                s3_dict["data_file"] = json.dumps(data, ensure_ascii=False)
            else:
                s3_dict["data_file"] = response.text

            s3_dict["file_path"] = (
                f"/{file_topic}_raw_data/{now_string}/{file_topic}_page_{page}.{file_ext[s3_dict['content_type']]}"
            )

            s3_upload.load_data_to_s3(s3_dict)
            logger.info(f"Successfully uploaded page {page} data to S3.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred on page {page}: {e}")
