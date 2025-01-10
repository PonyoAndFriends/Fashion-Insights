import argparse
import json
import requests
import logging
from datetime import datetime, timedelta
from script_modules import s3_upload
from zoneinfo import ZoneInfo

WANT_RANK = 5  # 원하는 등수

logger = logging.getLogger(__name__)


def sort_by_weekly_ratio(url, headers, keywords, gender):
    """
    키워드 트렌드 데이터 api를 호출, 1주일 단위로 반환했을 때 기준 오늘의 ratio 필드로 상대적 순위를 결정
    상대 순위를 기반으로 정렬된 키워드들을 반환
    """
    now = datetime.now().astimezone(ZoneInfo("Asia/Seoul"))
    one_week_ago = now - timedelta(days=7)
    now_string = now.strftime("%Y-%m-%d")
    one_week_ago_string = one_week_ago.strftime("%Y-%m-%d")

    ratios = []  # 오늘의 ratio 값을 저장

    logger.info(f"Fetching data for keywords={keywords}.")
    body = {
        "startDate": one_week_ago_string,
        "endDate": now_string,
        "timeUnit": "date",
        "category": "50000000",
        "keyword": [
            {"name": f"{'female' if gender == '여성' else 'male'}_{keyword}_trend", "param": [keyword]}
            for keyword in keywords
        ],
        "gender": "f" if gender == "여성" else "m",
    }

    logger.info(f"url: {url}")
    logger.info(f"headers: {headers}")
    logger.info(f"body: {body}")
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    data = response.json()

    # 오늘의 ratio 값 추출
    for result in data["results"]:
        keyword = result["keyword"][0]

        if len(result["data"]) == 0:
            today_ratio = 0
        else:
            ratios = result["data"][-1]["ratio"]
            today_ratio = ratios["ratio"]  # 마지막 날짜의 ratio 값
        ratios.append((keyword, float(today_ratio)))

    ratios.sort(key=lambda x: x[1], reverse=True)
    return [keyword for keyword, _ in ratios]


def get_top_five_keyword(url, headers, all_keywords, gender):
    """
    상위 5개의 트렌드를 얻기 위하여 api를 반복 호출하며 서로 순위를 비교함
    """
    current_top_5 = sort_by_weekly_ratio(url, headers, all_keywords[:5], gender)
    remainders = all_keywords[5:]

    for new_keyword in remainders:
        sorted_keywords_with_new_keyword = sort_by_weekly_ratio(
            url, headers, all_keywords[:4] + [new_keyword], gender
        )

        if sorted_keywords_with_new_keyword[-1] == new_keyword:
            current_fifth = current_top_5[-1]
            current_top_5[-1] = sort_by_weekly_ratio(
                url, headers, [current_fifth, new_keyword], gender
            )
        else:
            current_top_5 = sorted_keywords_with_new_keyword

    return current_top_5


def fetch_final_data(url, headers, s3_dict, top_5_keywords, gender):
    """
    최종 상위 5개 키워드로 API 호출하여 데이터를 반환.
    """
    logger.info(f"Fetching final data for top 5 keywords: {top_5_keywords}")
    body = {
        "startDate": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "timeUnit": "date",
        "category": "50000000",
        "keyword": [
            {"name": f"{keyword}_trend", "param": [keyword]}
            for keyword in top_5_keywords
        ],
        "gender": "f" if gender == "여성" else "m",
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    now_string = datetime.now().astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")

    # 최종 데이터 업로드
    s3_dict["data_file"] = response.json()
    s3_dict["file_path"] = (
        f"bronze/{now_string}/otherapis/{gender}_keyword_trends/final_top_5_keywords_data.json"
    )
    s3_dict["content_type"] = "application/json"
    s3_upload.load_data_to_s3(s3_dict)
    logger.info("Final top 5 keyword data uploaded to S3.")


def main():
    logger.info("Script started.")
    parser = argparse.ArgumentParser(
        description="Process keywords and upload API results to S3"
    )
    parser.add_argument("--url", required=True, help="API endpoint URL")
    parser.add_argument("--headers", required=True, help="API headers as JSON string")
    parser.add_argument("--all_keywords", required=True, help="List of all keywords")
    parser.add_argument(
        "--gender", required=True, help="Gender of current category list"
    )
    parser.add_argument("--s3_dict", required=True, help="S3 client config as JSON")

    try:
        args = parser.parse_args()

        url = args.url
        headers = json.loads(args.headers)
        gender = args.gender
        all_keywords = json.loads(args.all_keywords)
        s3_dict = json.loads(args.s3_dict)

        logger.info("Parsed arguments and started processing.")

        four_keywords = [keywords[-1] for keywords in all_keywords]
        top_five_keywords = get_top_five_keyword(url, headers, four_keywords, gender)
        fetch_final_data(url, headers, s3_dict, top_five_keywords, gender)

        logger.info("Script completed successfully.")
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
