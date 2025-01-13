import json
import googleapiclient.discovery
import re, time
import logging
import argparse

from script_modules import s3_upload
from script_modules import run_func_multi_thread
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from script_modules.gender_category_list import FEMALE_CATEGORY_LIST, MALE_CATEGORY_LIST


logger = logging.getLogger(__name__)


def group_categories_by_key(category_list):
    """gender, first_depth, second_depth가 같은 항목들을 묶음"""
    grouped_data = defaultdict(list)
    for gender, first_depth, second_depth, category in category_list:
        key = (gender, first_depth, second_depth)
        grouped_data[key].append(category)
    return grouped_data


def parse_duration(duration):
    """ISO 8601 포맷의 영상 길이를 초 단위로 변환"""
    try:
        match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        logger.debug(
            f"Parsed duration: {duration} -> {hours * 3600 + minutes * 60 + seconds} seconds"
        )
        return hours * 3600 + minutes * 60 + seconds
    except Exception as e:
        logger.error(f"Failed to parse duration: {duration}, error: {e}", exc_info=True)
        raise


def get_videos_with_details(
    youtube, input_category_data, s3_dict, file_topic, max_results=3
):
    """검색 결과와 세부 정보를 통합하여 반환"""
    gender, first_depth, second_depth, categories = input_category_data
    now = datetime.now().astimezone(ZoneInfo("Asia/Seoul"))
    now_string = now.strftime("%Y-%m-%d")

    for category in categories:
        logger.info(f"Fetching videos for category: {category}")
        time.sleep(1.5)
        search_response = (
            youtube.search()
            .list(
                q=category,
                part="snippet",
                type="video",
                maxResults=30,
                order="relevance",
            )
            .execute()
        )

        # videoId 수집
        video_ids = [item["id"]["videoId"] for item in search_response["items"]]
        logger.debug(f"Collected video IDs: {video_ids}")

        # videos().list 호출
        video_details_response = (
            youtube.videos()
            .list(id=",".join(video_ids), part="snippet,contentDetails,statistics")
            .execute()
        )

        # 통합 데이터 생성
        videos = []
        for video_info in video_details_response["items"]:
            try:
                duration = parse_duration(video_info["contentDetails"]["duration"])
                # 영상 길이 필터 (3분 ~ 20분 사이)
                if 180 <= duration <= 1800:
                    videos.append(
                        {
                            "search_category": f"{category}",
                            "videoId": video_info["id"],
                            "category_name": f"{gender}_{first_depth}_{second_depth}_{category}",
                            "title": video_info["snippet"]["title"],
                            "publishedAt": video_info["snippet"]["publishedAt"],
                            "description": video_info["snippet"].get("description", ""),
                            "channelName": video_info["snippet"]["channelTitle"],
                            "thumbnailUrl": video_info["snippet"]["thumbnails"]["high"][
                                "url"
                            ],
                            "tags": video_info["snippet"].get("tags", []),
                            "categoryId": video_info["snippet"].get("categoryId", ""),
                            "duration": duration,
                            "dimension": video_info["contentDetails"]["dimension"],
                            "definition": video_info["contentDetails"]["definition"],
                            "viewCount": video_info["statistics"].get("viewCount", "0"),
                            "likeCount": video_info["statistics"].get("likeCount", "0"),
                            "commentCount": video_info["statistics"].get(
                                "commentCount", "0"
                            ),
                        }
                    )
                    if len(videos) == max_results:  # 최대 결과 수 제한
                        break
            except Exception as e:
                logger.error(
                    f"Error processing video: {video_info['id']}, error: {e}",
                    exc_info=True,
                )

        logger.info(f"Collected {len(videos)} videos for category: {category}")

        s3_dict["data_file"] = json.dumps(videos)
        s3_dict["file_path"] = (
            f"bronze/{now_string}/otherapis/{gender}_{file_topic}_raw_data/{gender}_{first_depth}_{second_depth}_{category}_data.json"
        )

        logger.debug(f"Uploading data to S3 file path: {s3_dict['file_path']}")
        s3_upload.load_data_to_s3(s3_dict)
        logger.info(f"Data uploaded to S3 for category: {category}")


if __name__ == "__main__":
    logger.info("Script started.")
    parser = argparse.ArgumentParser(
        description="Process keywords and upload API results to S3"
    )
    parser.add_argument("--youtube_api_key", required=True, help="Youtube API key")
    parser.add_argument("--file_topic", required=True, help="Topic of data")
    parser.add_argument(
        "--list_choice", required=True, help="string for choice of gender"
    )
    parser.add_argument(
        "--max_threads", type=int, default=8, help="Maximum number of threads"
    )
    parser.add_argument("--s3_dict", required=True, help="S3 client config as JSON")

    try:
        args = parser.parse_args()

        api_key = args.youtube_api_key
        file_topic = args.file_topic
        max_threads = args.max_threads
        s3_dict = json.loads(args.s3_dict)
        list_choice = args.list_choice

        if list_choice == "f":
            category_list = FEMALE_CATEGORY_LIST
        else:
            category_list = MALE_CATEGORY_LIST

        logger.info(
            f"Parsed arguments: API key provided, File topic: {file_topic}, Max threads: {max_threads}"
        )
        logger.debug(f"Category list: {category_list}")
        logger.debug(f"S3 configuration: {s3_dict}")

        # YouTube Data API 설정
        youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
        logger.info("YouTube API client initialized.")

        # 카테고리 그룹화
        grouped_categories = group_categories_by_key(category_list)
        logger.debug(f"Grouped categories: {grouped_categories}")

        # 각 입력 데이터와 관련된 작업 생성
        args_list = [
            (
                youtube,
                (gender, first_depth, second_depth, categories),
                s3_dict,
                file_topic,
            )
            for (
                gender,
                first_depth,
                second_depth,
            ), categories in grouped_categories.items()
        ]

        run_func_multi_thread.execute_in_threads(
            get_videos_with_details, args_list, max_threads
        )
        logger.info("Script completed successfully.")
    except Exception as e:
        logger.error(f"Script failed: {e}", exc_info=True)
        raise
