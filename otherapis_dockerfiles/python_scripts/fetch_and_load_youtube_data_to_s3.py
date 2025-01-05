import json
import googleapiclient.discovery
import re
import argparse

from script_modules import s3_upload
from script_modules import run_func_multi_thread
from datetime import datetime


def parse_duration(duration):
    """ISO 8601 포맷의 영상 길이를 초 단위로 변환"""
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration)
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds


def get_videos_with_details(
    youtube, input_category_data, s3_dict, file_topic, max_results=3
):
    """검색 결과와 세부 정보를 통합하여 반환"""
    gender, first_depth, second_depth, categories = input_category_data
    now = datetime.now()
    now_string = now.strftime("%Y-%m-%d")

    for category in categories:
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

        # videos().list 호출
        video_details_response = (
            youtube.videos()
            .list(id=",".join(video_ids), part="snippet,contentDetails,statistics")
            .execute()
        )

        # 통합 데이터 생성
        videos = []
        for video_info in video_details_response["items"]:
            duration = parse_duration(video_info["contentDetails"]["duration"])
            # 영상 길이 필터 (3분 ~ 20분 사이)
            if 180 <= duration <= 1800:
                videos.append(
                    {
                        "search_category": "",
                        "videoId": video_info["id"],
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

        s3_dict["data_file"] = videos
        s3_dict["file_path"] = (
            f"/{file_topic}_raw_data/{now_string}/{gender}_{first_depth}_{second_depth}_{file_topic}_data.json"
        )
        s3_upload.load_data_to_s3(s3_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process keywords and upload API results to S3"
    )
    parser.add_argument("--youtube_api_key", required=True, help="Youtube API key")
    parser.add_argument("--file_topic", required=True, help="Topic of data")
    parser.add_argument(
        "--category_list", required=True, help="JSON string of keywords"
    )
    parser.add_argument(
        "--max_threads", type=int, default=15, help="Maximum number of threads"
    )
    parser.add_argument("--s3_dict", required=True, help="S3 client config as JSON")

    args = parser.parse_args()

    api_key = args.youtube_api_key
    file_topic = args.file_topic
    category_list = json.loads(args.category_list)
    max_threads = args.max_threads
    s3_dict = json.loads(args.s3_dict)

    # YouTube Data API 설정
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    # 각 입력 데이터와 관련된 작업 생성
    args_list = [
        (youtube, (gender, first_depth, second_depth, categories), s3_dict, file_topic)
        for gender, first_depth, second_depth, categories in category_list
    ]

    run_func_multi_thread.execute_in_threads(
        get_videos_with_details, args_list, max_threads
    )
