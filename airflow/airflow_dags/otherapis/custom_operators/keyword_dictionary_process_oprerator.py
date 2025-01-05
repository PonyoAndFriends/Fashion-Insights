from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_modules import json_dictionary_process


class KeywordDictionaryMergeAndExtractOperator(BaseOperator):
    """
    딕셔너리 리스트를 받아서 merge_dictionaries와 extract_tuples를 순차적으로 실행하는 커스텀 오퍼레이터.
    """

    @apply_defaults
    def __init__(self, dict_list, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dict_list = dict_list

    def merge_dictionaries(self, dict_list):
        """
        딕셔너리의 리스트를 받아서 구조를 유지하며 하나의 큰 딕셔너리로 만드는 함수
        """
        combined_category = {}
        for category in dict_list:
            for key, value in category.items():
                if key not in combined_category:
                    combined_category[key] = {}
                for sub_key, sub_value in value.items():
                    combined_category[key][sub_key] = sub_value

        return combined_category

    def extract_tuples(self, data, keys=[]):
        """
        딕셔너리를 탐색하여 3depth까지는 키를 유지하고,
        그 이후의 4depth의 리스트는 그대로 반환하여 튜플의 리스트를 반환
        (1st depth(gender), 2nd depth(category), 3rd depth(category), 4th depth(list))
        """
        tuples = []
        if isinstance(data, dict):
            for key, value in data.items():
                # 3뎁스까지만 튜플 생성
                if len(keys) < 2:
                    tuples.extend(self.extract_tuples(value, keys + [key]))
                else:
                    tuples.append((keys[0], keys[1], key, value))
        elif isinstance(data, list):
            # 3뎁스 이후의 리스트는 그대로 반환
            tuples.append((keys[0], keys[1], keys[2], data))
        return tuples

    def execute(self, context):

        self.log.info("Merging dictionaries...")
        merged_dict = json_dictionary_process.merge_dictionaries(self.dict_list)
        self.log.info(f"Merged Dictionary: {merged_dict}")

        self.log.info("Extracting tuples...")
        extracted_tuples = json_dictionary_process.extract_tuples(merged_dict)
        self.log.info(f"Extracted Tuples: {extracted_tuples}")

        return extracted_tuples
