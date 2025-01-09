from airflow.models import BaseOperator


class CategoryDictionaryMergeAndExplodeOperator(BaseOperator):
    """
    딕셔너리 리스트를 받아서 merge_dictionaries와 extract_tuples를 순차적으로 실행하는 커스텀 오퍼레이터.
    """

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
        딕셔너리와 리스트를 재귀적으로 탐색하여 모든 depth를 펼치고
        리스트의 원소를 튜플로 반환.
        """
        tuples = []
        if isinstance(data, dict):
            for key, value in data.items():
                tuples.extend(self.extract_tuples(value, keys + [key]))
        elif isinstance(data, list):
            for item in data:
                tuples.append((*keys, item))
        else:
            tuples.append((*keys, data))
        return tuples

    def execute(self, context):
        self.log.info("Merging dictionaries...")
        merged_dict = self.merge_dictionaries(self.dict_list)
        self.log.info(f"Merged Dictionary: {merged_dict}")

        self.log.info("Extracting tuples...")
        extracted_tuples = self.extract_tuples(merged_dict)
        self.log.info(f"Extracted Tuples: {extracted_tuples}")

        return extracted_tuples
