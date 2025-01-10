from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import json


class CustomKubernetesPodOperator(KubernetesPodOperator):
    """
    KubernetesPodOperator를 확장한 추상화된 커스텀 오퍼레이터.

    :param script_path: Pod 내에서 실행할 스크립트의 경로
    :param required_args: 필수 아규먼트 딕셔너리 (key-value 형태)
    :param optional_args: 선택적 아규먼트 딕셔너리 (key-value 형태)
    :param cpu_limit: Pod에 할당할 CPU 제한
    :param memory_limit: Pod에 할당할 메모리 제한
    :param cpu_request: Pod가 요청할 CPU
    :param memory_limit: Pod가 요청할 메모리
    :param image: 실행할 Docker 이미지
    :param namespace: Pod가 실행될 K8S namespace
    :param kwargs: KubernetesPodOperator의 추가 매개변수
    """

    def __init__(
        self,
        script_path,
        is_delete_operator_pod=True,
        get_logs=True,
        required_args=None,
        optional_args=None,
        cpu_limit="500m",
        memory_limit="512Mi",
        image="coffeeisnan/python_pod_image:latest",
        namespace="airflow",
        *args,
        **kwargs,
    ):
        self.script_path = script_path
        self.required_args = required_args or {}
        self.optional_args = optional_args or {}
        self.namespace = namespace
        self.is_delete_operator_pod = is_delete_operator_pod
        self.get_logs = get_logs

        # arguments 구성하기
        arguments = self._generate_arguments()

        # kwargs에서 arguments 제거
        if "arguments" in kwargs:
            self.log.warning("Removing duplicate 'arguments' from kwargs.")
            del kwargs["arguments"]

        super().__init__(
            cmds=["python", self.script_path],
            arguments=arguments,
            image=image,
            namespace=self.namespace,
            is_delete_operator_pod = self.is_delete_operator_pod,
            get_logs = self.get_logs,
            *args,
            **kwargs,
        )

    def _generate_arguments(self):
        """필수 및 선택적 아규먼트를 병합하여 리스트로 반환"""
        args = []

        # 필수 아규먼트 추가
        for key, value in self.required_args.items():
            args.append(f"--{key}")
            args.append(
                json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
            )

        # 선택적 아규먼트 추가
        for key, value in self.optional_args.items():
            if value is not None:
                args.append(f"--{key}")
                args.append(
                    json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
                )

        return args
