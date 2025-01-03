from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift import RedshiftSQLHook


class S3ToRedshiftCustomOperator(BaseOperator):
    """
    S3의 파일을 Redshift 내로 bulk update하는 Custom Operator

    Args:
        schema (str): 레드시프트의 스키마(데이터베이스) 이름
        table (str): 테이블 명
        columns (dict): 딕셔너리 형태의 컬럼 정보, key는 컬럼명 value는 컬럼 타입
        s3_bucket (str): S3 bucket 이름
        s3_key (str): S3 파일 경로
        copy_options (list): COPY 명령어의 부가 옵션
        aws_conn_id (str): airflow의 connections id, s3와 연결을 위해 사용
        redshift_conn_id (str): airflow의 connections id, redshift와 연결을 위해 사용
    """

    @apply_defaults
    def __init__(
        self,
        schema: str,  # 필수 작성
        table: str,  # 필수 작성
        columns: dict,  # 필수 작성
        s3_bucket: str,  # 필수 작성
        s3_key: str,  # 필수 작성
        copy_options: list = None,
        aws_conn_id: str = "aws_default",
        redshift_conn_id: str = "redshift_default",
        *args,
        **kwargs,
    ):
        super(S3ToRedshiftCustomOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.columns = columns
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_options = copy_options or ["FORMAT AS PARQUET"]
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        if not s3_hook.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            raise FileNotFoundError(f"The file {s3_path} does not exist in S3.")

        redshift_hook = RedshiftSQLHook(postgres_conn_id=self.redshift_conn_id)

        # 테이블명이 없다면 생성
        column_definitions = ", ".join(
            [f"{col} {dtype}" for col, dtype in self.columns.items()]
        )
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table} (
            {column_definitions}
        );
        """

        self.log.info(f"Executing CREATE TABLE query: {create_table_query}")
        redshift_hook.run(create_table_query)

        # 카피 커맨드 실행 - PK 등은 적절히 딕셔너리의 value를 수정
        copy_query = f"""
        COPY {self.schema}.{self.table}
        FROM '{s3_path}'
        CREDENTIALS 'aws_access_key_id={s3_hook.get_credentials().access_key};aws_secret_access_key={s3_hook.get_credentials().secret_key}'
        {" ".join(self.copy_options)};
        """

        self.log.info(f"Executing COPY command: {copy_query}")
        redshift_hook.run(copy_query)

        self.log.info(
            f"Data successfully copied from {s3_path} to {self.schema}.{self.table}"
        )
