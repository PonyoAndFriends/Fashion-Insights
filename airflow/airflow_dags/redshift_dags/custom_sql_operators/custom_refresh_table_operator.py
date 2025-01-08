from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class RefreshTableOperator(BaseOperator):
    """
    Custom Operator to execute SQL queries on Redshift.
    """

    def __init__(
        self, drop_sql, create_sql, redshift_conn_id="redshift_default", *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.drop_sql = drop_sql
        self.create_sql = create_sql

    def execute(self, context):
        hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        # 해당 테이블이 이미 있다면 삭제
        self.log.info(f"Drop table if exists...")
        hook.run(self.drop_sql)
        self.log.info("Drop query executed successfully.")

        # 테이블 재생성
        self.log.info(f"Create table...")
        hook.run(self.create_sql)
        self.log.info("Drop query executed successfully.")
