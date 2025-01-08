from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class RedshiftQueryOperator(BaseOperator):
    """
    Custom Operator to execute SQL queries on Redshift.
    """

    def __init__(self, sql, redshift_conn_id="redshift_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        # Create a Redshift Hook
        self.log.info(f"Executing SQL on Redshift: {self.sql}")
        hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        hook.run(self.sql)
        self.log.info("Query executed successfully.")
