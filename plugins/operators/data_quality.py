from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator starting')
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            self.log.info(f'Running data quality check for {table} table')
            recs = redshift_hook.get_records(f'SELECT count(*) FROM {table}')
            if len(recs) < 1 or len(recs[0]) < 1:
                raise ValueError(f'Check failed for {table} as no results were returned ')
            num_records = recs[0][0]
            if num_records < 1:
                raise ValueError(f'Check failed for {table} as it contained 0 rows')
            self.log.info(f'Quality check on table {table} passed with {recs[0][0]} records')
