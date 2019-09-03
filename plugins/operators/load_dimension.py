from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 sql = '',
                 table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator starting for {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Loading dim table in Redshift")
        insert_sql = '''INSERT INTO {}
                        {};
                        COMMIT;'''.format(
            self.table,
            self.sql
        )
        redshift.run(insert_sql)
