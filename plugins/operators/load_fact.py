from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 sql = '',
                 table = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info(f'LoadFactOperator starting for {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Loading fact table in Redshift")
        insert_sql = '''INSERT INTO {}
                        {};
                        COMMIT;'''.format(
            self.table,
            self.sql
        )
        redshift.run(insert_sql)
