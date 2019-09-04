from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_key='',
                 s3_path='',
                 aws_creds='',
                 conn_id='',
                 staging_table='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.s3_path=s3_path
        self.aws_creds=aws_creds
        self.staging_table=staging_table

    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting')
        aws_hook = AwsHook(self.aws_creds)
        creds = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info('Created connection to Redshift')

        self.log.info('Copying data from S3 to Redshift')
        key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{key}'
        self.log.info(f'S3 path: {s3_path}')

        sql = f"""COPY {self.staging_table}
                    FROM '{s3_path}'
                    ACCESS_KEY_ID '{creds.access_key}'
                    SECRET_ACCESS_KEY '{creds.secret_key}'
                    JSON '{self.s3_path}'
                    COMPUPDATE OFF
                """
        redshift.run(sql)
