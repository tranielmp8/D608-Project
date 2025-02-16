from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                JSON '{}'
                """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
            redshift_conn_id='',
            aws_credentials_id='',
            table='',
            s3_bucket='',
            s3_key='',
            s3_json="",
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json=s3_json

        try:

            self.log.info('connect to aws')
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()

            self.log.info('connect to redshift')
            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.log.info('remove tables')
            redshift.run("DELETE FROM {}".format(self.table))

            if self.table == 'staging_events':
                rendered_key = self.s3_key.format(**context)
                s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
                
            if self.table == 'staging_songs':
                s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

            self.log.info('s3_path: {}'.format(s3_path))

            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.s3_json
            )

            self.log.info('formatted_sql: {}'.format(formatted_sql))
            
            redshift.run(formatted_sql)


        except Exception as err:
            self.log.exception(err)
            raise err