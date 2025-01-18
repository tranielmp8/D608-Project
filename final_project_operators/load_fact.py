from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 conn_id='aws_credentials',
                 conn_type='Amazon Web Services',
                 aws_key_id='',
                 aws_secret_key='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        conn_id=conn_id
        conn_type=conn_type
        aws_key_id=aws_key_id
        aws_secret_key=aws_secret_key

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
