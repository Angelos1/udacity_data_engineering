from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):


        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Quality check for the ETL starting...')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.checks:
            records = redshift_hook.get_records(check['test_sql'])
            num_records = records[0][0]
            if comparison == '<':
                if num_records < checks['result']:
                    raise ValueError(check['error_message'])
            elif comparison == '>': 
                if num_records > checks['result']:
                    raise ValueError(check['error_message'])
            elif comparison == '=' or comparison == '=':
                if num_records == checks['result']:
                    raise ValueError(check['error_message'])        
        self.log.info("Data quality checks passed ")
