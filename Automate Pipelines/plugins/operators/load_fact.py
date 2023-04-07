from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 select_sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        
        
    def execute(self, context):
        self.log.info('Populating {} facts table... '.format(self.table))
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        insert_sql = 'INSERT INTO {} \n {}'.format(self.table, self.select_sql)
        self.log.info('Executing:\n {}'.format(insert_sql))
        redshift.run(insert_sql)