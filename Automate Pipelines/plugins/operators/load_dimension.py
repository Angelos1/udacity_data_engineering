from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 select_sql = '',
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Populating {} dimension table...'.format(self.table))
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info('Truncating {} dim table'.format(self.table))
            redshift.run('DELETE FROM {}'.format(self.table))
            
        insert_sql = 'INSERT INTO {} \n {}'.format(self.table, self.select_sql)
        self.log.info('Executing:\n {}'.format(insert_sql))
        redshift.run(insert_sql)    