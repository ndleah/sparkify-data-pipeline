from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table = table
        self.load_method = load_method

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.load_method == 'overwrite':
            postgres.run(f'TRUNCATE {self.table}')
        
        postgres.run(f"INSERT INTO {self.table} " + self.query)