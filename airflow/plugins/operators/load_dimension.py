# Import necessary modules and libraries
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                table,
                redshift_conn_id='redshift',
                select_sql='',
                mode='append',
                *args, **kwargs):
        """
        Initialize the LoadDimensionOperator.

        :param table: Target table name in Redshift.
        :param redshift_conn_id: Airflow connection ID for Redshift.
        :param select_sql: SQL query to select data for insertion.
        :param mode: Mode of insertion ('append' or 'truncate').
        :param args: Additional arguments.
        :param kwargs: Additional keyword arguments.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        """
        Execute the dimension table loading process.

        :param context: The context dictionary passed by Airflow.
        """
        redshift_hook = PostgresHook("redshift")

        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimension table...')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info("Deletion complete.")

        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        self.log.info(f'Loading data into {self.table} dimension table...')
        redshift_hook.run(sql)
        self.log.info("Loading complete.")
