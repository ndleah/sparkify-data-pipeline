# Import necessary modules and libraries
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 *args, **kwargs):
        """
        Initialize the LoadFactOperator.
        
        Args:
            table (str): The target table name to load data into.
            redshift_conn_id (str): The Airflow connection ID for Redshift.
            select_sql (str): The SELECT SQL query for extracting data to load.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        """
        Execute the loading of data into the specified fact table.
        
        Args:
            context (dict): The context dictionary provided by Airflow.

        Raises:
            None.
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} fact table...')

        sql = """
            INSERT INTO {table}
            {select_sql};
        """.format(table=self.table, select_sql=self.select_sql)

        redshift_hook.run(sql)
        self.log.info("Loading complete.")

