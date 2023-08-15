# Import necessary modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

# Define a custom operator for staging data from S3 to Redshift
class StageToRedshiftOperator(BaseOperator):
    # Set UI color for the Airflow UI
    ui_color = '#8EB6D4'

    # Initialize the operator with required parameters
    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options='',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Assign input parameters to instance variables
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    # Execute method to perform the actual data staging
    def execute(self, context):
        # Create an AWS hook to retrieve credentials
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        # Create a Redshift hook to interact with the database
        redshift_hook = PostgresHook("redshift")

        # Log the start of data staging process
        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{self.s3_prefix} to {self.table} table...')

        # Construct the COPY query for data transfer
        copy_query = """
                    COPY {table}
                    FROM 's3://{s3_bucket}/{s3_prefix}'
                    with credentials
                    'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                    {copy_options};
                """.format(table=self.table,
                           s3_bucket=self.s3_bucket,
                           s3_prefix=self.s3_prefix,
                           access_key=credentials.access_key,
                           secret_key=credentials.secret_key,
                           copy_options=self.copy_options)

        # Log the start of the COPY command execution
        self.log.info('Executing COPY command...')
        
        # Execute the COPY command on Redshift
        redshift_hook.run(copy_query)
        
        # Log the completion of the COPY command
        self.log.info("COPY command complete.")
