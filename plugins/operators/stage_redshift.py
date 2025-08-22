from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info(f"Staging data from S3 to Redshift table {self.table}")
        
        # Get AWS credentials from Airflow Metastore
        metastore = MetastoreBackend()
        aws_connection = metastore.get_connection(self.aws_credentials_id)
        access_key = aws_connection.login
        secret_key = aws_connection.password
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data from the table
        self.log.info(
            f"Deleting data from destination Redshift table {self.table}"
        )
        redshift_hook.run("DELETE FROM {}".format(self.table))
        
        # Render the S3 key with jinja templating
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # Build the COPY SQL statement
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            access_key,
            secret_key,
            self.region,
            self.json_path
        )
        
        # Execute the COPY command
        self.log.info(f"Executing COPY command: {formatted_sql}")
        redshift_hook.run(formatted_sql)
        self.log.info(f"Successfully staged data into table {self.table}")
