from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table_list_to_check=(),
                 *args, **kwargs):
        """
        Initiate operator object
        :param redshift_conn_id: airflow connection id for redshift hook
        :param table_list_to_check: the list of tables to check for validation
        :param args
        :param kwargs
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_list_to_check = table_list_to_check

    def execute(self, context):
        """
        execute data validation against Redshift cluster
        :param context: operator context
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.table_list_to_check:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
