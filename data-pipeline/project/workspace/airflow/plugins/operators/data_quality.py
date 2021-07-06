from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 test_function=None,
                 test_function_args=(),
                 *args, **kwargs):
        """
        Initiate operator object
        :param redshift_conn_id: airflow connection id for redshift hook
        :param table_list_to_check: the list of tables to check for validation
        :param args
        :param kwargs
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.test_function = test_function
        self.test_function_args = test_function_args

    def execute(self, context):
        """
        execute data validation against Redshift cluster
        :param context: operator context
        """
        self.test_function(*self.test_function_args)
        self.log.info("Done validating")

