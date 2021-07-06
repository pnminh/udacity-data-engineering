from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    dimension_sql_truncate_template = """
        TRUNCATE {dimension_table_name}; 
    """
    dimension_sql_template = """
            INSERT INTO {dimension_table_name}
            {dimension_table_insert_query};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_table_name="",
                 dimension_table_insert_query="",
                 append_only=False,
                 *args, **kwargs):
        """
        Initiate operator object
        :param redshift_conn_id: airflow connection id for redshift hook
        :param dimension_table_name
        :param dimension_table_insert_query: the insert query to be run against the dimension table
        :param append_only: specify if the insert mode is "append-only"(True) or "delete-load"(False)
        :param args
        :param kwargs
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table_name = dimension_table_name
        self.dimension_table_insert_query = dimension_table_insert_query
        self.append_only = append_only

    def execute(self, context):
        """
        execute dimension table data insert against Redshift cluster
        :param context: operator context
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            dimension_truncate_sql = LoadDimensionOperator.dimension_sql_truncate_template.format(
                dimension_table_name=self.dimension_table_name
            )
            self.log.info("Run dimension table truncate", dimension_truncate_sql)
            redshift.run(dimension_truncate_sql)
        dimension_sql = LoadDimensionOperator.dimension_sql_template.format(
            dimension_table_name=self.dimension_table_name,
            dimension_table_insert_query=self.dimension_table_insert_query
        )
        self.log.info("Run dimension table insert", dimension_sql)
        redshift.run(dimension_sql)
