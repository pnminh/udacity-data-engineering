from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dimension_sql_template = """
            TRUNCATE {dimension_table_name}; 
            INSERT INTO {dimension_table_name}
            {dimension_table_insert_query};
        """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dimension_table_name="",
                 dimension_table_insert_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table_name = dimension_table_name
        self.dimension_table_insert_query = dimension_table_insert_query
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dimension_sql = LoadDimensionOperator.dimension_sql_template.format(
            dimension_table_name=self.dimension_table_name,
            dimension_table_insert_query=self.dimension_table_insert_query
        )
        self.log.info("Run dimension table insert", dimension_sql)
        redshift.run(dimension_sql)
