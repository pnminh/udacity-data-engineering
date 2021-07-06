from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    fact_sql_template = """
        INSERT INTO {fact_table_name}
        {fact_table_insert_query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table_name="",
                 fact_table_insert_query="",
                 *args, **kwargs):
        """
        Initiate operator object
        :param redshift_conn_id: airflow connection id for redshift hook
        :param fact_table_name
        :param fact_table_insert_query: the insert query to be run against the fact table
        :param args
        :param kwargs
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table_name = fact_table_name
        self.fact_table_insert_query = fact_table_insert_query

    def execute(self, context):
        """
        execute fact table data insert against Redshift cluster
        :param context: operator context
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_sql = LoadFactOperator.fact_sql_template.format(
            fact_table_name=self.fact_table_name,
            fact_table_insert_query=self.fact_table_insert_query
        )
        self.log.info("Run fact table insert", fact_sql)
        redshift.run(fact_sql)
