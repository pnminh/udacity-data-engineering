from airflow.hooks.postgres_hook import PostgresHook


class DataValidationTests:
    def data_exist(self, redshift_connection_id, table):
        """
        check if data exist
        :param redshift_connection_id: airflow redshift connection id
        :param table: the table to check
        """
        redshift_hook = PostgresHook(postgres_conn_id=redshift_connection_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
        if len(records) < 1 or len(records[0]) < 1:
            return False
        num_records = records[0][0]
        if num_records < 1:
            return False
        return True

