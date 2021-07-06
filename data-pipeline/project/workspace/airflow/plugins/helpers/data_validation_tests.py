from airflow.hooks.postgres_hook import PostgresHook
import logging

class DataValidationTests:
    def test_data_exist(self, redshift_connection_id, table_list_to_check):
        """
        check if data exist
        :param redshift_connection_id: airflow redshift connection id
        :param table_list_to_check: tables to be checked
        """
        redshift_hook = PostgresHook(postgres_conn_id=redshift_connection_id)
        for table in table_list_to_check:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
