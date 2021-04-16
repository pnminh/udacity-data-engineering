import configparser

import psycopg2

from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Run copy statements to load dataset from source, e.g. s3 bucket
    into the database's staging tables
    :param cur: the cursor object used against the target database
    :param conn: the connection object used against the target database
    :return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Run insert statements against fact and dimensions tables
    :param cur: the cursor object used against the target database
    :param conn: the connection object used against the target database
    :return:
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Run the ETL pipeline that copies data from datasets in s3 bucket
    into Redshift database's staging tables, then insert data from
    these tables into fact and dimension tables for analytical purposes
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2. \
        connect("host={} dbname={} user={} password={} port={}".
                format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
