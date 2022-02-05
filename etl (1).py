import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This procedure loads the data from s3 into the staging tables in redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This procedure inserts the data into the fact and dimension tables from the staging tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except psycopg2.Error as e:
        print("Error in connecting with host")
        print(e)
    cur = conn.cursor()
    
    print("Loading the staging tables... ")
    load_staging_tables(cur, conn)
    print("Loading completed!!!")
    print("Inserting the records to Final table ...")
    insert_tables(cur, conn)
    print("Insertion completed")

    conn.close()

if __name__ == "__main__":
    main()