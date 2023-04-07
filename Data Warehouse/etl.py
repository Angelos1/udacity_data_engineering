import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    The function to handle inserting data into the staging tables.
    
    Runs the "COPY FROM" statements defined in the copy_table_queries list
    and inserts the data information to the staging_events and staging_songs tables.
    
    Parameters:
        cur : The cursor used to execute the "COPY FROM" statements.
        conn : The connection to the Redshift cluster.
    '''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    The function to handle inserting data into the data warehouse tables.
    
    Runs the "INSERT INTO" statements defined in the insert_table_queries list
    and inserts the data into the songplays, users, songs, artists and time tables.
    
    Parameters:
        cur : The cursor used to execute the "INSERT INTO" statements.
        conn : The connection to the Redshift cluster.
    '''
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """    
    - Establishes connection with the cluster and gets
    cursor to it.    
    
    - Inserts the data into the staging tables. 
    
    - Inserts the data into the data warehouse tables. 
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()