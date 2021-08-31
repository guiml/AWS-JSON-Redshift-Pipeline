import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    This function calls the query that drops the table 
    case they exist. 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function calls the query that create the tables
    in the Redshift environment. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function, everytime the script is ran
    this function calls the other functions. This function
    also sets the connection to the Cluster
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()