import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Replace these variables with your PostgreSQL database connection details
db_params = {
    'dbname': 'hamon_db',
    'user': 'username',
    'password': 'password',
    'host': 'your_host',
    'port': '5432',
}

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)

# Create a cursor object to execute SQL queries
cur = conn.cursor()

engine = create_engine(
    f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")


def insert_to_psql(dataframes):
    create_bookings_table_query = """
    CREATE TABLE IF NOT EXISTS bookings (
        booking_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        booking_date DATE,
        destination_id INTEGER,
        number_of_passengers DECIMAL,
        cost_per_passenger DECIMAL,
        total_booking_value DECIMAL,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (destination_id) REFERENCES destinations(destination_id)
    );
    """

    create_customers_table_query = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        phone VARCHAR(15)
    );
    """

    create_destinations_table_query = """
    CREATE TABLE IF NOT EXISTS destinations (
        destination_id SERIAL PRIMARY KEY,
        destination_name VARCHAR(50),
        country VARCHAR(50),
        popular_season VARCHAR(50)
    );
    """

    # Execute the queries to create tables
    cur.execute(create_bookings_table_query)
    cur.execute(create_customers_table_query)
    cur.execute(create_destinations_table_query)

    # Commit the changes to the database
    conn.commit()

    # Load the transformed DataFrames into PostgreSQL tables
    dataframes[0].to_sql('bookings', engine, if_exists='replace', index=False)
    dataframes[1].to_sql('customers', engine, if_exists='replace', index=False)
    dataframes[2].to_sql('destinations', engine, if_exists='replace', index=False)

    # Close the cursor and connection
    cur.close()
    conn.close()


def retrieve_data_from_postgresql(table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, engine)
    return df
