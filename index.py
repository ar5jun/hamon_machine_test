import requests
import pandas as pd
from insert_to_db import insert_to_psql
from insert_to_db import retrieve_data_from_postgresql
from upload_to_gcs import upload_dataframe_to_gcs


pd.set_option('display.max_columns', None)

dataframes = []


def download_from_source(repo_url, file_paths):

    # Loop through each file path
    for file_path in file_paths:
        raw_url = f'{repo_url}/raw/main/{file_path}'

        # Make a GET request to the raw URL of the CSV file
        response = requests.get(raw_url)

        # Check if the request was successful (status code 200)
        try:
            if response.status_code == 200:
                # Save the content to a local CSV file
                local_file_path = f'local_{file_path.split("/")[-1]}'  # Save with a local filename
                with open(local_file_path, 'wb') as f:
                    f.write(response.content)

                # Load the CSV file into a pandas DataFrame
                df = pd.read_csv(local_file_path)

                # Transform the data
                df = transform_data(df)

                dataframes.append(df)

        except Exception as e:
            raise e


def transform_data(df):
    # Handle missing values in numerical values
    numerical_columns = df.select_dtypes(include=['number']).columns
    df[numerical_columns] = df[numerical_columns].fillna(df[numerical_columns].mean())

    # Handle missing values in categorical values
    categorical_columns = df.select_dtypes(include=['object']).columns
    df[categorical_columns] = df[categorical_columns].fillna(df[categorical_columns].mode().iloc[0])

    # Convert date objects
    if 'booking_date' in df:
        df['booking_date'] = pd.to_datetime(df['booking_date'], errors='coerce')

    # Add total_booking_value filed for dataframe booking_data
    if 'number_of_passengers' in df:
        df['total_booking_value'] = df['number_of_passengers'] * df['cost_per_passenger']

    return df


if __name__ == '__main__':
    repo_url = 'https://github.com/ar5jun/hamon_machine_test'
    file_paths = ['hamon/booking_data.csv', 'hamon/customer_data.csv', 'hamon/destination_data.csv']

    # Function to download the csv files from GitHub repo and convert it into dataframes
    download_from_source(repo_url, file_paths)

    # Function to insert the dataframes into a PostgreSQL db
    insert_to_psql(dataframes)

    tables_to_transfer = ['bookings', 'customers', 'destinations']

    # Transfer the data to a gcs bucket
    for table_name in tables_to_transfer:
        df = retrieve_data_from_postgresql(table_name)
        gcs_filename = f'folder_path/{table_name}.csv'
        upload_dataframe_to_gcs(df, gcs_filename)
