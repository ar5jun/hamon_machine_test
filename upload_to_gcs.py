from google.cloud import storage


project_id = 'project-id'
bucket_name = 'bucket-name'
service_account_key_path = 'path/to/your/service-account-key.json'

# Set up the Google Cloud Storage client
gcs_client = storage.Client.from_service_account_json(service_account_key_path)


def upload_dataframe_to_gcs(dataframe, gcs_filename):
    bucket = gcs_client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_filename)

    # Convert DataFrame to CSV format
    csv_content = dataframe.to_csv(index=False)

    # Upload the CSV content to GCS
    blob.upload_from_string(csv_content, content_type='text/csv')
