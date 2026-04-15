# Uploads test data to Hetzner S3 storage
def main():
    import os

    files = [
        'tests/data/bronze/dataset_1/dataset_1_generated.json',
        'tests/data/bronze/dataset_2/dataset_2_generated.json',
        'tests/data/bronze/dataset_3/dataset_3.parquet',
    ]
    from data_integration_pipeline.common.io.cloud_storage.s3_storage.base_client import get_s3_client

    S3_ENDPOINT_URL = 'https://fsn1.your-objectstorage.com'
    S3_REGION = 'eu-central'
    S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
    S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')

    S3_CLIENT_KWARGS = {
        'endpoint_url': S3_ENDPOINT_URL,
        'region_name': S3_REGION,
    }
    S3_CONFIG_KWARGS = {
        'signature_version': 's3v4',
        's3': {
            'payload_signing_enabled': False,
            'addressing_style': 'virtual',
        },
    }

    s3_client = get_s3_client(
        s3_access_key=S3_ACCESS_KEY, s3_secret_key=S3_SECRET_ACCESS_KEY, client_kwargs=S3_CLIENT_KWARGS, config_kwargs=S3_CONFIG_KWARGS
    )
    for local_file_path in files:
        cloud_file_path = os.path.join('pmtq-data', 'dip_test_data', local_file_path)
        s3_client.put(local_file_path, cloud_file_path)
        print(f'Uploaded {local_file_path} to {cloud_file_path}')


if __name__ == '__main__':
    main()
