import s3fs
from data_integration_pipeline.common.io.logger import logger

from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_CLIENT_KWARGS,
    S3_CONFIG_KWARGS,
    S3_SECRET_ACCESS_KEY,
)


def get_s3_client(
    s3_access_key: str = S3_ACCESS_KEY,
    s3_secret_key: str = S3_SECRET_ACCESS_KEY,
    client_kwargs: dict = S3_CLIENT_KWARGS,
    config_kwargs: dict = S3_CONFIG_KWARGS,
) -> s3fs.S3FileSystem:
    logger.debug(f'Trying to connect to {S3_CLIENT_KWARGS.get("endpoint_url")}')
    return s3fs.S3FileSystem(key=s3_access_key, secret=s3_secret_key, client_kwargs=client_kwargs, config_kwargs=config_kwargs)


if __name__ == '__main__':
    client = get_s3_client()
