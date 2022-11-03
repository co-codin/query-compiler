from typing import Tuple
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Logging constants"""
    log_dir: str = 'logs'
    time_period_unit: str = 'D'
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = 'utf-8'

    """RabbitMQ constants"""
    mq_connection_string: str = 'amqp://dwh:dwh@rabbit:5672'
    request_queue: str = 'request_queue'
    result_queue: str = 'query_queue'

    request_channel_is_durable: bool = True
    request_channel_prefetch_count: int = 0

    result_channel_is_durable: bool = True
    result_channel_exchange: str = ''
    result_channel_routing_key: str = 'result'

    """DataCatalog constants"""
    data_catalog_url: str = "http://data_catalog"
    data_catalog_port: int = 8000
    retries: int = 5
    timeout: int = 1
    retry_status_list: Tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_method_list: Tuple[str, ...] = ('GET',)

    class Config:
        env_file = '../../.env'
        env_file_encoding = 'utf-8'


settings = Settings()
