from typing import Tuple
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Logging constants"""
    debug: bool = False
    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "query_compiler.log"

    """RabbitMQ constants"""
    mq_connection_string: str = 'amqp://dwh:dwh@rabbit.lan:5672'

    request_queue: str = 'compile_tasks'
    result_queue: str = 'compile_results'

    request_channel_is_durable: bool = True
    request_channel_prefetch_count: int = 0

    result_channel_is_durable: bool = True
    result_channel_exchange: str = 'query_compile'
    result_channel_routing_key: str = 'result'

    """DataCatalog constants"""
    data_catalog_url: str = "http://data-catalog.lan"
    data_catalog_port: int = 8000
    retries: int = 5
    timeout: int = 1
    retry_status_list: Tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_method_list: Tuple[str, ...] = ('GET',)

    class Config:
        env_file = '../../.env'
        env_file_encoding = 'utf-8'


settings = Settings()
