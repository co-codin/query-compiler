from typing import Tuple
from pydantic import BaseSettings


class Settings(BaseSettings):
    # Logging constants
    debug: bool = False
    log_dir: str = "/var/log/n3dwh/"
    log_name: str = "query_compiler.log"

    # RabbitMQ constants
    heartbeat: int = 5
    connection_attempts: int = 5
    retry_delay: int = 10
    mq_connection_string: str = 'amqp://dwh:dwh@rabbit.lan:5672'

    request_queue: str = 'compile_tasks'
    query_queue: str = 'compile_results'

    request_channel_is_durable: bool = True
    request_channel_prefetch_count: int = 0

    query_channel_is_durable: bool = True
    query_channel_exchange: str = 'query_compile'
    query_channel_routing_key: str = 'result'

    # DataCatalog constants
    data_catalog_url: str = "http://data-catalog.lan:8000"
    retries: int = 5
    timeout: int = 10
    retry_status_list: Tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_method_list: Tuple[str, ...] = ('GET',)

    # IAM constants
    iam_url: str = "http://iam.lan:8000"

    # Schemas constants
    pg_aggregation_functions: Tuple[str, ...] = ('count', 'avg', 'sum', 'min', 'max')
    operator_functions: Tuple[str, ...] = ('<', '<=', '=', '>', '>=', 'like')

    class Config:
        env_prefix = "dwh_query_compiler_"
        case_sensitive = False
        env_file = '../../.env'
        env_file_encoding = 'utf-8'


settings = Settings()
