from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Logging constants"""
    log_dir: str = 'logs'
    time_period_unit: str = 'D'
    backup_count: int = 5
    date_time_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = 'utf-8'

    """RabbitMQ constants"""
    task_broker_host: str = Field('localhost')
    request_queue: str = 'request_queue'
    query_queue: str = 'query_queue'

    request_channel_is_durable: bool = True
    request_channel_prefetch_count: int = 1

    query_channel_is_durable: bool = True
    query_channel_exchange: str = ''
    query_channel_routing_key: str = 'query_queue'

    class Config:
        env_file = '../../.env'
        env_file_encoding = 'utf-8'


settings = Settings()
