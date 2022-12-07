import json
import pika
import logging

from typing import Callable, Union
from pika.exceptions import (
    ChannelWrongStateError, ConnectionWrongStateError
)
from pika.channel import Channel
from socket import gaierror

from query_compiler.configs.settings import settings
from query_compiler.errors.rabbit_mq_errors import NoAMQPConnectionError


class RabbitMQService:
    def __init__(self):
        self._conn_params = pika.URLParameters(
            settings.mq_connection_string +
            f"?heartbeat={settings.heartbeat}&"
            f"connection_attempts={settings.connection_attempts}&"
            f"retry_delay={settings.retry_delay}"
        )
        self._logger = logging.getLogger(__name__)

        self._conn: Union[pika.BlockingConnection, None] = None
        self._request_channel: Union[Channel, None] = None
        self._query_channel: Union[Channel, None] = None

    def __enter__(self):
        try:
            self._set_connection()
        except (RuntimeError, gaierror):
            self._logger.error(
                NoAMQPConnectionError(settings.mq_connection_string)
            )
        else:
            self._set_request_channel()
            self._set_result_channel()

            self._declare_request_queue()
            self._declare_result_queue()
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_channels()
        self._close_connection()
        return True

    def _set_connection(self):
        self._logger.info(
            f"Setting a connection to RabbitMQ server "
            f"{settings.mq_connection_string}"
        )
        self._conn = pika.BlockingConnection(self._conn_params)

    def _set_request_channel(self):
        self._logger.info("Creating a request channel")
        self._request_channel = self._conn.channel()
        self._request_channel.basic_qos(
            settings.request_channel_prefetch_count
        )

    def _set_result_channel(self):
        self._logger.info("Creating a result channel")
        self._query_channel = self._conn.channel()

    def _declare_request_queue(self):
        self._logger.info(
            f"Declaring {settings.request_queue} queue in the request channel"
        )
        self._request_channel.queue_declare(
            settings.request_queue
        )

    def _declare_result_queue(self):
        self._logger.info(
            f"Declaring {settings.result_queue} queue in the result channel"
        )
        self._query_channel.queue_declare(
            settings.result_queue
        )

    def _close_channels(self):
        self._logger.info("Closing request and result channels")
        for channel in (self._request_channel, self._query_channel):
            try:
                if channel is not None:
                    channel.close()
            except ChannelWrongStateError:
                self._logger.warning(
                    f"Channel {channel.channel_number} is already closed"
                )
        self._request_channel = None
        self._query_channel = None

    def _close_connection(self):
        self._logger.info(f"Closing the connection")
        try:
            if self._conn is not None:
                self._conn.close()
        except ConnectionWrongStateError:
            self._logger.warning("Connection is already closed")
        finally:
            self._conn = None

    def set_callback_function(self, callback: Callable):
        self._logger.info(
            f"Setting a callback function to the {settings.request_queue}"
        )
        self._request_channel.basic_consume(
            settings.request_queue, on_message_callback=callback
        )

    def start_consuming(self):
        self._logger.info("Starting consuming...")
        self._request_channel.start_consuming()

    def publish_sql_query(self, guid: str, sql_query: str):
        result_dict = {'guid': guid, 'query': sql_query}
        self._logger.info(
            f"Sending {result_dict} to the {settings.result_queue}"
        )
        self._query_channel.basic_publish(
            exchange=settings.result_channel_exchange,
            routing_key=settings.result_channel_routing_key,
            body=json.dumps(result_dict).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
