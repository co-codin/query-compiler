import json
import sys
import pika
import logging

from typing import Callable
from pika.exceptions import (
    AMQPError, ChannelWrongStateError, ConnectionWrongStateError
)

from query_compiler.configs.settings import settings


class RabbitMQService:
    def __init__(self):
        self._conn_params = pika.URLParameters(
            settings.mq_connection_string +
            f"?heartbeat={settings.heartbeat}&"
            f"connection_attempts={settings.connection_attempts}&"
            f"retry_delay={settings.retry_delay}"
        )
        self._logger = logging.getLogger(__name__)

        self._conn = None
        self._request_channel = None
        self._result_channel = None

    def __enter__(self):
        try:
            self._set_connection()
            self._set_request_channel()
            self._set_result_channel()
            self._declare_request_queue()
            self._declare_result_queue()
        except AMQPError as pika_err:
            self._logger.exception(pika_err)
            self.__exit__(*sys.exc_info())
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_channels()
        self._close_connection()
        return True

    def _set_connection(self):
        self._logger.info(
            f"Setting a connection to the RabbitMQ server with "
            f"url = {settings.mq_connection_string}, "
            f"heartbeat = {settings.heartbeat}, "
            f"connection attempts = {settings.connection_attempts}, "
            f"retry delay = {settings.retry_delay}"
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
        self._result_channel = self._conn.channel()

    def _declare_request_queue(self):
        self._logger.info(
            f"Declaring {settings.request_queue} in the request channel "
            f"with durable = {settings.request_channel_is_durable}"
        )
        self._request_channel.queue_declare(
            settings.request_queue,
            settings.request_channel_is_durable
        )

    def _declare_result_queue(self):
        self._logger.info(
            f"Declaring {settings.result_queue} in the result channel "
            f"with durable = {settings.result_channel_is_durable}"
        )
        self._result_channel.queue_declare(
            settings.result_queue,
            settings.result_channel_is_durable
        )

    def _close_channels(self):
        self._logger.info("Closing request and result channels")
        for channel in (self._request_channel, self._result_channel):
            try:
                channel.close()
            except ChannelWrongStateError as channel_err:
                self._logger.warning(channel_err)
                continue
            except AttributeError as attr_err:
                self._logger.warning(attr_err)
                continue

    def _close_connection(self):
        self._logger.info(f"Closing the connection")
        try:
            self._conn.close()
        except ConnectionWrongStateError as conn_error:
            self._logger.warning(conn_error)
        except AttributeError as attr_err:
            self._logger.warning(attr_err)

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
        self._result_channel.basic_publish(
            exchange=settings.result_channel_exchange,
            routing_key=settings.result_channel_routing_key,
            body=json.dumps(result_dict).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
