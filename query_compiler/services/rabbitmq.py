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

LOG = logging.getLogger(__name__)


class RabbitMQService:
    def __init__(self):
        self._conn_params = pika.URLParameters(
            settings.mq_connection_string +
            f"?heartbeat={settings.heartbeat}&"
            f"connection_attempts={settings.connection_attempts}&"
            f"retry_delay={settings.retry_delay}"
        )

        self._conn: Union[pika.BlockingConnection, None] = None
        self._request_channel: Union[Channel, None] = None
        self._query_channel: Union[Channel, None] = None

    def __enter__(self):
        try:
            self._set_connection()
        except (RuntimeError, gaierror):
            LOG.error(NoAMQPConnectionError(settings.mq_connection_string))
        else:
            self._set_request_channel()
            self._set_query_channel()

            self._declare_request_queue()
            self._declare_query_queue()
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_channels()
        self._close_connection()
        return True

    def _set_connection(self):
        LOG.info(
            f"Setting a connection to RabbitMQ server: "
            f"{settings.mq_connection_string}"
        )
        self._conn = pika.BlockingConnection(self._conn_params)

    def _set_request_channel(self):
        LOG.info("Creating a request channel")
        self._request_channel = self._conn.channel()
        self._request_channel.basic_qos(
            settings.request_channel_prefetch_count
        )

    def _set_query_channel(self):
        LOG.info("Creating a query channel")
        self._query_channel = self._conn.channel()

    def _declare_request_queue(self):
        LOG.info(
            f"Declaring {settings.request_queue} queue in the request channel"
        )
        self._request_channel.queue_declare(
            settings.request_queue, durable=True
        )

    def _declare_query_queue(self):
        LOG.info(
            f"Declaring {settings.query_queue} queue in the result channel"
        )
        self._query_channel.queue_declare(
            settings.query_queue, durable=True
        )

    def _close_channels(self):
        LOG.info("Closing request and query channels")
        for channel in (self._request_channel, self._query_channel):
            try:
                if channel is not None:
                    channel.close()
            except ChannelWrongStateError:
                LOG.warning(
                    f"Channel {channel.channel_number} is already closed"
                )
        self._request_channel = None
        self._query_channel = None

    def _close_connection(self):
        LOG.info(f"Closing the connection")
        try:
            if self._conn is not None:
                self._conn.close()
        except ConnectionWrongStateError:
            LOG.warning("Connection is already closed")
        finally:
            self._conn = None

    def set_callback_function(self, callback: Callable):
        LOG.info(
            f"Setting a callback function to the {settings.request_queue} queue"
        )
        self._request_channel.basic_consume(
            settings.request_queue, on_message_callback=callback
        )

    def start_consuming(self):
        LOG.info("Starting consuming...")
        self._request_channel.start_consuming()

    def publish_sql_query(self, guid: str, sql_query: str):
        self._publish_status(guid, {
            'query': sql_query,
            'status': 'compiled',
        })

    def publish_sql_error(self, guid: str, error: str):
        self._publish_status(guid, {
            'error': error,
            'status': 'error',
        })

    def _publish_status(self, guid: str, data: dict):
        payload = {
            'guid': guid
        }
        payload.update(data)
        self._query_channel.basic_publish(
            exchange=settings.query_channel_exchange,
            routing_key=settings.query_channel_routing_key,
            body=json.dumps(payload).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
