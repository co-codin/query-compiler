import json

import pika
import logging

from typing import Callable

from query_compiler.configs.settings import settings


class RabbitMQService:
    def __init__(self):
        """Set connection parameters to RabbitMQ task broker"""
        self._conn_params = pika.URLParameters(settings.mq_connection_string)
        self._logger = logging.getLogger(__name__)

    def __enter__(self):
        """
        Log establishing the connection
        Set a connection to a RabbitMQ task broker.

        Log creating 2 channels(request channel, query channel)
        Create 2 channels(request channel, query channel)

        Log declaring queues
        For each channel create a queue(request queue, query queue)
        """
        self._conn = pika.BlockingConnection(self._conn_params)

        self._request_channel = self._conn.channel()
        self._query_channel = self._conn.channel()

        self._request_channel.queue_declare(
            settings.request_queue
        )
        self._query_channel.queue_declare(
            settings.result_queue
        )

        self._request_channel.basic_qos(
            settings.request_channel_prefetch_count
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Log exception type and exception value

        Log request channel closing
        Close request channel

        Log query channel closing
        Close query channel

        Log the connection closing
        Close the connection
        """
        self._request_channel.close()
        self._query_channel.close()
        self._conn.close()

    def set_callback_function(self, callback: Callable):
        self._request_channel.basic_consume(
            settings.request_queue, on_message_callback=callback
        )

    def start_consuming(self):
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
            exchange=settings.result_channel_exchange,
            routing_key=settings.result_channel_routing_key,
            body=json.dumps(payload).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
