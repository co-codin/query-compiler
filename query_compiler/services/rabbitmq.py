import pika
import logging

from typing import Callable

from query_compiler.configs.constants import *


class RabbitMQService:
    def __init__(self):
        """Set connection parameters to RabbitMQ task broker"""
        self._conn_params = pika.ConnectionParameters(
            host=TASK_BROKER_HOST
        )
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
            REQUEST_QUEUE,
            REQUEST_CHANNEL_IS_DURABLE
        )
        self._query_channel.queue_declare(
            QUERY_QUEUE,
            QUERY_CHANNEL_IS_DURABLE
        )

        self._request_channel.basic_qos(REQUEST_CHANNEL_PREFETCH_COUNT)
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
            REQUEST_QUEUE, on_message_callback=callback
        )

    def start_consuming(self):
        self._request_channel.start_consuming()

    def publish_sql_query(self, sql_query: str):
        self._query_channel.basic_publish(
            exchange=QUERY_CHANNEL_EXCHANGE,
            routing_key=QUERY_CHANNEL_ROUTING_KEY,
            body=bytes(sql_query),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
