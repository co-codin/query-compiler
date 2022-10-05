"""App constants"""
import os

"""RabbitMQ constants"""
TASK_BROKER_HOST = os.getenv('TASK_BROKER_HOST', default='localhost')
REQUEST_QUEUE = 'request_queue'
QUERY_QUEUE = 'query_queue'

REQUEST_CHANNEL_IS_DURABLE = True
REQUEST_CHANNEL_PREFETCH_COUNT = 1

QUERY_CHANNEL_IS_DURABLE = True
QUERY_CHANNEL_EXCHANGE = ''
QUERY_CHANNEL_ROUTING_KEY = 'query_queue'
