import logging
import json
import pika.channel

from query_compiler.configs.logger_config import config_logger
from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query
from query_compiler.errors.base_error import QueryCompilerError


config_logger()
LOG = logging.getLogger(__name__)


def main():
    logger = logging.getLogger(__name__)
    logger.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Deliver,
                properties: pika.BasicProperties,
                body: bytes
        ):
            guid = None
            try:
                payload = json.loads(body)
                guid = payload['guid']
                query = payload['query']
                LOG.info(f'Received task for {guid}')
                sql_query = generate_sql_query(query)
                LOG.info(f'Compiled task {guid}')
                rabbit_mq.publish_sql_query(guid, sql_query)
                LOG.info(f'Task {guid} sent to broker')
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as exc:
                logger.exception(str(exc))
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
                if guid:
                    rabbit_mq.publish_sql_error(guid, 'Failed to compile')

        rabbit_mq.set_callback_function(callback)
        rabbit_mq.start_consuming()
    logger.warning("Shutting down QueryCompiler service")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOG.exception(f'Failed to run: {e}')
        raise
