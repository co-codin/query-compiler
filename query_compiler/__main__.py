import logging
import json
import pika.channel

from query_compiler.configs.logger_config import config_logger

from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query

from query_compiler.errors.base_error import QueryCompilerError
from query_compiler.errors.schemas_errors import HTTPErrorFromDataCatalog

config_logger()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Deliver,
                properties: pika.BasicProperties,
                body: bytes
        ):
            try:
                payload = json.loads(body)
                guid = payload['guid']
                query = payload['query']
                sql_query = generate_sql_query(query)
                rabbit_mq.publish_sql_query(guid, sql_query)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except HTTPErrorFromDataCatalog as data_catalog_exc:
                logger.error(data_catalog_exc)
            except QueryCompilerError as exc:
                logger.error(exc)
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )

        rabbit_mq.set_callback_function(callback)
        rabbit_mq.start_consuming()

    logger.info("Shutting down QueryCompiler service")


if __name__ == '__main__':
    main()
