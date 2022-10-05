import logging
import pika.channel

from query_compiler.configs.logger_config import config_logger
from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query
from query_compiler.errors.query_parse_errors import QueryParseServiceError


def main():
    config_logger()
    logger = logging.getLogger(__name__)
    logger.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Deliver,
                properties: pika.BasicProperties,
                body: bytes
        ):
            try:
                sql_query = generate_sql_query(body)
                rabbit_mq.publish_sql_query(sql_query)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except QueryParseServiceError as exc:
                logger.exception(str(exc))
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )

        rabbit_mq.set_callback_function(callback)
        rabbit_mq.start_consuming()
    logger.warning("Shutting down QueryCompiler service")


if __name__ == '__main__':
    main()
