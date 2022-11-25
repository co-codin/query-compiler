import logging
import pika.channel

from query_compiler.configs.logger_config import config_logger
from query_compiler.utils.parse_utils import get_guid_and_query_from_json
from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query, clear
from query_compiler.errors.base_error import QueryCompilerError

config_logger()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Deliver,
                properties: pika.BasicProperties,
                body: str
        ):
            try:
                guid, json_query = get_guid_and_query_from_json(body)
                sql_query = generate_sql_query(json_query)
                rabbit_mq.publish_sql_query(guid, sql_query)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except QueryCompilerError as exc:
                logger.error(str(exc))
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
            finally:
                clear()

        rabbit_mq.set_callback_function(callback)
        rabbit_mq.start_consuming()
    logger.warning("Shutting down QueryCompiler service")


if __name__ == '__main__':
    main()
