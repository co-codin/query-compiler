import logging
import pika.channel

from typing import Tuple
from uuid import UUID

from query_compiler.configs.logger_config import config_logger

from query_compiler.services.common import deserialize_json_query
from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query

from query_compiler.errors.base_error import QueryCompilerError
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError


def _get_guid_and_query_from_json(
        query: str, logger: logging.Logger
) -> Tuple[UUID, str]:
    logger.info(f"Deserializing an input request {query}")
    try:
        payload = deserialize_json_query(query)
        guid, json_query = payload['guid'], payload['query']
    except KeyError as key_err:
        raise DeserializeJSONQueryError(query) from key_err
    else:
        logger.info("Request deserialization successfully completed")
        return guid, json_query


def main():
    config_logger()
    logger = logging.getLogger(__name__)
    logger.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel,
                method: pika.spec.Basic.Deliver,
                properties: pika.BasicProperties,
                body: str
        ):
            try:
                guid, json_query = _get_guid_and_query_from_json(body, logger)
                sql_query = generate_sql_query(json_query)
                rabbit_mq.publish_sql_query(guid, sql_query)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except QueryCompilerError as exc:
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
