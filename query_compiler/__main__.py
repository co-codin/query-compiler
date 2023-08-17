import logging
import pika.channel

from query_compiler.errors.query_parse_errors import AccessDeniedError
from query_compiler.configs.logger_config import config_logger
from query_compiler.utils.parse_utils import deserialize_json_query
from query_compiler.services.rabbitmq import RabbitMQService
from query_compiler.services.query_parse import generate_sql_query

config_logger()
LOG = logging.getLogger(__name__)


def main():
    LOG.info("Starting QueryCompiler service")

    with RabbitMQService() as rabbit_mq:
        def callback(
                ch: pika.channel.Channel, method: pika.spec.Basic.Deliver, properties: pika.BasicProperties, body: str
        ):
            guid = None
            run_guid = None
            try:
                payload = deserialize_json_query(body)
                guid = payload['guid']
                query = payload['query']
                identity_id = payload['identity_id']
                run_guid = payload['run_guid']
                LOG.info(f'Received task for {guid}')
                sql_query = generate_sql_query(query, identity_id)
                LOG.info(f'Compiled task {guid}')
                conn_string = payload['conn_string']

                rabbit_mq.publish_sql_query(guid, sql_query, conn_string, run_guid)
                LOG.info(f'Task {guid} sent to broker')
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except AccessDeniedError as exc:
                LOG.error(str(exc))
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
                if guid and run_guid:
                    rabbit_mq.publish_sql_error(guid, f'Access denied for {exc.denied_fields}', run_guid)
            except Exception as exc:
                LOG.error(str(exc))
                ch.basic_reject(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )
                if guid and run_guid:
                    rabbit_mq.publish_sql_error(guid, 'Failed to compile', run_guid)

        rabbit_mq.set_callback_function(callback)
        rabbit_mq.start_consuming()
    LOG.info("Shutting down QueryCompiler service")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOG.exception(f'Failed to run: {e}')
        raise
