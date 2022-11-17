class NoAMQPConnectionError(Exception):
    def __init__(self, mq_connection_string):
        super().__init__(
            f"Couldn't establish a connection with RabbitMQ server "
            f"{mq_connection_string}"
        )
