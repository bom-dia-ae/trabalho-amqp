from typing import Callable
import pika
from pika.exchange_type import ExchangeType


class RabitMQPublisher:
    def __init__(self, url: str):
        self.connection = pika.BlockingConnection(pika.URLParameters(url))
        self.channel = self.connection.channel()

    def exchange_declare(self, exchange: str):
        self.exchange = exchange
        self.channel.exchange_declare(
            exchange=exchange,
            exchange_type=ExchangeType.fanout,
            durable=True,
        )

    def declare_and_bind_queue(self):
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name)

    def start(self, callback: Callable):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=callback,
            auto_ack=True,
        )
        self.channel.start_consuming()

    def disconnect(self):
        self.connection.close()


def callback(ch, method, properties, body: bytes):
    msg = body.decode()
    if msg == "STOP":
        print(" [x] Recebendo mensagem para parar...")
        ch.stop_consuming()
    else:
        print(f" [x] Recebido mensagem da sede: {body.decode()}")


def main():
    rabbitmq = RabitMQPublisher(
        "amqps://xbpqokxq:3yrwXuPGN21sHX6z6s2Esejtx-U7JnoO@jackal.rmq.cloudamqp.com/xbpqokxq"
    )

    rabbitmq.exchange_declare("maquinas")
    rabbitmq.declare_and_bind_queue()
    rabbitmq.start(callback)


main()
