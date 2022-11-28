from time import sleep
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

    def publish(self, message: str):
        if not self.exchange:
            raise Exception("Exchange not declared")

        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key="",
            body=message,
        )

    def disconnect(self):
        self.connection.close()


def main():
    rabbitmq = RabitMQPublisher(
        "amqps://xbpqokxq:3yrwXuPGN21sHX6z6s2Esejtx-U7JnoO@jackal.rmq.cloudamqp.com/xbpqokxq"
    )
    rabbitmq.exchange_declare("maquinas")

    print(" [x] Enviando mensagem para as filiais desbloquearem as maquinas")
    rabbitmq.publish("Desbloquear maquinas")

    sleep(2)
    print(" [x] Enviando mensagem para as filiais aumentarem o preço das maquinas")
    rabbitmq.publish("Aumentar preço em 25%")
    sleep(2)

    print(" [x] Enviando mensagem para as filiais diminuirem o preço das maquinas")
    sleep(2)
    rabbitmq.publish("Diminuir preço em 15%")

    print(" [x] Enviando mensagem para as filiais bloquearem as maquinas")
    rabbitmq.publish("Bloquear maquinas")

    print(" [x] Enviando mensagem para as filiais pararem")
    rabbitmq.publish("STOP")
    sleep(2)


main()
