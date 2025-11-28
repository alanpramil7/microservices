from confluent_kafka import Producer
import socket

KAFKA_OUTGOING_TOPIC = "outgoing"

producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": socket.gethostname(),
}

producer = Producer(producer_conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


producer.produce(KAFKA_OUTGOING_TOPIC, "Hello World", callback=acked)
producer.poll(1)
