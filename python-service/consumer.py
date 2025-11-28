from confluent_kafka import Consumer

consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "python-consumer",
}

consumer = Consumer(consumer_conf)
