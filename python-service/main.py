from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9098"
KAFKA_TOPIC = "python-service-topic"

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "python-producer",
}

producer = Producer(producer_config)

consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "python-consumer-group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

async def process_message(msg):
    print(f"Processing message: {msg.value().decode('utf-8')}")

async def consume():
    loop = asyncio.get_event_loop()
    consumer.subscribe([KAFKA_TOPIC])
    try:
        while True:
            msg = await loop.run_in_executor(None, consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            asyncio.create_task(process_message(msg))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

async def produce(message: str):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, producer.produce, KAFKA_TOPIC, message.encode('utf-8'))
    await loop.run_in_executor(None, producer.flush)



@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def read_root():
    return {"message": "Hello World"}
    
@app.post("/produce")
async def produce_message(message: str):
    await produce(message)
    return {"message": "Message produced"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)