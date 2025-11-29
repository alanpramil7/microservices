from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio
from confluent_kafka import Consumer, Producer
import os
import logging

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

# OpenTelemetry Setup
resource = Resource(attributes={
    "service.name": os.getenv("OTEL_SERVICE_NAME", "python-service")
})

# Tracing
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Add Console Exporter for debugging
console_exporter = ConsoleSpanExporter()
trace.get_tracer_provider().add_span_processor(SimpleSpanProcessor(console_exporter))

# Metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
    export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(__name__)

# Create custom metrics
request_counter = meter.create_counter(
    "http_requests_total",
    description="Total HTTP requests",
    unit="1"
)

# Logging
logger_provider = LoggerProvider(resource=resource)
set_logger_provider(logger_provider)
logger_provider.add_log_record_processor(
    BatchLogRecordProcessor(OTLPLogExporter(endpoint=otlp_endpoint, insecure=True))
)

# Attach OTLP handler to root logger
handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

# Instrument Kafka
ConfluentKafkaInstrumentor().instrument()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9098")
KAFKA_TOPIC_1 = "python-service-topic_1"
KAFKA_TOPIC_2 = "python-service-topic_2"

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
    consumer.subscribe([KAFKA_TOPIC_1])
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
    await loop.run_in_executor(None, producer.produce, KAFKA_TOPIC_1, message.encode('utf-8'))
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
FastAPIInstrumentor.instrument_app(app)

@app.get("/")
async def read_root():
    logging.info("Root endpoint called")
    request_counter.add(1, {"endpoint": "/", "method": "GET"})
    with tracer.start_as_current_span("root_span"):
        return {"message": "Hello World"}
    
@app.post("/produce")
async def produce_message(message: str):
    logging.info(f"Producing message: {message}")
    request_counter.add(1, {"endpoint": "/produce", "method": "POST"})
    await produce(message)
    return {"message": "Message produced"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)