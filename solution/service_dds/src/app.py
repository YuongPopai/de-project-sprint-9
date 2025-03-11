import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect

app = Flask(__name__)

@app.get('/health')
def health():
    return 'healthy'

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    config = AppConfig()
    kafka_consumer = KafkaConsumer(
        config.kafka_host(),
        config.kafka_port(),
        config.kafka_consumer_group(),
        config.kafka_source_topic()
    )
    kafka_producer = KafkaProducer(
        config.kafka_host(),
        config.kafka_port(),
        config.kafka_destination_topic()
    )

    pg_warehouse_db = PgConnect(
        config.pg_warehouse_host(),
        config.pg_warehouse_port(),
        config.pg_warehouse_dbname(),
        config.pg_warehouse_user(),
        config.pg_warehouse_password()
    )

    dds_repository = DdsRepository(pg_warehouse_db)
    batch_size = 30
    proc = DdsMessageProcessor(
        kafka_consumer,
        kafka_producer,
        dds_repository,
        batch_size,
        app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    app.run(debug=True, host='0.0.0.0', use_reloader=False)