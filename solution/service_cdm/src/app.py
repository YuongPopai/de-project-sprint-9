import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository.cdm_repository import CdmRepository
from lib.kafka_connect import KafkaConsumer
from lib.pg import PgConnect

app = Flask(__name__)

@app.get('/health')
def health():
    return 'healthy'

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()
    
    kafka_consumer = KafkaConsumer(
        config.KAFKA_HOST,
        config.KAFKA_PORT,
        config.KAFKA_CONSUMER_GROUP,
        config.KAFKA_SOURCE_TOPIC
    )
    pg_warehouse_db = PgConnect(
        config.PG_WAREHOUSE_HOST,
        config.PG_WAREHOUSE_PORT,
        config.PG_WAREHOUSE_DBNAME,
        config.PG_WAREHOUSE_USER,
        config.PG_WAREHOUSE_PASSWORD
    )
    cdm_repository = CdmRepository(pg_warehouse_db)
    proc = CdmMessageProcessor(
        kafka_consumer,
        cdm_repository,
        app.logger
    )
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()
    app.run(debug=True, host='0.0.0.0', use_reloader=False)