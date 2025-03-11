from datetime import datetime
from typing import Dict, Any, List
import json
from logging import Logger
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository

class CdmMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger) -> None:
        self._consumer = kafka_consumer
        self._repository = cdm_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info("START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            payload = msg.get('payload', {})
            user_id = payload.get('user_id')
            products = payload.get('products', [])

            if not user_id or not products:
                continue

            for product in products:
                product_id = product.get('product_id')
                product_name = product.get('name')
                category = product.get('category')

                if product_id and product_name:
                    self._repository.update_user_product_counter(user_id, product_id, product_name)

                if category:
                    category_id = category 
                    self._repository.update_user_category_counter(user_id, category_id, category)

        self._logger.info("FINISH")