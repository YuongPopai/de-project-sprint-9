from datetime import datetime
from typing import Dict, Any, List
import json
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository

class DdsMessageProcessor:
    def __init__(self,
                 kafka_consumer: KafkaConsumer,
                 kafka_producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = kafka_consumer
        self._producer = kafka_producer
        self._repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info("START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            payload = msg['payload']
            user_pk = self._repository.insert_hub('h_user', 'h_user_pk', payload['user']['id'], 'Kafka')
            restaurant_pk = self._repository.insert_hub('h_restaurant', 'h_restaurant_pk', payload['restaurant']['id'], 'Kafka')
            order_pk = self._repository.insert_hub('h_order', 'h_order_pk', str(payload['id']), 'Kafka')

            self._repository.insert_link('l_order_user', 'hk_order_user_pk',
                                         {'h_order_pk': order_pk, 'h_user_pk': user_pk}, 'Kafka')

            self._repository.insert_satellite('s_user_names', 'h_user_pk', user_pk,
                                              {'username': payload['user']['name'], 'userlogin': ''}, 'Kafka')
            self._repository.insert_satellite('s_restaurant_names', 'h_restaurant_pk', restaurant_pk,
                                              {'name': payload['restaurant']['name']}, 'Kafka')
            self._repository.insert_satellite('s_order_cost', 'h_order_pk', order_pk,
                                              {'cost': payload['cost'], 'payment': payload['payment']}, 'Kafka')
            self._repository.insert_satellite('s_order_status', 'h_order_pk', order_pk,
                                              {'status': payload['final_status']}, 'Kafka')
            for item in payload['order_items']:
                product_pk = self._repository.insert_hub('h_product', 'h_product_pk', item['id'], 'Kafka')
                self._repository.insert_link('l_order_product', 'hk_order_product_pk',
                                             {'h_order_pk': order_pk, 'h_product_pk': product_pk}, 'Kafka')
                self._repository.insert_satellite('s_product_names', 'h_product_pk', product_pk,
                                                  {'name': item['name']}, 'Kafka')
            cdm_payload = {
                "user_id": str(user_pk),
                "products": [
                    {
                        "product_id": str(product_pk),
                        "quantity": item['quantity'],
                        "price": item['price'],
                        "category": item.get('category', '')
                    } for item in payload['order_items']
                ]
            }
            self._producer.produce(cdm_payload)

        self._logger.info("FINISH")